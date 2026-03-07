mod buffer;

#[cfg(feature = "python-extension")]
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use once_cell::sync::OnceCell;
use pyo3::conversion::IntoPyObject;
use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::PyModuleMethods;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
#[cfg(feature = "python-extension")]
use nextmini::node::controller::interface::ControllerInterfaceHandle;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::{
    PayloadDelivery as RustPayloadDelivery, PythonDelivery, PythonEvent, PythonInterfaceHandle,
};
#[cfg(feature = "python-extension")]
use nextmini::node::session;
#[cfg(feature = "python-extension")]
use nextmini::node::session::api::LosslessRuntimeHandle;
use nextmini::node::{NodeId, NodeIdExt};
#[cfg(feature = "python-extension")]
use nextmini_messages::DataplaneToController;

pub use crate::buffer::{PacketBuilder, PacketView};

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
static TRACING: OnceCell<()> = OnceCell::new();

#[cfg(feature = "python-extension")]
type BufferRegistry = Arc<Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>>;

fn rt() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("nextmini-py")
            .build()
            .expect("unable to create tokio runtime for nextmini_py")
    })
}

fn init_tracing_subscriber() {
    TRACING.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_thread_ids(true)
            .with_target(true)
            .try_init();
    });
}

static PY_MESSAGE_ID_SEQ: AtomicU64 = AtomicU64::new(1);

#[pyclass]
struct PacketReceiver {
    inner: Arc<Mutex<mpsc::Receiver<PythonDelivery>>>,
}

#[pymethods]
impl PacketReceiver {
    #[pyo3(signature = (timeout_ms=None))]
    fn recv(&self, timeout_ms: Option<u64>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = self.inner.clone();
        let maybe_delivery = Python::detach(py, move || {
            rt().block_on(async move {
                match timeout_ms {
                    Some(ms) => tokio::time::timeout(
                        std::time::Duration::from_millis(ms),
                        inner.lock().await.recv(),
                    )
                    .await
                    .unwrap_or_default(),
                    None => inner.lock().await.recv().await,
                }
            })
        });
        maybe_delivery
            .map(|delivery| delivery_to_pyobject(py, delivery))
            .transpose()
    }

    fn recv_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let delivery = inner.lock().await.recv().await;
            Python::attach(|py| {
                delivery
                    .map(|delivery| delivery_to_pyobject(py, delivery))
                    .transpose()
            })
        })
    }
}

fn delivery_to_pyobject(py: Python<'_>, delivery: PythonDelivery) -> PyResult<Py<PyAny>> {
    // PythonDelivery is now just PayloadDelivery (type alias)
    let obj = Py::new(py, PyPayloadDelivery::from(delivery))?;

    Ok(obj.into_pyobject(py)?.unbind().into())
}

#[pyclass(name = "PayloadDelivery")]
struct PyPayloadDelivery {
    buffer: PacketView,
    flow_id: u128,
    src_ip: String,
    dst_ip: String,
    src_port: u16,
    dst_port: u16,
    message_id: Option<u64>,
    total_len: Option<u32>,
    fragment_count: Option<u16>,
}

#[pymethods]
impl PyPayloadDelivery {
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.buffer.inner.as_ref())
    }

    #[getter]
    fn frozen_payload(&self) -> PacketView {
        self.buffer.clone()
    }

    #[getter]
    fn flow_id(&self) -> u128 {
        self.flow_id
    }

    #[getter]
    fn src_ip(&self) -> &str {
        &self.src_ip
    }

    #[getter]
    fn dst_ip(&self) -> &str {
        &self.dst_ip
    }

    #[getter]
    fn src_port(&self) -> u16 {
        self.src_port
    }

    #[getter]
    fn dst_port(&self) -> u16 {
        self.dst_port
    }

    #[getter]
    fn message_id(&self) -> Option<u64> {
        self.message_id
    }

    #[getter]
    fn total_len(&self) -> Option<u32> {
        self.total_len
    }

    #[getter]
    fn fragment_count(&self) -> Option<u16> {
        self.fragment_count
    }
}

impl From<RustPayloadDelivery> for PyPayloadDelivery {
    fn from(payload: RustPayloadDelivery) -> Self {
        Self {
            buffer: PacketView::from_bytes(payload.bytes),
            flow_id: payload.flow_id,
            src_ip: payload.src_ip.to_string(),
            dst_ip: payload.dst_ip.to_string(),
            src_port: payload.src_port,
            dst_port: payload.dst_port,
            message_id: payload.message_id,
            total_len: payload.total_len,
            fragment_count: payload.fragment_count,
        }
    }
}

#[pyclass]
struct Dataplane {
    cfg: LocalConfig,
    py_if: PythonInterfaceHandle,
    processor: ProcessorHandle,
    controller: ControllerInterfaceHandle,
    _join: tokio::task::JoinHandle<()>,
    #[cfg(feature = "python-extension")]
    lossless_runtime: Option<LosslessRuntimeHandle>,
    #[cfg(feature = "python-extension")]
    buffer_registry: BufferRegistry,
    event_stash: Arc<Mutex<VecDeque<PythonEvent>>>,
}

impl Dataplane {
    #[cfg(feature = "python-extension")]
    fn remember_buffer_sink(&self, session_id: u64, buf: Arc<Mutex<Vec<u8>>>) {
        let mut guard = rt().block_on(self.buffer_registry.lock());
        guard.insert(session_id, buf);
    }
}

#[pymethods]
impl Dataplane {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, dest_ip, receiver_ids, buffer, *, block_size=8500, src_port=None, dst_port=None))]
    fn send_data(
        &self,
        group_id: u64,
        dest_ip: &str,
        receiver_ids: Vec<usize>,
        buffer: PacketView,
        block_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        #[allow(unused_variables)]
        let dest_ip_addr = parse_ipv4(dest_ip)?;
        if receiver_ids.is_empty() {
            return Err(PyRuntimeError::new_err(
                "receiver_ids must contain at least one entry.",
            ));
        }

        if block_size == 0 {
            return Err(PyRuntimeError::new_err("block_size must be positive."));
        }

        let total_bytes = buffer.inner.len() as u64;
        if total_bytes == 0 {
            return Err(PyRuntimeError::new_err(
                "buffer is empty; nothing to transmit.",
            ));
        }

        // Compute deterministic session_id from group_id and source_node_id
        #[allow(unused_variables)]
        let sid = multicast_session_id(group_id, self.cfg.node_id);
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let runtime_config = &self.cfg.lossless_runtime_config;
                let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
                let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
                let common = session::runtime::CommonConfig {
                    session_id: sid,
                    dest_ip: dest_ip_addr,
                    block_size,
                    src_port: sp,
                    dst_port: dp,
                    data_bucket: runtime_config.data_bucket.clone(),
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
                };
                let cfg = session::runtime::SenderRequest {
                    common,
                    receiver_ids,
                    total_bytes,
                    source_buffer: buffer.inner.clone(),
                    ready_grace_ms: runtime_config.ready_grace_ms,
                };
                let started_sid = rt().block_on(handle.start_sender(cfg)).map_err(|err| {
                    PyRuntimeError::new_err(format!(
                        "lossless sender preflight rejected session {sid}: {err}"
                    ))
                })?;
                return Ok(started_sid);
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = total_bytes;

        Ok(sid)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, dest_ip, source_node_id, expected_bytes, *, block_size=8500, src_port=None, dst_port=None))]
    fn receive_data(
        &self,
        group_id: u64,
        dest_ip: &str,
        source_node_id: usize,
        expected_bytes: u64,
        block_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        #[allow(unused_variables)]
        let ip = parse_ipv4(dest_ip)?;
        if expected_bytes == 0 {
            return Err(PyRuntimeError::new_err("expected_bytes must be positive."));
        }

        if block_size == 0 {
            return Err(PyRuntimeError::new_err("block_size must be positive."));
        }

        // Compute deterministic session_id from group_id and source_node_id
        #[allow(unused_variables)]
        let sid = multicast_session_id(group_id, source_node_id);
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let runtime_config = &self.cfg.lossless_runtime_config;
                let cap = usize::try_from(expected_bytes).unwrap_or(0);
                let sink_buf = Arc::new(Mutex::new(Vec::with_capacity(cap)));
                let common = session::runtime::CommonConfig {
                    session_id: sid,
                    dest_ip: ip,
                    block_size,
                    src_port: src_port.unwrap_or(self.cfg.user_space_client_port),
                    dst_port: dst_port.unwrap_or(self.cfg.user_space_server_port),
                    data_bucket: runtime_config.data_bucket.clone(),
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
                };
                let cfg = session::runtime::ReceiverRequest {
                    common,
                    source_node_id,
                    expected_bytes,
                    sink_buffer: Some(sink_buf.clone()),
                };
                // Direct registration - both sender and receiver compute same session_id
                let started_sid = rt().block_on(handle.start_receiver(cfg));
                self.remember_buffer_sink(started_sid, sink_buf);
                return Ok(started_sid);
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = (src_port, dst_port);

        Ok(sid)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, dest_ip, source_node_id, expected_bytes, *, block_size=8500, src_port=None, dst_port=None))]
    fn receive_data_async<'py>(
        &self,
        py: Python<'py>,
        group_id: u64,
        dest_ip: String,
        source_node_id: usize,
        expected_bytes: u64,
        block_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if expected_bytes == 0 {
            return Err(PyRuntimeError::new_err("expected_bytes must be positive."));
        }
        if block_size == 0 {
            return Err(PyRuntimeError::new_err("block_size must be positive."));
        }

        // Compute deterministic session_id from group_id and source_node_id
        let sid = multicast_session_id(group_id, source_node_id);

        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let handle = handle.clone();
                let runtime_config = self.cfg.lossless_runtime_config.clone();
                let buffer_registry = self.buffer_registry.clone();
                let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
                let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
                let local_node_id = self.cfg.node_id;
                let base_addr = self.cfg.user_space_base_addr;
                let netmask = self.cfg.local_netmask;

                return future_into_py(py, async move {
                    let ip = parse_ipv4(&dest_ip)?;

                    let cap = usize::try_from(expected_bytes).unwrap_or(0);
                    let sink_buf = Arc::new(Mutex::new(Vec::with_capacity(cap)));
                    let common = session::runtime::CommonConfig {
                        session_id: sid,
                        dest_ip: ip,
                        block_size,
                        src_port: sp,
                        dst_port: dp,
                        data_bucket: runtime_config.data_bucket.clone(),
                        local_node_id,
                        user_space_base_addr: base_addr,
                        local_netmask: netmask,
                    };
                    let cfg = session::runtime::ReceiverRequest {
                        common,
                        source_node_id,
                        expected_bytes,
                        sink_buffer: Some(sink_buf.clone()),
                    };

                    // Direct registration - both sender and receiver compute same session_id
                    let started_sid = handle.start_receiver(cfg).await;

                    {
                        let mut guard = buffer_registry.lock().await;
                        guard.insert(started_sid, sink_buf);
                    }

                    Ok(started_sid)
                });
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = (&dest_ip, src_port, dst_port);

        // Fallback if feature disabled (immediate return)
        future_into_py(py, async move {
            info!("receive_data_async: lossless runtime not available, returning immediate sid");
            Ok(sid)
        })
    }

    #[pyo3(signature = (session_id, timeout_ms=None))]
    fn lossless_wait(&self, session_id: u64, timeout_ms: Option<u64>) -> PyResult<bool> {
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let fut = handle.wait_completion(session_id);
                let ok = if let Some(ms) = timeout_ms {
                    rt().block_on(async move {
                        tokio::time::timeout(std::time::Duration::from_millis(ms), fut)
                            .await
                            .unwrap_or(false)
                    })
                } else {
                    rt().block_on(fut)
                };

                // Proactively stop the session to clean up runtime state (tasks, inputs).
                // This prevents stale senders/receivers from holding onto session IDs that
                // may be reused by subsequent lossless transfers (e.g., RL rollouts).
                handle.stop(session_id);

                return Ok(ok);
            }
        }
        // feature disabled ⇒ nothing to wait for
        let _ = (session_id, timeout_ms);
        Ok(false)
    }

    #[pyo3(signature = (session_id, timeout_ms=None))]
    fn lossless_wait_async<'py>(
        &self,
        py: Python<'py>,
        session_id: u64,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let handle = handle.clone();
                return future_into_py(py, async move {
                    let fut = handle.wait_completion(session_id);
                    let ok = if let Some(ms) = timeout_ms {
                        tokio::time::timeout(std::time::Duration::from_millis(ms), fut)
                            .await
                            .unwrap_or(false)
                    } else {
                        fut.await
                    };

                    // After completion, stop the session to drop its task and inputs.
                    // Safe to call even if the session was already cleaned up.
                    handle.stop(session_id);

                    Ok(ok)
                });
            }
        }
        // Fallback
        future_into_py(py, async move {
            info!("lossless_wait_async: lossless runtime not available, returning false");
            Ok(false)
        })
    }

    #[cfg(feature = "python-extension")]
    #[pyo3(signature = (session_id, consume=true))]
    fn get_data_buffer(&self, session_id: u64, consume: bool) -> PyResult<PacketView> {
        let buf_arc = {
            let guard = rt().block_on(self.buffer_registry.lock());
            guard
                .get(&session_id)
                .cloned()
                .ok_or_else(|| PyKeyError::new_err(format!("no buffer for session {session_id}")))?
        };

        let bytes = {
            let mut guard = rt().block_on(buf_arc.lock());
            if consume {
                Bytes::from(std::mem::take(&mut *guard))
            } else {
                Bytes::copy_from_slice(&guard)
            }
        };

        if consume {
            let mut guard = rt().block_on(self.buffer_registry.lock());
            guard.remove(&session_id);
        }

        Ok(PacketView::from_bytes(bytes))
    }

    #[new]
    fn new(config_path: &str) -> PyResult<Self> {
        let toml_str = std::fs::read_to_string(config_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to read config: {e}")))?;
        let mut initial_cfg = LocalConfig::from_toml_str(&toml_str)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to parse config: {e}")))?;
        initial_cfg.enable_local_interface = false;
        initial_cfg.config_path = config_path.to_string();
        initial_cfg.populate_runtime_defaults();

        let conductor = rt().block_on(async { Conductor::new(initial_cfg.clone()).await });
        let processor = conductor.processor_handle();
        let mut cfg = conductor.local_config();
        cfg.config_path = config_path.to_string();
        let controller = conductor.controller_handle();

        #[cfg(feature = "python-extension")]
        let lossless_runtime = conductor.lossless_runtime_handle();

        // enters the bindings runtime so tokio::spawn inside PythonInterfaceHandle::new() succeeds
        let py_if = {
            let _rt_guard = rt().enter();

            PythonInterfaceHandle::new(cfg.channel_capacity, cfg.channel_backpressure)
        };

        processor.connect_python_interface(py_if.clone());
        rt().block_on(controller.attach_python_interface(py_if.clone()));

        let join = rt().spawn(async move {
            conductor.run().await;
        });

        #[cfg(feature = "python-extension")]
        let buffer_registry = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self {
            cfg,
            py_if,
            processor,
            controller,
            _join: join,
            #[cfg(feature = "python-extension")]
            lossless_runtime: Some(lossless_runtime),
            #[cfg(feature = "python-extension")]
            buffer_registry,
            event_stash: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    #[pyo3(signature = (src_node_id, src_port=None, dst_port=None))]
    fn register_receiver_from_node(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = self.cfg.user_space_address;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (src_node_id, group_ip, src_port=None, dst_port=None))]
    fn register_receiver_for_group(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        group_ip: &str,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = parse_ipv4(group_ip)?;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (dst_node_id, frozen, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        dst_node_id: usize,
        frozen: PacketView,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        let body = frozen.inner.clone();

        let src_ip = self.cfg.user_space_address;
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        self.transmit_python_payload(src_ip, dst_ip, sp, dp, body)
    }

    #[pyo3(signature = (label))]
    fn create_group(&self, label: String) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::CreateGroup { label })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (group_id))]
    fn join_group(&self, group_id: usize) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::JoinGroup { group_id })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (group_id))]
    fn leave_group(&self, group_id: usize) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::LeaveGroup { group_id })
                .await;
        });
        self.emit_python_event(PythonEvent::LocalMemberLeft {
            group_id,
            node_id: self.cfg.node_id,
        });
        Ok(())
    }

    /// Set multicast DAG edges for a group (directed edges).
    ///
    /// Intended for external optimizers (e.g. LP solvers) that want the controller to install a
    /// specific multicast tree without rewriting unicast routes.
    #[pyo3(signature = (group_id, edges))]
    fn set_group_routes(&self, group_id: usize, edges: Vec<(u32, u32)>) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::SetGroupRoutes { group_id, edges })
                .await;
        });
        Ok(())
    }

    /// Set multiple multicast trees for a group.
    ///
    /// Each tree is described as `(tree_id, edges)` where `edges` is a list of directed edges
    /// `(from_node_id, to_node_id)`.
    #[pyo3(signature = (group_id, trees))]
    fn set_group_routes_multi(
        &self,
        group_id: usize,
        trees: Vec<(usize, Vec<(u32, u32)>)>,
    ) -> PyResult<()> {
        let trees = trees
            .into_iter()
            .map(|(tree_id, edges)| nextmini_messages::GroupRouteTree {
                tree_id,
                weight: None,
                edges,
            })
            .collect();

        rt().block_on(async {
            self.controller
                .send(DataplaneToController::SetGroupRoutesMulti { group_id, trees })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn group_is_ready(&self, timeout_ms: Option<u64>) -> PyResult<Option<(usize, String, usize)>> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(event, PythonEvent::GroupCreated { .. })
        });

        match matched {
            Some(PythonEvent::GroupCreated {
                group_id,
                src_node_id,
                group_ip,
            }) => Ok(Some((group_id, group_ip.to_string(), src_node_id))),
            _ => Ok(None),
        }
    }

    #[pyo3(signature = (group_id, timeout_ms=None))]
    fn wait_for_local_membership(
        &self,
        group_id: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let local_node = self.cfg.node_id;
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(
                event,
                PythonEvent::LocalMemberJoined {
                    group_id: gid,
                    node_id
                } if *gid == group_id && *node_id == local_node
            )
        });

        Ok(matched.is_some())
    }

    #[pyo3(signature = (group_id, src_node_id, min_routes=1, timeout_ms=None))]
    fn wait_for_group_routes(
        &self,
        group_id: usize,
        src_node_id: usize,
        min_routes: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(
                event,
                PythonEvent::GroupRoutesInstalled {
                    group_id: gid,
                    src_node_id: sid,
                    routes,
                } if *gid == group_id && *sid == src_node_id && routes.len() >= min_routes
            )
        });

        Ok(matched.is_some())
    }

    /// Waits for the topology to be ready (all nodes connected and routes installed).
    /// Returns True if topology is ready, False if timeout occurred.
    #[pyo3(signature = (timeout_ms=None))]
    fn wait_for_topology_ready(&self, timeout_ms: Option<u64>) -> PyResult<bool> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self
            .wait_for_event_matching(timeout, |event| matches!(event, PythonEvent::TopologyReady));

        Ok(matched.is_some())
    }

    /// Returns the local node ID.
    #[getter]
    fn node_id(&self) -> usize {
        self.cfg.node_id
    }

    /// Returns a dictionary with all network configuration for this node.
    /// Useful for throughput testing to get underlay addresses.
    fn get_network_info(&self) -> PyResult<std::collections::HashMap<String, String>> {
        let mut info = std::collections::HashMap::new();
        info.insert("node_id".to_string(), self.cfg.node_id.to_string());
        info.insert(
            "private_network_addr".to_string(),
            self.cfg.private_network_addr.clone(),
        );
        info.insert(
            "public_network_addr".to_string(),
            self.cfg.public_network_addr.clone(),
        );
        info.insert(
            "private_network_port".to_string(),
            self.cfg.private_network_port.clone(),
        );
        info.insert(
            "public_network_port".to_string(),
            self.cfg.public_network_port.clone(),
        );
        info.insert(
            "private_network_interface".to_string(),
            self.cfg.private_network_interface.clone(),
        );
        info.insert(
            "controller_addr".to_string(),
            self.cfg.controller_addr.clone(),
        );
        info.insert(
            "user_space_address".to_string(),
            self.cfg.user_space_address.to_string(),
        );
        Ok(info)
    }
}

impl Dataplane {
    fn transmit_python_payload(
        &self,
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        body: Bytes,
    ) -> PyResult<u64> {
        let message_id = next_py_message_id();
        let packet = Packet::build_ipv4_tcp_packet(src_ip, src_port, dst_ip, dst_port, &body);
        self.processor.process_packet_blocking(packet);

        Ok(message_id)
    }

    fn wait_for_event_matching<F>(
        &self,
        timeout: Option<Duration>,
        mut matcher: F,
    ) -> Option<PythonEvent>
    where
        F: FnMut(&PythonEvent) -> bool,
    {
        let deadline = timeout.map(|dur| Instant::now() + dur);

        // First, check the stash
        {
            let mut stash = rt().block_on(self.event_stash.lock());
            // We need to find the first matching event, remove it, and keep the rest in order.
            // VecDeque doesn't have a "remove_first_matching" that preserves order easily without iteration.
            // We can iterate indices.
            for i in 0..stash.len() {
                if matcher(&stash[i]) {
                    return stash.remove(i);
                }
            }
        }

        // If not found in stash, poll the channel
        loop {
            if let Some(dl) = deadline
                && Instant::now() >= dl
            {
                return None;
            }

            let remaining = deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
            let event = self.recv_event_with_timeout(remaining)?;

            if matcher(&event) {
                return Some(event);
            } else {
                // Not a match, stash it at the back
                rt().block_on(self.event_stash.lock()).push_back(event);
            }
        }
    }

    fn recv_event_with_timeout(&self, timeout: Option<Duration>) -> Option<PythonEvent> {
        match timeout {
            Some(duration) => {
                let handle = self.py_if.clone();
                rt().block_on(async {
                    tokio::time::timeout(duration, handle.next_event())
                        .await
                        .ok()
                        .flatten()
                })
            }
            None => rt().block_on(self.py_if.next_event()),
        }
    }

    fn emit_python_event(&self, event: PythonEvent) {
        let handle = self.py_if.clone();
        rt().block_on(async move {
            handle.publish_event(event).await;
        });
    }
}

fn next_py_message_id() -> u64 {
    PY_MESSAGE_ID_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Generates a deterministic session ID from group_id and source_node_id.
/// Both sender and receiver compute the same session_id, enabling direct matching
/// without needing the pending receiver mechanism.
fn multicast_session_id(group_id: u64, source_node_id: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    group_id.hash(&mut hasher);
    source_node_id.hash(&mut hasher);
    let raw = hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF;
    raw | 0x8000_0000_0000_0000
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_tracing_subscriber();

    m.add_class::<Dataplane>()?;
    m.add_class::<PacketReceiver>()?;
    m.add_class::<PyPayloadDelivery>()?;
    m.add_class::<PacketView>()?;
    m.add_class::<PacketBuilder>()?;

    Ok(())
}

fn parse_ipv4(addr: &str) -> PyResult<Ipv4Addr> {
    addr.parse::<Ipv4Addr>()
        .map_err(|e| PyRuntimeError::new_err(format!("invalid IPv4 address \"{addr}\": {e}")))
}
