mod buffer;

#[cfg(feature = "reliable")]
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "reliable")]
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use once_cell::sync::OnceCell;
use pyo3::conversion::IntoPyObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::PyModuleMethods;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
// macros like tracing::warn! can be used without importing `warn` specifically.
use tracing_subscriber::EnvFilter;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
use nextmini::node::controller::interface::ControllerInterfaceHandle;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::{
    PayloadDelivery as RustPayloadDelivery, PayloadFormat as RustPayloadFormat, PythonDelivery,
    PythonEvent, PythonFragmentationPolicy, PythonInterfaceHandle,
};
use nextmini::node::python::payload::{build_py_payload_segments, python_payload_budget};
#[cfg(feature = "reliable")]
use nextmini::node::reliable::api::ReliableHandle as RustReliableHandle;
#[cfg(feature = "reliable")]
use nextmini::node::reliable::session as reliable_session;
use nextmini::node::{NodeId, NodeIdExt};
#[cfg(feature = "reliable")]
use nextmini_messages::rlm as rlm_msg;
use nextmini_messages::DataplaneToController;

pub use crate::buffer::FrozenBuffer;

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
static TRACING: OnceCell<()> = OnceCell::new();

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

type RouteHopList = Vec<usize>;
type RouteInstallations = Vec<(usize, RouteHopList)>;

#[pyclass]
struct PacketReceiver {
    payload_only: bool,
    inner: Arc<Mutex<mpsc::Receiver<PythonDelivery>>>,
}

#[pymethods]
impl PacketReceiver {
    #[pyo3(signature = (timeout_ms=None))]
    fn recv(&self, timeout_ms: Option<u64>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = self.inner.clone();
        let payload_only = self.payload_only;
        let fut = async move {
            match timeout_ms {
                Some(ms) => tokio::time::timeout(
                    std::time::Duration::from_millis(ms),
                    inner.lock().await.recv(),
                )
                .await
                .unwrap_or_default(),
                None => inner.lock().await.recv().await,
            }
        };
        let maybe_delivery = rt().block_on(fut);
        maybe_delivery
            .map(|delivery| delivery_to_pyobject(py, payload_only, delivery))
            .transpose()
    }

    fn recv_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let payload_only = self.payload_only;
        future_into_py(py, async move {
            let delivery = inner.lock().await.recv().await;
            Python::attach(|py| {
                delivery
                    .map(|delivery| delivery_to_pyobject(py, payload_only, delivery))
                    .transpose()
            })
        })
    }
}

fn delivery_to_pyobject(
    py: Python<'_>,
    payload_only: bool,
    delivery: PythonDelivery,
) -> PyResult<Py<PyAny>> {
    match delivery {
        PythonDelivery::Raw(packet) => {
            if payload_only {
                warn_misrouted_payload();
            }
            let bytes = PyBytes::new(py, packet.bytes());
            Ok(bytes.into_pyobject(py)?.unbind().into())
        }
        PythonDelivery::Payload(payload) => {
            let obj = Py::new(py, PyPayloadDelivery::from(payload))?;
            Ok(obj.into_pyobject(py)?.unbind().into())
        }
    }
}

fn warn_misrouted_payload() {
    tracing::warn!(
        "PythonInterface: received raw packet for a payload-only receiver; delivering raw bytes."
    );
}

#[pyclass(name = "PayloadDelivery")]
struct PyPayloadDelivery {
    buffer: FrozenBuffer,
    flow_id: u128,
    src_ip: String,
    dst_ip: String,
    src_port: u16,
    dst_port: u16,
    message_id: Option<u64>,
    total_len: Option<u32>,
    fragment_count: Option<u16>,
    format: String,
}

#[pymethods]
impl PyPayloadDelivery {
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.buffer.inner.as_ref())
    }

    #[getter]
    fn frozen_payload(&self) -> FrozenBuffer {
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

    #[getter]
    fn payload_format(&self) -> &str {
        &self.format
    }
}

impl From<RustPayloadDelivery> for PyPayloadDelivery {
    fn from(payload: RustPayloadDelivery) -> Self {
        Self {
            buffer: FrozenBuffer::from_bytes(payload.bytes),
            flow_id: payload.flow_id,
            src_ip: payload.src_ip.to_string(),
            dst_ip: payload.dst_ip.to_string(),
            src_port: payload.src_port,
            dst_port: payload.dst_port,
            message_id: payload.message_id,
            total_len: payload.total_len,
            fragment_count: payload.fragment_count,
            format: match payload.payload_format {
                RustPayloadFormat::Payload => "payload".to_string(),
                RustPayloadFormat::RawPacket => "raw_packet".to_string(),
            },
        }
    }
}

// Removed legacy Python-side reliable multicast helpers and control/chunk encoders.

#[pyclass]
struct Dataplane {
    cfg: LocalConfig,
    py_if: PythonInterfaceHandle,
    processor: ProcessorHandle,
    controller: ControllerInterfaceHandle,
    _join: tokio::task::JoinHandle<()>,
    #[cfg(feature = "reliable")]
    reliable: Option<RustReliableHandle>,
    #[cfg(feature = "reliable")]
    session_registry: Arc<StdMutex<HashMap<(Ipv4Addr, usize), u64>>>,
}

impl Dataplane {
    #[cfg(feature = "reliable")]
    fn remember_session(&self, group_ip: Ipv4Addr, source_node_id: usize, session_id: u64) {
        if let Ok(mut guard) = self.session_registry.lock() {
            guard.insert((group_ip, source_node_id), session_id);
        }
    }

    #[cfg(feature = "reliable")]
    fn lookup_session(&self, group_ip: Ipv4Addr, source_node_id: usize) -> Option<u64> {
        self.session_registry
            .lock()
            .ok()
            .and_then(|guard| guard.get(&(group_ip, source_node_id)).copied())
    }
}

#[pymethods]
impl Dataplane {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_ip, receiver_ids, tensor_path, *, chunk_size=32768, src_port=None, dst_port=None, ack_policy="all", session_id=None))]
    fn reliable_send_file_rs(
        &self,
        group_ip: &str,
        receiver_ids: Vec<usize>,
        tensor_path: &str,
        chunk_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        ack_policy: &str,
        session_id: Option<u64>,
    ) -> PyResult<u64> {
        // Validate inputs early to surface helpful errors even while stubbed.
        #[allow(unused_variables)]
        let group_ip_addr = parse_ipv4(group_ip)?;
        if receiver_ids.is_empty() {
            return Err(PyRuntimeError::new_err(
                "receiver_ids must contain at least one entry.",
            ));
        }
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        let path = std::path::Path::new(tensor_path);
        if !path.exists() {
            return Err(PyRuntimeError::new_err(format!(
                "tensor_path does not exist: {}",
                tensor_path
            )));
        }
        if !(ack_policy == "all" || ack_policy.starts_with("k:") || ack_policy.starts_with("frac:"))
        {
            return Err(PyRuntimeError::new_err(format!(
                "invalid ack_policy: {ack_policy}"
            )));
        }
        let _ = (src_port, dst_port); // reserved for future plumbing
        #[allow(unused_mut)]
        let mut sid = session_id.unwrap_or_else(next_py_message_id);
        #[cfg(feature = "reliable")]
        {
            let frag_cfg = reliable_session::FragmentationConfig {
                enabled: self.cfg.python_fragmentation_enabled,
                max_message_bytes: self.cfg.python_fragmentation_max_message_bytes as usize,
                reassembly_window_bytes: self.cfg.python_fragmentation_reassembly_window_bytes
                    as usize,
                fragment_timeout_ms: self.cfg.python_fragmentation_fragment_timeout_ms as u64,
            };
            // Map ack_policy string to dataplane enum via messages helper.
            let ap = rlm_msg::parse_ack_policy(ack_policy).ok_or_else(|| {
                PyRuntimeError::new_err(format!("invalid ack_policy: {ack_policy}"))
            })?;
            let ack = match ap {
                rlm_msg::AckPolicy::All => reliable_session::AckPolicy::All,
                rlm_msg::AckPolicy::KofN(n) => reliable_session::AckPolicy::KofN(n as usize),
                rlm_msg::AckPolicy::Fraction(p) => reliable_session::AckPolicy::Fraction(p),
            };

            if let Some(handle) = &self.reliable {
                let reliable_cfg = &self.cfg.reliable;
                let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
                let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
                if session_id.is_none() {
                    sid = rt().block_on(handle.allocate_session_id());
                }
                // Build sender config and start session.
                let common = reliable_session::CommonConfig {
                    session_id: sid,
                    group_ip: group_ip_addr,
                    chunk_size,
                    src_port: sp,
                    dst_port: dp,
                    control_weight: reliable_cfg.control_weight,
                    data_bucket: reliable_cfg.data_bucket.clone(),
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
                    mtu: self.cfg.mtu,
                    fragmentation: frag_cfg.clone(),
                };
                let total_bytes = std::fs::metadata(tensor_path)
                    .map_err(|e| PyRuntimeError::new_err(format!("failed to stat file: {e}")))?
                    .len();
                let cfg = reliable_session::SenderConfig {
                    common,
                    receiver_ids,
                    total_bytes,
                    source_path: Some(tensor_path.to_string()),
                    checksum_out: false,
                    ack_policy: ack,
                    repair_backoff_ms: 10,
                    fec_k: None,
                    fec_p: 0,
                    ready_grace_ms: reliable_cfg.ready_grace_ms,
                };
                let started_sid = rt().block_on(handle.start_sender(cfg));
                self.remember_session(group_ip_addr, self.cfg.node_id, started_sid);
                return Ok(started_sid);
            }
        }
        // Fallback stub when feature is disabled or handle unavailable.
        tracing::warn!(
            "reliable_send_file_rs called (stub): sid={} group_ip={} receivers={:?} file={} chunk_size={} ack_policy={}",
            sid,
            group_ip,
            receiver_ids,
            tensor_path,
            chunk_size,
            ack_policy
        );
        Ok(sid)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_ip, source_node_id, expected_bytes, *, chunk_size=32768, src_port=None, dst_port=None, sink_path=None, session_id=None))]
    fn reliable_receive_file_rs(
        &self,
        group_ip: &str,
        source_node_id: usize,
        expected_bytes: u64,
        chunk_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        sink_path: Option<String>,
        session_id: Option<u64>,
    ) -> PyResult<u64> {
        #[allow(unused_variables)]
        let ip = parse_ipv4(group_ip)?;
        if expected_bytes == 0 {
            return Err(PyRuntimeError::new_err("expected_bytes must be positive."));
        }
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        let _ = (src_port, dst_port); // reserved for future plumbing
        let sid = session_id.unwrap_or_else(next_py_message_id);
        #[cfg(feature = "reliable")]
        {
            let frag_cfg = reliable_session::FragmentationConfig {
                enabled: self.cfg.python_fragmentation_enabled,
                max_message_bytes: self.cfg.python_fragmentation_max_message_bytes as usize,
                reassembly_window_bytes: self.cfg.python_fragmentation_reassembly_window_bytes
                    as usize,
                fragment_timeout_ms: self.cfg.python_fragmentation_fragment_timeout_ms as u64,
            };
            if let Some(handle) = &self.reliable {
                let reliable_cfg = &self.cfg.reliable;
                let mut resolved_sid = session_id;
                if resolved_sid.is_none() {
                    if let Some(known) = self.lookup_session(ip, source_node_id) {
                        resolved_sid = Some(known);
                    }
                }
                let common = reliable_session::CommonConfig {
                    session_id: resolved_sid.unwrap_or(0),
                    group_ip: ip,
                    chunk_size,
                    src_port: src_port.unwrap_or(self.cfg.user_space_client_port),
                    dst_port: dst_port.unwrap_or(self.cfg.user_space_server_port),
                    control_weight: reliable_cfg.control_weight,
                    data_bucket: reliable_cfg.data_bucket.clone(),
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
                    mtu: self.cfg.mtu,
                    fragmentation: frag_cfg.clone(),
                };
                let cfg = reliable_session::ReceiverConfig {
                    common,
                    source_node_id,
                    expected_bytes,
                    verify_checksum: false,
                    sink_path,
                    nack_min_interval_ms: reliable_cfg.nack_min_interval_ms,
                    nack_jitter_ms: reliable_cfg.nack_jitter_ms,
                    sack_interval_ms: reliable_cfg.sack_interval_ms,
                };
                let started_sid = if resolved_sid.is_some() {
                    rt().block_on(handle.start_receiver(cfg))
                } else {
                    let key = reliable_session::PendingReceiverKey {
                        group_ip: ip,
                        source_node_id,
                    };
                    rt().block_on(handle.start_receiver_pending(cfg, key))
                };
                self.remember_session(ip, source_node_id, started_sid);
                return Ok(started_sid);
            }
        }
        tracing::warn!(
            "reliable_receive_file_rs called (stub): sid={} group_ip={} src_node={} expected_bytes={} chunk_size={} sink_path={:?}",
            sid,
            group_ip,
            source_node_id,
            expected_bytes,
            chunk_size,
            sink_path
        );
        Ok(sid)
    }

    #[pyo3(signature = (session_id, timeout_ms=None))]
    fn reliable_wait(&self, session_id: u64, timeout_ms: Option<u64>) -> PyResult<bool> {
        #[cfg(feature = "reliable")]
        {
            if let Some(handle) = &self.reliable {
                let fut = handle.wait_completion(session_id);
                if let Some(ms) = timeout_ms {
                    let ok = rt().block_on(async move {
                        tokio::time::timeout(std::time::Duration::from_millis(ms), fut)
                            .await
                            .unwrap_or(false)
                    });
                    return Ok(ok);
                } else {
                    let ok = rt().block_on(fut);
                    return Ok(ok);
                }
            }
        }
        // feature disabled ⇒ nothing to wait for
        let _ = (session_id, timeout_ms);
        Ok(false)
    }

    #[cfg(feature = "reliable")]
    #[pyo3(signature = (group_ip, source_node_id, session_id))]
    fn reliable_register_session_id(
        &self,
        group_ip: &str,
        source_node_id: usize,
        session_id: u64,
    ) -> PyResult<()> {
        let ip = parse_ipv4(group_ip)?;
        self.remember_session(ip, source_node_id, session_id);
        Ok(())
    }

    #[cfg(feature = "reliable")]
    #[pyo3(signature = (group_ip, source_node_id))]
    fn reliable_lookup_session_id(
        &self,
        group_ip: &str,
        source_node_id: usize,
    ) -> PyResult<Option<u64>> {
        let ip = parse_ipv4(group_ip)?;
        Ok(self.lookup_session(ip, source_node_id))
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
        #[cfg(feature = "reliable")]
        let reliable = conductor.reliable_handle();

        // Enter the bindings runtime so tokio::spawn inside PythonInterfaceHandle::new succeeds.
        let py_if = {
            let _rt_guard = rt().enter();
            PythonInterfaceHandle::new(
                cfg.channel_capacity,
                PythonFragmentationPolicy::from(&cfg),
                Some((controller.clone(), cfg.node_id)),
            )
        };
        processor.connect_python_interface(py_if.clone());
        rt().block_on(controller.attach_python_interface(py_if.clone()));

        let join = rt().spawn(async move {
            conductor.run().await;
        });

        #[cfg(feature = "reliable")]
        let session_registry = Arc::new(StdMutex::new(HashMap::new()));

        Ok(Self {
            cfg,
            py_if,
            processor,
            controller,
            _join: join,
            #[cfg(feature = "reliable")]
            reliable,
            #[cfg(feature = "reliable")]
            session_registry,
        })
    }

    #[pyo3(signature = (src_node_id, dst_node_id, src_port=None, dst_port=None))]
    fn flow_id_from_nodes(
        &self,
        src_node_id: usize,
        dst_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u128> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        Ok(Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp))
    }

    #[pyo3(signature = (src_node_id, src_port=None, dst_port=None, payload_only=false))]
    fn register_receiver_from_node(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        payload_only: bool,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = self.cfg.user_space_address;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id, payload_only));
        Py::new(
            py,
            PacketReceiver {
                payload_only,
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (src_node_id, group_ip, src_port=None, dst_port=None, payload_only=false))]
    fn register_receiver_for_group(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        group_ip: &str,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        payload_only: bool,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = parse_ipv4(group_ip)?;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id, payload_only));
        Py::new(
            py,
            PacketReceiver {
                payload_only,
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (dst_node_id, frozen, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        dst_node_id: usize,
        frozen: FrozenBuffer,
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

    #[pyo3(signature = (dst_ip, frozen, src_port=None, dst_port=None))]
    fn send_to_ip(
        &self,
        dst_ip: &str,
        frozen: FrozenBuffer,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        let body = frozen.inner.clone();
        let dst_ip = parse_ipv4(dst_ip)?;
        let src_ip = self.cfg.user_space_address;
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

    #[pyo3(signature = (group_id, src_node_id=None, timeout_ms=None))]
    fn wait_for_routes_installed(
        &self,
        group_id: usize,
        src_node_id: Option<usize>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<RouteInstallations>> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(
                event,
                PythonEvent::GroupRoutesInstalled {
                    group_id: gid,
                    src_node_id: event_src_node_id,
                    ..
                } if *gid == group_id
                    && src_node_id.is_none_or(|target| target == *event_src_node_id)
            )
        });

        let routes: Option<RouteInstallations> = matched.map(|event| match event {
            PythonEvent::GroupRoutesInstalled { routes, .. } => routes
                .into_iter()
                .map(|entry| (entry.route_id, entry.next_hops))
                .collect(),
            _ => unreachable!("matched variant should be routes installed"),
        });

        Ok(routes)
    }

    #[pyo3(signature = (dst_node_id, frozen_buffers, src_port=None, dst_port=None))]
    fn send_batch_to_node(
        &self,
        dst_node_id: usize,
        frozen_buffers: Vec<FrozenBuffer>,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        for frozen in frozen_buffers {
            self.send_to_node(dst_node_id, frozen, src_port, dst_port)?;
        }
        Ok(())
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
        if !self.cfg.python_fragmentation_enabled {
            let packet = Packet::build_ipv4_tcp_packet(src_ip, src_port, dst_ip, dst_port, &body);
            self.processor.process_packet(packet);
            return Ok(message_id);
        }

        let chunk_budget = python_payload_budget(self.cfg.mtu)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let max_message_bytes = self.cfg.python_fragmentation_max_message_bytes as usize;

        let fragments =
            build_py_payload_segments(body.as_ref(), chunk_budget, max_message_bytes, message_id)
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        for fragment in fragments {
            let packet =
                Packet::build_ipv4_tcp_packet(src_ip, src_port, dst_ip, dst_port, &fragment);
            self.processor.process_packet(packet);
        }

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
        let mut backlog = Vec::new();

        loop {
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    self.requeue_events(backlog);
                    return None;
                }
            }

            let remaining = deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
            let event = match self.recv_event_with_timeout(remaining) {
                Some(event) => event,
                None => {
                    self.requeue_events(backlog);
                    return None;
                }
            };

            if matcher(&event) {
                self.requeue_events(backlog);
                return Some(event);
            } else {
                backlog.push(event);
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

    fn requeue_events(&self, backlog: Vec<PythonEvent>) {
        for event in backlog.into_iter() {
            self.emit_python_event(event);
        }
    }
}

fn next_py_message_id() -> u64 {
    PY_MESSAGE_ID_SEQ.fetch_add(1, Ordering::Relaxed)
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_tracing_subscriber();
    m.add_class::<Dataplane>()?;
    m.add_class::<PacketReceiver>()?;
    m.add_class::<PyPayloadDelivery>()?;
    m.add_class::<FrozenBuffer>()?;
    Ok(())
}

fn parse_ipv4(addr: &str) -> PyResult<Ipv4Addr> {
    addr.parse::<Ipv4Addr>()
        .map_err(|e| PyRuntimeError::new_err(format!("invalid IPv4 address \"{addr}\": {e}")))
}
