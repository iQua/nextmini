mod buffer;

use std::net::Ipv4Addr;
//
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
use nextmini::node::controller::interface::ControllerInterfaceHandle;
use nextmini::node::packet::{Packet, PyPayloadSegHeader, PY_PAYLOAD_SEGMENT_HEADER_LEN};
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::{
    PayloadDelivery as RustPayloadDelivery, PayloadFormat as RustPayloadFormat, PythonDelivery,
    PythonEvent, PythonFragmentationPolicy, PythonInterfaceHandle,
};
use nextmini::node::{NodeId, NodeIdExt};
use nextmini_messages::DataplaneToController;
#[cfg(feature = "reliable")]
use nextmini::node::reliable::api::ReliableHandle as RustReliableHandle;
#[cfg(feature = "reliable")]
use nextmini::node::reliable::session as reliable_session;
#[cfg(feature = "reliable")]
use nextmini_messages::rlm as rlm_msg;

pub use crate::buffer::FrozenBuffer;

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

fn rt() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("nextmini-py")
            .build()
            .expect("unable to create tokio runtime for nextmini_py")
    })
}

const IPV4_HEADER_LEN: usize = 20;
const TCP_HEADER_LEN: usize = 20;
const PY_MIN_FRAGMENTATION_MTU: usize =
    IPV4_HEADER_LEN + TCP_HEADER_LEN + PY_PAYLOAD_SEGMENT_HEADER_LEN + 1;

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
}

#[pymethods]
impl Dataplane {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_ip, receiver_ids, tensor_path, *, chunk_size=32768, src_port=None, dst_port=None, ack_policy="all"))]
    fn reliable_send_file_rs(
        &self,
        group_ip: &str,
        receiver_ids: Vec<usize>,
        tensor_path: &str,
        chunk_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        ack_policy: &str,
    ) -> PyResult<u64> {
        // Validate inputs early to surface helpful errors even while stubbed.
        let _ip = parse_ipv4(group_ip)?;
        if receiver_ids.is_empty() {
            return Err(PyRuntimeError::new_err("receiver_ids must contain at least one entry."));
        }
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        let path = std::path::Path::new(tensor_path);
        if !path.exists() {
            return Err(PyRuntimeError::new_err(format!("tensor_path does not exist: {}", tensor_path)));
        }
        if !(ack_policy == "all"
            || ack_policy.starts_with("k:")
            || ack_policy.starts_with("frac:"))
        {
            return Err(PyRuntimeError::new_err(format!(
                "invalid ack_policy: {ack_policy}"
            )));
        }
        let _ = (src_port, dst_port); // reserved for future plumbing
        let sid = next_py_message_id();
        #[cfg(feature = "reliable")]
        {
            // Map ack_policy string to dataplane enum via messages helper.
            let ap = rlm_msg::parse_ack_policy(ack_policy)
                .ok_or_else(|| PyRuntimeError::new_err(format!("invalid ack_policy: {ack_policy}")))?;
            let ack = match ap {
                rlm_msg::AckPolicy::All => reliable_session::AckPolicy::All,
                rlm_msg::AckPolicy::KofN(n) => reliable_session::AckPolicy::KofN(n as usize),
                rlm_msg::AckPolicy::Fraction(p) => reliable_session::AckPolicy::Fraction(p),
            };

            if let Some(handle) = &self.reliable {
                // Build sender config and start session. For now, we keep a minimal baseline.
                let common = reliable_session::CommonConfig {
                    session_id: sid,
                    group_ip: parse_ipv4(group_ip)?,
                    chunk_size,
                    src_port: src_port.unwrap_or(self.cfg.user_space_client_port),
                    dst_port: dst_port.unwrap_or(self.cfg.user_space_server_port),
                    control_weight: 10,
                    data_bucket: None,
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
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
                    sack_interval_ms: 100,
                    repair_backoff_ms: 10,
                    fec_k: None,
                    fec_p: 0,
                };
                // Synchronous start for a session id; actual data plane runs in background.
                let started_sid = rt().block_on(handle.start_sender(cfg));
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
    #[pyo3(signature = (group_ip, source_node_id, expected_bytes, *, chunk_size=32768, src_port=None, dst_port=None, sink_path=None))]
    fn reliable_receive_file_rs(
        &self,
        group_ip: &str,
        source_node_id: usize,
        expected_bytes: u64,
        chunk_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
        sink_path: Option<String>,
    ) -> PyResult<u64> {
        let _ip = parse_ipv4(group_ip)?;
        if expected_bytes == 0 {
            return Err(PyRuntimeError::new_err("expected_bytes must be positive."));
        }
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        let _ = (src_port, dst_port); // reserved for future plumbing
        let sid = next_py_message_id();
        #[cfg(feature = "reliable")]
        {
            if let Some(handle) = &self.reliable {
                let common = reliable_session::CommonConfig {
                    session_id: sid,
                    group_ip: parse_ipv4(group_ip)?,
                    chunk_size,
                    src_port: src_port.unwrap_or(self.cfg.user_space_client_port),
                    dst_port: dst_port.unwrap_or(self.cfg.user_space_server_port),
                    control_weight: 10,
                    data_bucket: None,
                    local_node_id: self.cfg.node_id,
                    user_space_base_addr: self.cfg.user_space_base_addr,
                    local_netmask: self.cfg.local_netmask,
                };
                let cfg = reliable_session::ReceiverConfig {
                    common,
                    source_node_id,
                    expected_bytes,
                    verify_checksum: false,
                    sink_path,
                    nack_min_interval_ms: 5,
                    nack_jitter_ms: 3,
                };
                let started_sid = rt().block_on(handle.start_receiver(cfg));
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
                            .unwrap_or(Ok(false))
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

        Ok(Self {
            cfg,
            py_if,
            processor,
            controller,
            _join: join,
            #[cfg(feature = "reliable")]
            reliable,
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

    // Removed legacy send_file_reliable Python entrypoint.

    // Removed legacy receive_file_reliable Python entrypoint.
}

impl Dataplane {
    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn run_reliable_sender(
        &self,
        group_ip: Ipv4Addr,
        receiver_ids: &[usize],
        tensor_path: &Path,
        chunk_size: usize,
        flow_window: usize,
        poll_interval: Duration,
        ready_timeout: Duration,
        src_port: u16,
        dst_port: u16,
        sleep_gap: Duration,
        checksum_path: Option<&Path>,
        write_checksum: bool,
    ) -> PyResult<ReliableSendReport> {
        if receiver_ids.is_empty() {
            return Err(PyRuntimeError::new_err(
                "receiver_ids must contain at least one entry.",
            ));
        }
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        if flow_window == 0 {
            return Err(PyRuntimeError::new_err("flow_window must be positive."));
        }
        if write_checksum && checksum_path.is_none() {
            return Err(PyRuntimeError::new_err(
                "checksum_path is required when write_checksum is enabled.",
            ));
        }

        let file = File::open(tensor_path).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "failed to open tensor file {}: {e}",
                tensor_path.display()
            ))
        })?;
        let total_bytes = file
            .metadata()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to stat file: {e}")))?
            .len();
        if total_bytes == 0 {
            return Err(PyRuntimeError::new_err("tensor file is empty."));
        }
        let total_chunks = chunk_count(total_bytes, chunk_size);
        if total_chunks == 0 {
            return Err(PyRuntimeError::new_err(
                "computed zero chunks for provided tensor.",
            ));
        }

        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; chunk_size];

        let mut control_receivers =
            self.register_control_receivers(receiver_ids, src_port, dst_port)?;
        let mut ready_nodes = HashSet::new();
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        let mut chunk_cache: HashMap<u64, Bytes> = HashMap::new();
        let mut resend_queue: BTreeSet<u64> = BTreeSet::new();
        let mut resends = 0u64;

        let ready_deadline = if ready_timeout.is_zero() {
            None
        } else {
            Some(Instant::now() + ready_timeout)
        };
        while ready_nodes.len() < receiver_ids.len() {
            self.flush_control_messages(
                &mut control_receivers,
                poll_interval,
                &mut ready_nodes,
                &mut inflight,
                &mut chunk_cache,
                &mut resend_queue,
                receiver_ids.len(),
            )?;
            if let Some(deadline) = ready_deadline {
                if Instant::now() > deadline {
                    return Err(PyRuntimeError::new_err(
                        "timed out waiting for receivers to report ready state.",
                    ));
                }
            }
            if poll_interval.is_zero() {
                thread::yield_now();
            } else {
                thread::sleep(poll_interval);
            }
        }

        let mut chunk_index = 1u64;
        let mut bytes_sent = 0u64;
        let start = Instant::now();
        let mut sha = if write_checksum {
            Some(Sha256::new())
        } else {
            None
        };

        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|e| PyRuntimeError::new_err(format!("failed to read tensor: {e}")))?;
            if read == 0 {
                break;
            }

            self.wait_for_window(
                flow_window,
                &mut inflight,
                &mut control_receivers,
                poll_interval,
                &mut ready_nodes,
                &mut chunk_cache,
                &mut resend_queue,
                receiver_ids.len(),
            )?;

            let payload = encode_chunk_payload(chunk_index, &buffer[..read]);
            self.transmit_python_payload(
                self.cfg.user_space_address,
                group_ip,
                src_port,
                dst_port,
                payload.clone(),
            )?;
            inflight.insert(chunk_index, HashSet::new());
            chunk_cache.insert(chunk_index, payload);
            bytes_sent += read as u64;
            if let Some(digest) = sha.as_mut() {
                digest.update(&buffer[..read]);
            }

            self.flush_control_messages(
                &mut control_receivers,
                Duration::from_millis(0),
                &mut ready_nodes,
                &mut inflight,
                &mut chunk_cache,
                &mut resend_queue,
                receiver_ids.len(),
            )?;
            resends += self.process_resends(
                &mut resend_queue,
                &chunk_cache,
                group_ip,
                src_port,
                dst_port,
            )?;

            if !sleep_gap.is_zero() {
                thread::sleep(sleep_gap);
            }
            chunk_index += 1;
        }

        while !inflight.is_empty() {
            self.flush_control_messages(
                &mut control_receivers,
                poll_interval,
                &mut ready_nodes,
                &mut inflight,
                &mut chunk_cache,
                &mut resend_queue,
                receiver_ids.len(),
            )?;
            resends += self.process_resends(
                &mut resend_queue,
                &chunk_cache,
                group_ip,
                src_port,
                dst_port,
            )?;
            if poll_interval.is_zero() {
                thread::yield_now();
            } else {
                thread::sleep(poll_interval);
            }
        }

        if bytes_sent != total_bytes {
            return Err(PyRuntimeError::new_err(format!(
                "bytes sent ({bytes_sent}) did not match tensor ({total_bytes})."
            )));
        }

        let checksum_hex = if let Some(digest) = sha {
            let hex = format!("{:x}", digest.finalize());
            if let Some(path) = checksum_path {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "failed to create checksum parent dir {}: {e}",
                            parent.display()
                        ))
                    })?;
                }
                fs::write(path, format!("{hex}\n")).map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "failed to write checksum file {}: {e}",
                        path.display()
                    ))
                })?;
            }
            Some(hex)
        } else {
            None
        };

        Ok(ReliableSendReport {
            bytes_sent,
            chunks_sent: total_chunks,
            resends,
            duration_ms: start.elapsed().as_millis() as u64,
            checksum: checksum_hex,
        })
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn run_reliable_receiver(
        &self,
        group_ip: Ipv4Addr,
        source_node_id: usize,
        expected_bytes: u64,
        chunk_size: usize,
        receive_timeout: Duration,
        src_port: u16,
        dst_port: u16,
        sink_path: Option<&Path>,
        verify_checksum: bool,
        checksum_path: Option<&Path>,
    ) -> PyResult<ReliableReceiveReport> {
        if chunk_size == 0 {
            return Err(PyRuntimeError::new_err("chunk_size must be positive."));
        }
        if expected_bytes == 0 {
            return Err(PyRuntimeError::new_err(
                "expected_bytes must be greater than zero.",
            ));
        }
        if verify_checksum && checksum_path.is_none() {
            return Err(PyRuntimeError::new_err(
                "checksum_path is required when verify_checksum is enabled.",
            ));
        }

        let expected_chunks = chunk_count(expected_bytes, chunk_size);
        if expected_chunks == 0 {
            return Err(PyRuntimeError::new_err(
                "computed zero chunks for expected bytes.",
            ));
        }

        let src_ip = (source_node_id as NodeId)
            .ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let flow_id = Packet::flow_id_from_parts(src_ip, src_port, group_ip, dst_port);
        let mut data_receiver = rt().block_on(self.py_if.register_receiver(flow_id, true));

        self.send_control_signal(source_node_id, ControlKind::Ready, 0, src_port, dst_port)?;

        let mut sink_file = if let Some(path) = sink_path {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "failed to create sink parent dir {}: {e}",
                        parent.display()
                    ))
                })?;
            }
            Some(File::create(path).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "failed to create sink file {}: {e}",
                    path.display()
                ))
            })?)
        } else {
            None
        };

        let mut pending_chunks: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut expected_chunk = 1u64;
        let mut bytes_received = 0u64;
        let mut repairs_requested = 0u64;
        let start = Instant::now();
        let mut sha = if verify_checksum {
            Some(Sha256::new())
        } else {
            None
        };

        while expected_chunk <= expected_chunks {
            let maybe_delivery = self.recv_with_timeout(&mut data_receiver, receive_timeout)?;
            let delivery = match maybe_delivery {
                Some(delivery) => delivery,
                None => {
                    self.send_control_signal(
                        source_node_id,
                        ControlKind::Repair,
                        expected_chunk,
                        src_port,
                        dst_port,
                    )?;
                    repairs_requested += 1;
                    continue;
                }
            };

            let payload = match delivery {
                PythonDelivery::Payload(payload) => payload,
                PythonDelivery::Raw(_) => {
                    warn!("reliability receiver expected payload delivery but saw raw packet");
                    continue;
                }
            };

            let (chunk_index, chunk_bytes) = decode_chunk_payload(&payload.bytes)?;
            self.send_control_signal(
                source_node_id,
                ControlKind::Ack,
                chunk_index,
                src_port,
                dst_port,
            )?;

            if chunk_index < expected_chunk {
                continue;
            }
            if chunk_index > expected_chunks {
                warn!(
                    "dropping chunk {} beyond expected target {}",
                    chunk_index, expected_chunks
                );
                continue;
            }

            pending_chunks.insert(chunk_index, chunk_bytes);
            loop {
                match pending_chunks.remove(&expected_chunk) {
                    Some(bytes) => {
                        if let Some(file) = sink_file.as_mut() {
                            file.write_all(bytes.as_ref()).map_err(|e| {
                                PyRuntimeError::new_err(format!("failed to write sink bytes: {e}"))
                            })?;
                        }
                        if let Some(digest) = sha.as_mut() {
                            digest.update(bytes.as_ref());
                        }
                        bytes_received += bytes.len() as u64;
                        expected_chunk += 1;
                    }
                    None => break,
                }
            }
        }

        if let Some(file) = sink_file.as_mut() {
            file.flush()
                .map_err(|e| PyRuntimeError::new_err(format!("flush failed: {e}")))?;
        }

        let checksum_hex = if let Some(digest) = sha {
            let hex = format!("{:x}", digest.finalize());
            if let Some(path) = checksum_path {
                let expected = fs::read_to_string(path).map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "failed to read checksum file {}: {e}",
                        path.display()
                    ))
                })?;
                let expected = expected.trim();
                if expected != hex {
                    return Err(PyRuntimeError::new_err(format!(
                        "checksum mismatch: expected {expected}, received {hex}",
                    )));
                }
            }
            Some(hex)
        } else {
            None
        };

        Ok(ReliableReceiveReport {
            bytes_received,
            chunks_received: expected_chunks,
            repairs_requested,
            duration_ms: start.elapsed().as_millis() as u64,
            checksum: checksum_hex,
        })
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn register_control_receivers(
        &self,
        receiver_ids: &[usize],
        src_port: u16,
        dst_port: u16,
    ) -> PyResult<HashMap<usize, mpsc::Receiver<PythonDelivery>>> {
        let mut map = HashMap::new();
        for node_id in receiver_ids {
            let src_ip =
                (*node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
            let dst_ip = self.cfg.user_space_address;
            let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
            let rx = rt().block_on(self.py_if.register_receiver(flow_id, true));
            map.insert(*node_id, rx);
        }
        Ok(map)
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn wait_for_window(
        &self,
        flow_window: usize,
        inflight: &mut BTreeMap<u64, HashSet<usize>>,
        control_receivers: &mut HashMap<usize, mpsc::Receiver<PythonDelivery>>,
        poll_interval: Duration,
        ready_nodes: &mut HashSet<usize>,
        chunk_cache: &mut HashMap<u64, Bytes>,
        resend_queue: &mut BTreeSet<u64>,
        receiver_count: usize,
    ) -> PyResult<()> {
        while inflight.len() >= flow_window {
            self.flush_control_messages(
                control_receivers,
                poll_interval,
                ready_nodes,
                inflight,
                chunk_cache,
                resend_queue,
                receiver_count,
            )?;
            if poll_interval.is_zero() {
                thread::yield_now();
            } else {
                thread::sleep(poll_interval);
            }
        }
        Ok(())
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn flush_control_messages(
        &self,
        receivers: &mut HashMap<usize, mpsc::Receiver<PythonDelivery>>,
        timeout: Duration,
        ready_nodes: &mut HashSet<usize>,
        inflight: &mut BTreeMap<u64, HashSet<usize>>,
        chunk_cache: &mut HashMap<u64, Bytes>,
        resend_queue: &mut BTreeSet<u64>,
        receiver_count: usize,
    ) -> PyResult<()> {
        let events = self.poll_control_events(receivers, timeout)?;
        for event in events {
            match event.kind {
                ControlKind::Ready => {
                    ready_nodes.insert(event.node_id);
                }
                ControlKind::Ack => {
                    if let Some(entry) = inflight.get_mut(&event.chunk_index) {
                        entry.insert(event.node_id);
                        if entry.len() == receiver_count {
                            inflight.remove(&event.chunk_index);
                            chunk_cache.remove(&event.chunk_index);
                        }
                    }
                }
                ControlKind::Repair => {
                    if chunk_cache.contains_key(&event.chunk_index) {
                        resend_queue.insert(event.chunk_index);
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn poll_control_events(
        &self,
        receivers: &mut HashMap<usize, mpsc::Receiver<PythonDelivery>>,
        timeout: Duration,
    ) -> PyResult<Vec<ControlMessage>> {
        let mut events = Vec::new();
        for receiver in receivers.values_mut() {
            if let Some(delivery) = self.recv_with_timeout(receiver, timeout)? {
                events.push(self.control_from_delivery(delivery)?);
            }
            loop {
                match receiver.try_recv() {
                    Ok(delivery) => events.push(self.control_from_delivery(delivery)?),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
        Ok(events)
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn process_resends(
        &self,
        resend_queue: &mut BTreeSet<u64>,
        chunk_cache: &HashMap<u64, Bytes>,
        group_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
    ) -> PyResult<u64> {
        if resend_queue.is_empty() {
            return Ok(0);
        }
        let indices: Vec<u64> = resend_queue.iter().copied().collect();
        resend_queue.clear();
        let mut resent = 0u64;
        for chunk_index in indices {
            if let Some(payload) = chunk_cache.get(&chunk_index) {
                self.transmit_python_payload(
                    self.cfg.user_space_address,
                    group_ip,
                    src_port,
                    dst_port,
                    payload.clone(),
                )?;
                resent += 1;
            }
        }
        Ok(resent)
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn control_from_delivery(&self, delivery: PythonDelivery) -> PyResult<ControlMessage> {
        match delivery {
            PythonDelivery::Payload(payload) => decode_control_payload(payload.bytes.as_ref()),
            PythonDelivery::Raw(_) => Err(PyRuntimeError::new_err(
                "expected payload delivery for control channel",
            )),
        }
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn send_control_signal(
        &self,
        dst_node_id: usize,
        kind: ControlKind,
        chunk_index: u64,
        src_port: u16,
        dst_port: u16,
    ) -> PyResult<()> {
        let payload = encode_control_payload(kind, self.cfg.node_id, chunk_index);
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        self.transmit_python_payload(
            self.cfg.user_space_address,
            dst_ip,
            src_port,
            dst_port,
            payload,
        )
        .map(|_| ())
    }

    #[cfg(feature = "legacy_py_reliable")]
    #[allow(dead_code)]
    fn recv_with_timeout(
        &self,
        receiver: &mut mpsc::Receiver<PythonDelivery>,
        timeout: Duration,
    ) -> PyResult<Option<PythonDelivery>> {
        if timeout.is_zero() {
            return match receiver.try_recv() {
                Ok(delivery) => Ok(Some(delivery)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => Ok(None),
            };
        }

        let fut = receiver.recv();
        let result = rt().block_on(async { tokio::time::timeout(timeout, fut).await });
        match result {
            Ok(delivery) => Ok(delivery),
            Err(_) => Ok(None),
        }
    }

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

        let chunk_budget = python_payload_budget(self.cfg.mtu)?;
        let max_message_bytes = self.cfg.python_fragmentation_max_message_bytes as usize;

        let fragments =
            build_py_payload_segments(body.as_ref(), chunk_budget, max_message_bytes, message_id)?;
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

fn python_payload_budget(mtu: i32) -> PyResult<usize> {
    let mtu_value = usize::try_from(mtu).map_err(|_| {
        PyRuntimeError::new_err(format!(
            "configured MTU {mtu} is invalid; expected positive value."
        ))
    })?;
    let header_overhead = IPV4_HEADER_LEN + TCP_HEADER_LEN + PY_PAYLOAD_SEGMENT_HEADER_LEN;
    match mtu_value.checked_sub(header_overhead) {
        Some(0) | None => Err(PyRuntimeError::new_err(format!(
            "configured MTU {mtu} is below the minimum {} required for Python fragmentation \
             (must exceed {} bytes of IPv4/TCP/PyPayloadSeg headers).",
            PY_MIN_FRAGMENTATION_MTU, header_overhead
        ))),
        Some(budget) => Ok(budget),
    }
}

fn next_py_message_id() -> u64 {
    PY_MESSAGE_ID_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn build_py_payload_segments(
    body: &[u8],
    chunk_budget: usize,
    max_message_bytes: usize,
    message_id: u64,
) -> PyResult<Vec<Vec<u8>>> {
    if chunk_budget == 0 {
        return Err(PyRuntimeError::new_err(
            "python payload chunk budget is zero; increase MTU to enable fragmentation.",
        ));
    }

    if body.len() > max_message_bytes {
        return Err(PyRuntimeError::new_err(format!(
            "python buffer length {} exceeds configured limit of {} bytes.",
            body.len(),
            max_message_bytes
        )));
    }

    if body.len() > u32::MAX as usize {
        return Err(PyRuntimeError::new_err(format!(
            "python buffer length {} exceeds 4 GiB limit for a single message.",
            body.len()
        )));
    }

    let fragment_count = if body.is_empty() {
        1
    } else {
        body.len().div_ceil(chunk_budget)
    };

    if fragment_count > u16::MAX as usize {
        return Err(PyRuntimeError::new_err(format!(
            "python buffer requires {} fragments which exceeds the limit of {}; \
             increase MTU or reduce payload size.",
            fragment_count,
            u16::MAX
        )));
    }

    let total_len = body.len() as u32;
    let fragment_count_u16 = fragment_count as u16;
    let mut fragments = Vec::with_capacity(fragment_count);

    let is_fragmented = fragment_count > 1;

    for idx in 0..fragment_count {
        let (chunk_start, chunk_end) = if body.is_empty() {
            (0, 0)
        } else {
            let start = idx * chunk_budget;
            let end = std::cmp::min(start + chunk_budget, body.len());
            (start, end)
        };
        let chunk = &body[chunk_start..chunk_end];
        let header = PyPayloadSegHeader {
            fragmented: is_fragmented,
            last_fragment: is_fragmented && idx + 1 == fragment_count,
            message_id,
            total_len,
            fragment_index: idx as u16,
            fragment_count: fragment_count_u16,
            fragment_payload_len: chunk.len() as u32,
        };

        let mut payload = vec![0u8; PY_PAYLOAD_SEGMENT_HEADER_LEN + chunk.len()];
        header
            .encode_into(&mut payload[..PY_PAYLOAD_SEGMENT_HEADER_LEN])
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        payload[PY_PAYLOAD_SEGMENT_HEADER_LEN..].copy_from_slice(chunk);

        fragments.push(payload);
    }

    Ok(fragments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_payload_budget_accounts_for_headers() {
        assert_eq!(python_payload_budget(1400).unwrap(), 1336);
    }

    #[test]
    fn python_payload_budget_minimum_valid_mtu() {
        // Minimum working MTU is 65 (64 + 1 byte payload)
        assert_eq!(python_payload_budget(65).unwrap(), 1);
    }

    #[test]
    fn python_payload_budget_exactly_at_header_boundary_errors() {
        // MTU=64 leaves 0 bytes for payload with actual 64-byte overhead
        let err = python_payload_budget(64).unwrap_err();
        assert!(err.to_string().contains("below the minimum"));
    }

    #[test]
    fn python_payload_budget_negative_mtu_errors() {
        let err = python_payload_budget(-1).unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn python_payload_budget_standard_ethernet_mtu() {
        // Standard 1500 MTU Ethernet → 1500 - 64 = 1436
        assert_eq!(python_payload_budget(1500).unwrap(), 1436);
    }

    #[test]
    fn python_payload_budget_jumbo_frame_mtu() {
        // Jumbo frame (9000 bytes) → 9000 - 64 = 8936
        assert_eq!(python_payload_budget(9000).unwrap(), 8936);
    }

    #[test]
    fn build_segments_single_fragment_sets_header() {
        let payload = vec![0xAA; 512];
        let segments = build_py_payload_segments(&payload, 1024, 4096, 42).expect("segments");
        assert_eq!(segments.len(), 1);
        let fragment = &segments[0];
        assert_eq!(
            fragment.len(),
            PY_PAYLOAD_SEGMENT_HEADER_LEN + payload.len()
        );
        let (header, body) = PyPayloadSegHeader::decode_from(fragment).expect("header");
        assert!(!header.is_fragmented());
        assert!(!header.is_last_fragment());
        assert_eq!(header.message_id, 42);
        assert_eq!(header.total_len as usize, payload.len());
        assert_eq!(header.fragment_index, 0);
        assert_eq!(header.fragment_count, 1);
        assert_eq!(header.fragment_payload_len as usize, payload.len());
        assert_eq!(body, payload.as_slice());
    }

    #[test]
    fn build_segments_multi_fragment_sets_flags_and_counts() {
        let payload: Vec<u8> = (0..3500).map(|i| (i % 256) as u8).collect();
        let message_id = 7;
        let segments =
            build_py_payload_segments(&payload, 1000, 4096, message_id).expect("segments");
        assert_eq!(segments.len(), 4);

        for (idx, fragment) in segments.iter().enumerate() {
            let (header, body) =
                PyPayloadSegHeader::decode_from(fragment).expect("fragment header");

            // Validate fragmentation flags
            assert!(header.is_fragmented());
            if idx == segments.len() - 1 {
                assert!(header.is_last_fragment());
            } else {
                assert!(!header.is_last_fragment());
            }

            // Validate fragment indices
            assert_eq!(header.fragment_index, idx as u16);
            assert_eq!(header.fragment_count, segments.len() as u16);

            // Validate consistent metadata across all fragments
            assert_eq!(
                header.message_id, message_id,
                "message_id should be consistent"
            );
            assert_eq!(
                header.total_len as usize,
                payload.len(),
                "total_len should match original payload"
            );

            // Validate body content
            let start = idx * 1000;
            let end = std::cmp::min(start + 1000, payload.len());
            assert_eq!(body, &payload[start..end]);
        }
    }

    #[test]
    fn build_segments_errors_when_payload_exceeds_limit() {
        let payload = vec![0xCC; 10];
        let err = build_py_payload_segments(&payload, 8, 5, 1).unwrap_err();
        assert!(err
            .to_string()
            .contains("exceeds configured limit of 5 bytes"));
    }

    #[test]
    fn build_segments_empty_payload_creates_single_fragment() {
        let payload = vec![];
        let segments = build_py_payload_segments(&payload, 1024, 4096, 99).expect("segments");
        assert_eq!(segments.len(), 1);
        let (header, body) = PyPayloadSegHeader::decode_from(&segments[0]).expect("header");
        assert!(!header.is_fragmented());
        assert!(!header.is_last_fragment());
        assert_eq!(header.message_id, 99);
        assert_eq!(header.total_len, 0);
        assert_eq!(header.fragment_count, 1);
        assert_eq!(header.fragment_payload_len, 0);
        assert_eq!(body.len(), 0);
    }

    #[test]
    fn build_segments_exactly_at_chunk_boundary() {
        let payload = vec![0xDD; 2000];
        let segments = build_py_payload_segments(&payload, 1000, 4096, 15).expect("segments");
        assert_eq!(segments.len(), 2);

        let (first_header, first_body) =
            PyPayloadSegHeader::decode_from(&segments[0]).expect("first");
        assert!(first_header.is_fragmented());
        assert!(!first_header.is_last_fragment());
        assert_eq!(first_header.fragment_index, 0);
        assert_eq!(first_header.fragment_count, 2);
        assert_eq!(first_body.len(), 1000);

        let (second_header, second_body) =
            PyPayloadSegHeader::decode_from(&segments[1]).expect("second");
        assert!(second_header.is_fragmented());
        assert!(second_header.is_last_fragment());
        assert_eq!(second_header.fragment_index, 1);
        assert_eq!(second_body.len(), 1000);
    }

    #[test]
    fn build_segments_errors_when_chunk_budget_is_zero() {
        let payload = vec![0xEE; 100];
        let err = build_py_payload_segments(&payload, 0, 4096, 1).unwrap_err();
        assert!(err.to_string().contains("chunk budget is zero"));
    }

    #[test]
    fn build_segments_errors_when_payload_exceeds_u32_max() {
        let payload_size = (u32::MAX as usize) + 1;
        let payload = vec![0xFF; payload_size];
        let err = build_py_payload_segments(&payload, 1024, usize::MAX, 1).unwrap_err();
        assert!(err.to_string().contains("exceeds 4 GiB limit"));
    }

    #[test]
    fn build_segments_errors_when_fragment_count_exceeds_u16_max() {
        let chunk_budget = 1;
        let payload_size = (u16::MAX as usize) + 1;
        let payload = vec![0xAB; payload_size];
        let err = build_py_payload_segments(&payload, chunk_budget, usize::MAX, 1).unwrap_err();
        assert!(err.to_string().contains("exceeds the limit of 65535"));
    }

    #[test]
    fn build_segments_last_fragment_can_be_partial() {
        let payload = vec![0x11; 2100];
        let segments = build_py_payload_segments(&payload, 1000, 4096, 8).expect("segments");
        assert_eq!(segments.len(), 3);

        let (last_header, last_body) = PyPayloadSegHeader::decode_from(&segments[2]).expect("last");
        assert!(last_header.is_last_fragment());
        assert_eq!(last_body.len(), 100);
        assert_eq!(last_header.fragment_payload_len, 100);
    }

    #[test]
    fn build_segments_reconstructed_payload_matches_original() {
        let payload = (0..3500).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
        let segments = build_py_payload_segments(&payload, 1000, 5000, 50).expect("segments");

        let mut reconstructed = Vec::new();
        for fragment in segments {
            let (_, body) = PyPayloadSegHeader::decode_from(&fragment).expect("fragment");
            reconstructed.extend_from_slice(body);
        }

        assert_eq!(reconstructed, payload);
    }
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
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
