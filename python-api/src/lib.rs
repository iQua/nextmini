mod buffer;

use std::net::Ipv4Addr;
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

#[pyclass]
struct Dataplane {
    cfg: LocalConfig,
    py_if: PythonInterfaceHandle,
    processor: ProcessorHandle,
    controller: ControllerInterfaceHandle,
    _join: tokio::task::JoinHandle<()>,
}

#[pymethods]
impl Dataplane {
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
