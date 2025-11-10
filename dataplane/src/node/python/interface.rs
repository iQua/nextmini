use std::fmt;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ahash::AHashMap;
use byteorder::{BigEndian, ByteOrder};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc};
use tokio::time::interval;
use tracing::{error, warn};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::packet::{Packet, PyPayloadSegHeader, PyPayloadSegHeaderError};
use crate::node::python::fragment::{
    EvictedMessage, FragmentAssembler, FragmentAssemblerConfig, FragmentDropKind, FragmentResult,
    InsertReport, ReassembledMessage,
};
use crate::node::{FlowId, FlowIdExt, NodeId};
use nextmini_messages::{
    DataplaneToController, GroupDirectoryEntry, GroupId, GroupRoutingTableEntry,
    PythonFragmentEvent, PythonFragmentEventKind,
    PythonFragmentMetricsSnapshot as ControllerFragmentMetricsSnapshot,
};

#[derive(Clone)]
pub struct PythonInterfaceHandle {
    inner: Arc<Inner>,
}

impl fmt::Debug for PythonInterfaceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PythonInterfaceHandle")
            .field("capacity", &self.inner.capacity)
            .finish()
    }
}

struct Inner {
    capacity: usize,
    senders: Mutex<AHashMap<FlowId, ReceiverEntry>>,
    event_tx: mpsc::Sender<PythonEvent>,
    event_rx: Mutex<mpsc::Receiver<PythonEvent>>,
    fragmentation: FragmentationRuntime,
    telemetry: Option<FragmentTelemetry>,
    metrics: FragmentMetrics,
}

#[derive(Clone, Debug)]
struct ReceiverEntry {
    mode: DeliveryMode,
    sender: mpsc::Sender<PythonDelivery>,
}

#[derive(Clone)]
pub struct FragmentTelemetry {
    controller: ControllerInterfaceHandle,
    node_id: NodeId,
}

impl FragmentTelemetry {
    pub fn new(controller: ControllerInterfaceHandle, node_id: NodeId) -> Self {
        Self {
            controller,
            node_id,
        }
    }

    async fn publish(
        &self,
        flow_id: FlowId,
        message_id: Option<u64>,
        kind: PythonFragmentEventKind,
        detail: String,
        missing_fragments: Option<usize>,
    ) {
        let event = PythonFragmentEvent {
            flow_id: flow_id.to_be_bytes(),
            message_id,
            kind,
            detail,
            missing_fragments,
        };

        self.controller
            .send(DataplaneToController::PythonFragmentEvents {
                node_id: self.node_id,
                events: vec![event],
            })
            .await;
    }

    async fn publish_metrics(&self, snapshot: ControllerFragmentMetricsSnapshot) {
        self.controller
            .send(DataplaneToController::PythonFragmentMetrics {
                node_id: self.node_id,
                snapshot,
            })
            .await;
    }
}

#[derive(Default)]
struct FragmentMetrics {
    fragments_received: AtomicU64,
    invalid_header_drops: AtomicU64,
    reassembly_timeouts: AtomicU64,
    window_overflow_drops: AtomicU64,
}

impl FragmentMetrics {
    fn record_fragment_received(&self) {
        self.fragments_received.fetch_add(1, Ordering::Relaxed);
    }

    fn record_invalid_header_drop(&self) {
        self.invalid_header_drops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_reassembly_timeouts(&self, count: u64) {
        if count > 0 {
            self.reassembly_timeouts.fetch_add(count, Ordering::Relaxed);
        }
    }

    fn record_window_overflow_drop(&self) {
        self.window_overflow_drops.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> FragmentMetricsSnapshot {
        FragmentMetricsSnapshot {
            fragments_received: self.fragments_received.load(Ordering::Relaxed),
            invalid_header_drops: self.invalid_header_drops.load(Ordering::Relaxed),
            reassembly_timeouts: self.reassembly_timeouts.load(Ordering::Relaxed),
            window_overflow_drops: self.window_overflow_drops.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentMetricsSnapshot {
    pub fragments_received: u64,
    pub invalid_header_drops: u64,
    pub reassembly_timeouts: u64,
    pub window_overflow_drops: u64,
}

impl From<FragmentMetricsSnapshot> for ControllerFragmentMetricsSnapshot {
    fn from(value: FragmentMetricsSnapshot) -> Self {
        ControllerFragmentMetricsSnapshot {
            fragments_received: value.fragments_received,
            invalid_header_drops: value.invalid_header_drops,
            reassembly_timeouts: value.reassembly_timeouts,
            window_overflow_drops: value.window_overflow_drops,
        }
    }
}

#[derive(Clone, Debug)]
pub enum PythonDelivery {
    Raw(Packet),
    Payload(PayloadDelivery),
}

#[derive(Clone, Debug)]
pub struct PayloadDelivery {
    pub flow_id: FlowId,
    pub bytes: Vec<u8>,
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
    pub message_id: Option<u64>,
    pub total_len: Option<u32>,
    pub fragment_count: Option<u16>,
    pub payload_format: PayloadFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadFormat {
    Payload,
    RawPacket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryMode {
    RawPacket,
    PayloadOnly,
}

#[derive(Clone, Debug)]
pub struct PythonFragmentationPolicy {
    pub enabled: bool,
    pub max_message_bytes: usize,
    pub reassembly_window_bytes: usize,
    pub fragment_timeout: Duration,
    pub trace_events: bool,
}

impl Default for PythonFragmentationPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_message_bytes: 0,
            reassembly_window_bytes: 0,
            fragment_timeout: Duration::from_secs(0),
            trace_events: false,
        }
    }
}

impl From<&LocalConfig> for PythonFragmentationPolicy {
    fn from(cfg: &LocalConfig) -> Self {
        Self {
            enabled: cfg.python_fragmentation_enabled,
            max_message_bytes: cfg.python_fragmentation_max_message_bytes as usize,
            reassembly_window_bytes: cfg.python_fragmentation_reassembly_window_bytes as usize,
            fragment_timeout: Duration::from_millis(
                cfg.python_fragmentation_fragment_timeout_ms as u64,
            ),
            trace_events: cfg.python_fragmentation_trace_flow_events,
        }
    }
}

#[derive(Debug)]
struct FragmentationRuntime {
    policy: PythonFragmentationPolicy,
    assembler: Mutex<FragmentAssembler>,
}

impl FragmentationRuntime {
    fn new(policy: PythonFragmentationPolicy) -> Self {
        let assembler_cfg = FragmentAssemblerConfig {
            enabled: policy.enabled,
            max_message_bytes: policy.max_message_bytes,
            reassembly_window_bytes: policy.reassembly_window_bytes,
            fragment_timeout: policy.fragment_timeout,
        };
        Self {
            policy,
            assembler: Mutex::new(FragmentAssembler::new(assembler_cfg)),
        }
    }

    fn policy(&self) -> &PythonFragmentationPolicy {
        &self.policy
    }

    async fn insert(&self, fragment: PythonFragment) -> InsertReport {
        let mut assembler = self.assembler.lock().await;
        let now = Instant::now();
        assembler.insert_fragment(
            fragment.flow_id,
            fragment.header,
            fragment.chunk,
            fragment.header_prefix,
            now,
        )
    }
}

impl PythonInterfaceHandle {
    #[allow(dead_code)]
    pub fn new(
        capacity: usize,
        policy: PythonFragmentationPolicy,
        telemetry: Option<FragmentTelemetry>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(capacity);
        Self {
            inner: Arc::new(Inner {
                capacity,
                senders: Mutex::new(AHashMap::new()),
                event_tx,
                event_rx: Mutex::new(event_rx),
                fragmentation: FragmentationRuntime::new(policy),
                telemetry,
                metrics: FragmentMetrics::default(),
            }),
        }
        .with_metrics_task()
    }

    fn with_metrics_task(self) -> Self {
        Self::spawn_metrics_task(&self.inner);
        self
    }

    fn spawn_metrics_task(inner: &Arc<Inner>) {
        let Some(_) = inner.telemetry else {
            return;
        };
        let weak = Arc::downgrade(inner);
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let Some(telemetry) = &inner.telemetry else {
                    continue;
                };
                let snapshot = inner.metrics.snapshot();
                telemetry.publish_metrics(snapshot.into()).await;
            }
        });
    }

    #[allow(dead_code)]
    pub async fn register_receiver(
        &self,
        flow_id: FlowId,
        mode: DeliveryMode,
    ) -> mpsc::Receiver<PythonDelivery> {
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, ReceiverEntry { mode, sender: tx });
        rx
    }

    #[allow(dead_code)]
    pub async fn unregister_receiver(&self, flow_id: FlowId) {
        let mut map = self.inner.senders.lock().await;
        map.remove(&flow_id);
    }

    #[allow(dead_code)]
    pub async fn has_receiver(&self, flow_id: FlowId) -> bool {
        self.inner.senders.lock().await.contains_key(&flow_id)
    }

    pub async fn deliver(&self, packet: Packet) -> Result<(), Packet> {
        let flow_id = packet.flow_id;
        let entry = {
            let map = self.inner.senders.lock().await;
            map.get(&flow_id).cloned()
        };

        let Some(entry) = entry else {
            return Err(packet);
        };

        match entry.mode {
            DeliveryMode::RawPacket => self.deliver_raw(entry, packet).await,
            DeliveryMode::PayloadOnly => self.deliver_payload(entry, packet).await,
        }
    }

    async fn deliver_raw(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        if !self.inner.fragmentation.policy().enabled {
            return Self::send_raw(entry, packet);
        }

        match parse_python_fragment(&packet) {
            FragmentProbe::NotSegmented => Self::send_raw(entry, packet),
            FragmentProbe::Single(fragment) => {
                self.inner.metrics.record_fragment_received();
                let rebuilt = rebuild_single_fragment(&packet, &fragment);
                match rebuilt {
                    Some(pkt) => Self::send_raw(entry, pkt),
                    None => Ok(()),
                }
            }
            FragmentProbe::Fragmented(fragment) => {
                self.inner.metrics.record_fragment_received();
                let fragment_flow = fragment.flow_id;
                let fragment_message = fragment.header.message_id;
                let report = self.inner.fragmentation.insert(fragment).await;
                self.log_evictions(&report.evicted).await;
                match report.result {
                    FragmentResult::Pending | FragmentResult::Bypassed => Ok(()),
                    FragmentResult::Complete(msg) => match rebuild_frame_from_message(&msg) {
                        Some(pkt) => Self::send_raw(entry, pkt),
                        None => {
                            warn!(
                                "PythonInterface: missing header prefix while rebuilding raw frame."
                            );
                            Ok(())
                        }
                    },
                    FragmentResult::Dropped(drop) => {
                        if matches!(drop.kind, FragmentDropKind::WindowOverflow) {
                            self.inner.metrics.record_window_overflow_drop();
                        }
                        if self.inner.fragmentation.policy().trace_events {
                            warn!(
                                "PythonInterface: dropped fragmented message {} on flow {}: {}",
                                fragment_message, fragment_flow, drop.detail
                            );
                        }
                        self.emit_fragment_event(
                            fragment_flow,
                            Some(fragment_message),
                            drop.kind.into(),
                            drop.detail.clone(),
                            None,
                        )
                        .await;
                        Ok(())
                    }
                }
            }
            FragmentProbe::ParseError(err) => {
                let detail = format!(
                    "PythonInterface: failed to decode py-payload header: {}",
                    err
                );
                self.inner.metrics.record_invalid_header_drop();
                warn!("{}", detail);
                self.emit_fragment_event(
                    packet.flow_id,
                    None,
                    PythonFragmentEventKind::InvalidHeader,
                    detail,
                    None,
                )
                .await;
                Ok(())
            }
        }
    }

    async fn deliver_payload(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        if !self.inner.fragmentation.policy().enabled {
            return self.deliver_payload_compat(entry, packet);
        }

        match parse_python_fragment(&packet) {
            FragmentProbe::NotSegmented => {
                let delivery = raw_payload_delivery(&packet);
                if Self::send_payload(entry, delivery).is_err() {
                    return Err(packet);
                }
                Ok(())
            }
            FragmentProbe::Single(fragment) => {
                self.inner.metrics.record_fragment_received();
                let delivery = PayloadDelivery {
                    flow_id: fragment.flow_id,
                    bytes: fragment.payload.to_vec(),
                    src_ip: fragment.flow_id.src_ip(),
                    dst_ip: fragment.flow_id.dst_ip(),
                    src_port: fragment.flow_id.src_port(),
                    dst_port: fragment.flow_id.dst_port(),
                    message_id: Some(fragment.header.message_id),
                    total_len: Some(fragment.header.total_len),
                    fragment_count: Some(fragment.header.fragment_count),
                    payload_format: PayloadFormat::Payload,
                };
                if Self::send_payload(entry, delivery).is_err() {
                    return Err(packet);
                }
                Ok(())
            }
            FragmentProbe::Fragmented(fragment) => {
                self.inner.metrics.record_fragment_received();
                let fragment_flow = fragment.flow_id;
                let fragment_msg = fragment.header.message_id;
                let report = self.inner.fragmentation.insert(fragment).await;
                self.log_evictions(&report.evicted).await;
                match report.result {
                    FragmentResult::Pending | FragmentResult::Bypassed => Ok(()),
                    FragmentResult::Complete(msg) => {
                        let delivery = payload_from_message(msg);
                        if Self::send_payload(entry, delivery).is_err() {
                            return Err(packet);
                        }
                        Ok(())
                    }
                    FragmentResult::Dropped(drop) => {
                        if matches!(drop.kind, FragmentDropKind::WindowOverflow) {
                            self.inner.metrics.record_window_overflow_drop();
                        }
                        if self.inner.fragmentation.policy().trace_events {
                            warn!(
                                "PythonInterface: dropped fragmented payload message {} on flow {}: {}",
                                fragment_msg, fragment_flow, drop.detail
                            );
                        }
                        self.emit_fragment_event(
                            fragment_flow,
                            Some(fragment_msg),
                            drop.kind.into(),
                            drop.detail.clone(),
                            None,
                        )
                        .await;
                        Ok(())
                    }
                }
            }
            FragmentProbe::ParseError(err) => {
                let detail = format!(
                    "PythonInterface: failed to decode py-payload header: {}",
                    err
                );
                self.inner.metrics.record_invalid_header_drop();
                warn!("{}", detail);
                self.emit_fragment_event(
                    packet.flow_id,
                    None,
                    PythonFragmentEventKind::InvalidHeader,
                    detail,
                    None,
                )
                .await;
                Ok(())
            }
        }
    }

    fn deliver_payload_compat(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        let delivery = raw_payload_delivery(&packet);
        if Self::send_payload(entry, delivery).is_err() {
            return Err(packet);
        }
        Ok(())
    }

    async fn emit_fragment_event(
        &self,
        flow_id: FlowId,
        message_id: Option<u64>,
        kind: PythonFragmentEventKind,
        detail: impl Into<String>,
        missing_fragments: Option<usize>,
    ) {
        if !self.inner.fragmentation.policy().trace_events {
            return;
        }
        if let Some(telemetry) = &self.inner.telemetry {
            telemetry
                .publish(flow_id, message_id, kind, detail.into(), missing_fragments)
                .await;
        }
    }

    async fn log_evictions(&self, evicted: &[EvictedMessage]) {
        if evicted.is_empty() {
            return;
        }
        self.inner
            .metrics
            .record_reassembly_timeouts(evicted.len() as u64);
        if !self.inner.fragmentation.policy().trace_events {
            return;
        }
        for entry in evicted {
            let detail = format!(
                "PythonInterface: evicted message {} on flow {} with {} missing fragments.",
                entry.message_id, entry.flow_id, entry.missing_fragments
            );
            warn!("{}", detail);
            self.emit_fragment_event(
                entry.flow_id,
                Some(entry.message_id),
                PythonFragmentEventKind::Timeout,
                detail,
                Some(entry.missing_fragments),
            )
            .await;
        }
    }

    fn send_payload(entry: ReceiverEntry, payload: PayloadDelivery) -> Result<(), ()> {
        match entry
            .sender
            .try_send(PythonDelivery::Payload(payload.clone()))
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                warn!(
                    "PythonInterface: queue unavailable for flow {}; dropping payload delivery.",
                    payload.flow_id
                );
                Err(())
            }
        }
    }

    fn send_raw(entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        match entry.sender.try_send(PythonDelivery::Raw(packet)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(PythonDelivery::Raw(pkt)))
            | Err(TrySendError::Closed(PythonDelivery::Raw(pkt))) => {
                warn!(
                    "PythonInterface: queue unavailable for flow {}; dropping packet.",
                    pkt.flow_id
                );
                Err(pkt)
            }
            Err(_) => unreachable!("unexpected delivery variant"),
        }
    }

    pub async fn publish_event(&self, event: PythonEvent) {
        if let Err(err) = self.inner.event_tx.send(event).await {
            error!(
                "PythonInterface: failed to publish event to Python: {}",
                err
            );
        }
    }

    #[allow(dead_code)]
    pub async fn next_event(&self) -> Option<PythonEvent> {
        let mut rx = self.inner.event_rx.lock().await;
        rx.recv().await
    }

    #[allow(dead_code)]
    pub fn metrics_snapshot(&self) -> FragmentMetricsSnapshot {
        self.inner.metrics.snapshot()
    }
}

enum FragmentProbe<'a> {
    NotSegmented,
    Single(ParsedFragment<'a>),
    Fragmented(PythonFragment),
    ParseError(PyPayloadSegHeaderError),
}

struct ParsedFragment<'a> {
    header: PyPayloadSegHeader,
    payload: &'a [u8],
    payload_offset: usize,
    bytes: &'a [u8],
    flow_id: FlowId,
}

struct PythonFragment {
    flow_id: FlowId,
    header: PyPayloadSegHeader,
    chunk: Vec<u8>,
    header_prefix: Option<Vec<u8>>,
}

fn parse_python_fragment(packet: &Packet) -> FragmentProbe<'_> {
    let bytes = packet.bytes();
    let Some(payload_offset) = tcp_payload_offset(bytes) else {
        return FragmentProbe::NotSegmented;
    };

    if payload_offset >= bytes.len() {
        return FragmentProbe::NotSegmented;
    }

    let payload = &bytes[payload_offset..];
    match PyPayloadSegHeader::decode_from(payload) {
        Ok((header, remainder)) => {
            let chunk_len = header.fragment_payload_len as usize;
            if remainder.len() < chunk_len {
                return FragmentProbe::ParseError(PyPayloadSegHeaderError::BufferTooSmall);
            }
            let chunk = remainder[..chunk_len].to_vec();
            if header.fragment_count <= 1 && !header.fragmented {
                FragmentProbe::Single(ParsedFragment {
                    header,
                    payload: &remainder[..chunk_len],
                    payload_offset,
                    bytes,
                    flow_id: packet.flow_id,
                })
            } else {
                let header_prefix = if header.fragment_index == 0 {
                    Some(bytes[..payload_offset].to_vec())
                } else {
                    None
                };

                FragmentProbe::Fragmented(PythonFragment {
                    flow_id: packet.flow_id,
                    header,
                    chunk,
                    header_prefix,
                })
            }
        }
        Err(err) => FragmentProbe::ParseError(err),
    }
}

fn rebuild_single_fragment(_packet: &Packet, fragment: &ParsedFragment<'_>) -> Option<Packet> {
    let mut frame = Vec::with_capacity(fragment.payload_offset + fragment.payload.len());
    frame.extend_from_slice(&fragment.bytes[..fragment.payload_offset]);
    frame.extend_from_slice(fragment.payload);
    let frame_len = frame.len();
    if frame_len > u16::MAX as usize {
        warn!(
            "PythonInterface: reassembled packet for flow {} exceeds IPv4 length limit.",
            fragment.flow_id
        );
        return None;
    }
    BigEndian::write_u16(&mut frame[2..4], frame_len as u16);
    Some(Packet::from_vec(frame))
}

fn rebuild_frame_from_message(msg: &ReassembledMessage) -> Option<Packet> {
    let mut prefix = msg.header_prefix.clone()?;
    prefix.extend_from_slice(&msg.payload);
    let frame_len = prefix.len();
    if frame_len > u16::MAX as usize {
        return None;
    }
    BigEndian::write_u16(&mut prefix[2..4], frame_len as u16);
    Some(Packet::from_vec(prefix))
}

fn payload_from_message(msg: ReassembledMessage) -> PayloadDelivery {
    PayloadDelivery {
        flow_id: msg.flow_id,
        bytes: msg.payload,
        src_ip: msg.flow_id.src_ip(),
        dst_ip: msg.flow_id.dst_ip(),
        src_port: msg.flow_id.src_port(),
        dst_port: msg.flow_id.dst_port(),
        message_id: Some(msg.message_id),
        total_len: Some(msg.total_len as u32),
        fragment_count: None,
        payload_format: PayloadFormat::Payload,
    }
}

fn tcp_payload_offset(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 20 || (bytes[0] >> 4) != 4 {
        return None;
    }
    let ihl = (bytes[0] & 0x0F) as usize;
    let ip_header_len = ihl * 4;
    if ip_header_len < 20 || bytes.len() < ip_header_len + 20 {
        return None;
    }
    let tcp_data_offset = (bytes[ip_header_len + 12] >> 4) as usize;
    let tcp_header_len = tcp_data_offset * 4;
    if tcp_header_len < 20 || bytes.len() < ip_header_len + tcp_header_len {
        return None;
    }
    Some(ip_header_len + tcp_header_len)
}

fn tcp_payload_from_frame(bytes: &[u8]) -> Option<&[u8]> {
    tcp_payload_offset(bytes).map(|offset| &bytes[offset..])
}

fn raw_payload_delivery(packet: &Packet) -> PayloadDelivery {
    PayloadDelivery {
        flow_id: packet.flow_id,
        bytes: tcp_payload_from_frame(packet.bytes())
            .unwrap_or(packet.bytes())
            .to_vec(),
        src_ip: packet.flow_id.src_ip(),
        dst_ip: packet.flow_id.dst_ip(),
        src_port: packet.flow_id.src_port(),
        dst_port: packet.flow_id.dst_port(),
        message_id: None,
        total_len: None,
        fragment_count: None,
        payload_format: PayloadFormat::RawPacket,
    }
}

impl From<FragmentDropKind> for PythonFragmentEventKind {
    fn from(kind: FragmentDropKind) -> Self {
        match kind {
            FragmentDropKind::WindowOverflow => PythonFragmentEventKind::WindowOverflow,
            _ => PythonFragmentEventKind::AssemblerDrop,
        }
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum PythonEvent {
    GroupCreated {
        group_id: GroupId,
        src_node_id: NodeId,
        group_ip: Ipv4Addr,
    },
    GroupDirectoryUpdated {
        entries: Vec<GroupDirectoryEntry>,
    },
    GroupRoutesInstalled {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    LocalMemberJoined {
        group_id: GroupId,
        node_id: NodeId,
    },
    LocalMemberLeft {
        group_id: GroupId,
        node_id: NodeId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::packet::PyPayloadSegHeader;
    use std::net::Ipv4Addr;

    fn disabled_policy() -> PythonFragmentationPolicy {
        PythonFragmentationPolicy::default()
    }

    fn enabled_policy() -> PythonFragmentationPolicy {
        PythonFragmentationPolicy {
            enabled: true,
            max_message_bytes: 64 * 1024,
            reassembly_window_bytes: 64 * 1024,
            fragment_timeout: Duration::from_millis(1000),
            trace_events: true,
        }
    }

    #[tokio::test]
    async fn deliver_success_for_registered_flow() {
        let handle = PythonInterfaceHandle::new(4, disabled_policy(), None);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle
            .register_receiver(flow_id, DeliveryMode::RawPacket)
            .await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        match receiver.recv().await {
            Some(PythonDelivery::Raw(received)) => {
                assert_eq!(received.bytes(), packet.bytes())
            }
            other => panic!("unexpected delivery: {:?}", other),
        }
    }

    #[tokio::test]
    async fn deliver_fails_when_queue_full() {
        let handle = PythonInterfaceHandle::new(1, disabled_policy(), None);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle
            .register_receiver(flow_id, DeliveryMode::RawPacket)
            .await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        assert!(handle.deliver(packet.clone()).await.is_err());
        receiver.recv().await;
    }

    #[tokio::test]
    async fn deliver_returns_error_without_receiver() {
        let handle = PythonInterfaceHandle::new(4, disabled_policy(), None);
        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet).await.is_err());
    }

    fn single_fragment_header(message_id: u64, payload_len: usize) -> PyPayloadSegHeader {
        PyPayloadSegHeader {
            fragmented: false,
            last_fragment: true,
            message_id,
            total_len: payload_len as u32,
            fragment_index: 0,
            fragment_count: 1,
            fragment_payload_len: payload_len as u32,
        }
    }

    fn build_python_packet(header: &PyPayloadSegHeader, payload: &[u8]) -> Packet {
        let mut body = vec![0u8; PyPayloadSegHeader::LEN + payload.len()];
        header
            .encode_into(&mut body[..PyPayloadSegHeader::LEN])
            .expect("encode header");
        body[PyPayloadSegHeader::LEN..].copy_from_slice(payload);
        Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &body,
        )
    }

    #[tokio::test]
    async fn metrics_increment_for_fragment_delivery() {
        let handle = PythonInterfaceHandle::new(4, enabled_policy(), None);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle
            .register_receiver(flow_id, DeliveryMode::PayloadOnly)
            .await;

        let payload = vec![0xAA; 32];
        let header = single_fragment_header(42, payload.len());
        let packet = build_python_packet(&header, &payload);

        assert!(handle.deliver(packet).await.is_ok());
        receiver.recv().await;

        let metrics = handle.metrics_snapshot();
        assert_eq!(metrics.fragments_received, 1);
    }

    #[tokio::test]
    async fn metrics_increment_for_invalid_header_and_timeouts() {
        let handle = PythonInterfaceHandle::new(4, enabled_policy(), None);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle
            .register_receiver(flow_id, DeliveryMode::PayloadOnly)
            .await;

        // Build a packet with an invalid magic so it exercises the parse-error path.
        let payload = vec![0xBB; 16];
        let mut header_bytes = vec![0u8; PyPayloadSegHeader::LEN + payload.len()];
        let header = single_fragment_header(7, payload.len());
        header
            .encode_into(&mut header_bytes[..PyPayloadSegHeader::LEN])
            .unwrap();
        header_bytes[0] = 0x00; // corrupt magic
        header_bytes[PyPayloadSegHeader::LEN..].copy_from_slice(&payload);
        let invalid_packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &header_bytes,
        );

        handle.deliver(invalid_packet).await.unwrap();
        // No delivery expected; drain receiver if something arrived.
        receiver.try_recv().ok();

        // Manually log eviction to simulate timeout telemetry.
        let evicted = vec![EvictedMessage {
            flow_id,
            message_id: 99,
            missing_fragments: 2,
        }];
        handle.log_evictions(&evicted).await;

        let metrics = handle.metrics_snapshot();
        assert_eq!(metrics.invalid_header_drops, 1);
        assert_eq!(metrics.reassembly_timeouts, 1);
    }
}
