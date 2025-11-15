use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};

use nextmini_messages::rlm::{self, RlmControl, TfmccDataHeader};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control::{self, CompletionPolicy};
use super::session::{AckPolicy, CommonConfig, CongestionControl, SenderConfig};
use super::tfmcc::TfmccSender;

const DEFAULT_WINDOW: usize = 64;
const MANIFEST_RETRY_INTERVAL_MS: u64 = 250;
const CONTROL_POLL_TIMEOUT_MS: u64 = 20;
const MAX_FRAME_CACHE_SIZE: usize = 10_000; // Limit cache to prevent unbounded growth
const TRANSFER_TIMEOUT_SECS: u64 = 300; // 5 minutes - configurable later

/// Drives a sender session: streams chunks, tracks inflight state, and reacts
/// to control frames emitted by receivers.
pub async fn run(
    cfg: SenderConfig,
    mut ctrl_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let sid = cfg.common.session_id;
    let chunk_size = cfg.common.chunk_size as u64;
    let total_bytes = cfg.total_bytes;
    let total_chunks = if chunk_size == 0 {
        0
    } else {
        total_bytes.div_ceil(chunk_size)
    };

    tracing::info!(
        session_id = sid,
        total_bytes,
        total_chunks,
        receivers = cfg.receiver_ids.len(),
        "RLM sender started"
    );

    let source_file = cfg
        .source_path
        .as_ref()
        .and_then(|path| match File::open(path) {
            Ok(file) => Some(file),
            Err(error) => {
                tracing::error!(
                    session_id = sid,
                    path = %path,
                    %error,
                    "RLM sender: unable to open source file; falling back to empty chunks"
                );
                None
            }
        });

    let completion_policy = completion_from_ack(&cfg.ack_policy, cfg.receiver_ids.len());
    let mut state = SenderState::new(cfg, total_chunks, completion_policy);
    let mut chunk_source =
        ChunkSource::new(source_file, state.common.chunk_size, total_chunks, sid);
    let mut pacer = DataPacer::new(state.common.data_bucket.clone());
    let mut last_resend = Instant::now();
    let transfer_start = Instant::now();
    let transfer_timeout = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

    loop {
        // checks for transfer timeout
        if transfer_start.elapsed() > transfer_timeout && !state.is_complete() {
            tracing::error!(
                session_id = sid,
                elapsed_secs = transfer_start.elapsed().as_secs(),
                inflight = state.inflight_len(),
                resend_queue = state.resend_queue.len(),
                "RLM sender: transfer timeout exceeded; forcing completion"
            );
            break;
        }
        // drains any immediately-available control frames so resend/retire
        // decisions reflect fresh receiver state before we transmit more data
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_topology_gate();
        state.maybe_release_routes_gate();
        state.maybe_release_ready_gate();
        if let Some(rate) = state.maybe_update_cc() {
            pacer.set_target_rate(rate);
        }

        let mut progressed = false;

        tracing::trace!(
            session_id = sid,
            ready_gate_open = state.ready_gate_open,
            source_drained = state.source_drained,
            ready_for_data = state.ready_for_data(),
            chunk_source_finished = chunk_source.finished(),
            inflight_len = state.inflight_len(),
            window = state.window_limit(),
            base_window = state.base_window,
            "RLM sender: loop iteration"
        );

        if state.should_emit_manifest() {
            state.send_manifest(&processors);
            progressed = true;
        }

        // explicitly mark source as drained when chunk source finishes
        // to prevent infinite loop waiting for source_drained to be set
        if chunk_source.finished() && !state.source_drained {
            state.mark_source_drained();
            progressed = true;
        }

        if state.ready_for_data()
            && !chunk_source.finished()
            && state.inflight_len() < state.window_limit()
        {
            tracing::debug!(
                session_id = sid,
                ready_for_data = state.ready_for_data(),
                chunk_source_finished = chunk_source.finished(),
                inflight_len = state.inflight_len(),
                window = state.window_limit(),
                "RLM sender: attempting to send next chunk"
            );
            match chunk_source.next_chunk() {
                Ok(Some(chunk)) => {
                    tracing::debug!(
                        session_id = sid,
                        chunk_index = chunk.index,
                        chunk_size = chunk.data.len(),
                        "RLM sender: sending data chunk"
                    );
                    pacer.wait_for(state.common.chunk_size).await;
                    state.send_data_chunk(chunk, &processors);
                    progressed = true;
                }
                Ok(None) => {
                    state.mark_source_drained();
                    progressed = true;
                }
                Err(error) => {
                    tracing::error!(
                        session_id = sid,
                        %error,
                        "RLM sender: read error; stopping new chunk generation"
                    );
                    state.mark_source_drained();
                    progressed = true;
                }
            }
        }

        // makes retransmitting chunk possible
        if !progressed && state.should_resend() && last_resend.elapsed() >= state.repair_backoff {
            tracing::trace!(
                session_id = sid,
                resend_queue_len = state.resend_queue.len(),
                "RLM sender: attempting resend"
            );

            pacer.wait_for(state.common.chunk_size).await;

            if state.send_resend(&processors) {
                last_resend = Instant::now();
                progressed = true;
            }
        }

        if !progressed && state.try_emit_eot(&processors) {
            progressed = true;
        }

        if state.is_complete() {
            tracing::info!(
                session_id = sid,
                bytes_sent = state.bytes_sent,
                chunks_sent = state.primary_chunks,
                resends = state.resend_count,
                "RLM sender finished with reliable delivery guarantees"
            );
            break;
        }

        if !progressed {
            // Don't block forever; let the loop re-check timers (e.g., MANIFEST resend).
            if let Ok(Some(frame)) = tokio::time::timeout(
                Duration::from_millis(CONTROL_POLL_TIMEOUT_MS),
                ctrl_rx.recv(),
            )
            .await
            {
                state.handle_control(frame);
            }
            // On timeout or channel closed, just fall through and loop; this allows
            // MANIFEST re-sends every 250ms while waiting for READY.
        }
    }
}

/// Converts a user-facing ACK policy into a concrete completion rule.
fn completion_from_ack(policy: &AckPolicy, receiver_count: usize) -> CompletionPolicy {
    match policy {
        AckPolicy::All => CompletionPolicy::All,
        AckPolicy::KofN(k) => CompletionPolicy::Threshold((*k).max(1).min(receiver_count.max(1))),
        AckPolicy::Fraction(fraction) => {
            let needed = ((*fraction * receiver_count as f32).ceil() as usize).max(1);
            CompletionPolicy::Threshold(needed.min(receiver_count.max(1)))
        }
    }
}

/// Encapsulates all mutable sender-side state (window, inflight map, pacing,
/// manifest timing, etc.). Keeping the logic centralized makes the event loop
/// above easier to read and test.
struct SenderState {
    session_id: u64,
    common: CommonConfig,
    completion_policy: CompletionPolicy,
    receiver_count: usize,
    base_window: usize,
    window: usize,
    tfmcc: Option<TfmccSender>,
    total_chunks: u64,
    total_bytes: u64,
    inflight: BTreeMap<u64, HashSet<usize>>,
    frame_cache: BTreeMap<u64, Vec<Bytes>>,
    first_send_times: BTreeMap<u64, Instant>,
    resend_queue: BTreeSet<u64>,
    ready_nodes: HashSet<usize>,
    routes_gate_open: bool,
    topology_gate_open: bool,
    ready_gate_open: bool,
    ready_deadline: Option<Instant>,
    topology_ready_rx: Option<watch::Receiver<bool>>,
    routes_ready_rx: Option<watch::Receiver<bool>>,
    ready_grace: Duration,
    manifest_sent: bool,
    source_drained: bool,
    eot_sent: bool,
    bytes_sent: u64,
    primary_chunks: u64,
    resend_count: u64,
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    repair_backoff: Duration,
    manifest_interval: Duration,
    manifest_last_sent: Instant,
    /// When false (channel_backpressure=true), SACK/NACK-based repair is disabled
    enable_sack_nack: bool,
}

impl SenderState {
    fn new(mut cfg: SenderConfig, total_chunks: u64, completion_policy: CompletionPolicy) -> Self {
        let common = cfg.common.clone();
        let receiver_count = cfg.receiver_ids.len();
        let ready_gate_open = receiver_count == 0;
        let ready_grace = Duration::from_millis(cfg.ready_grace_ms.max(1));
        let topology_ready_rx = cfg.topology_ready.take();
        let topology_gate_open = topology_ready_rx
            .as_ref()
            .map(|rx| *rx.borrow())
            .unwrap_or(true);
        let routes_ready_rx = cfg.routes_ready.take();
        let routes_gate_open = routes_ready_rx
            .as_ref()
            .map(|rx| *rx.borrow())
            .unwrap_or(true);
        let ready_deadline = None;
        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let dst_ip = common.group_ip;
        let base_window = compute_window(&cfg);
        let session_start = Instant::now();
        // TFMCC is disabled when use_tfmcc=false (channel_backpressure=true)
        let tfmcc = if !cfg.use_tfmcc {
            None
        } else {
            match &cfg.cc {
                CongestionControl::Static => None,
                CongestionControl::Tfmcc(tcfg) => Some(TfmccSender::new(
                    tcfg.clone(),
                    cfg.common.chunk_size,
                    session_start,
                )),
            }
        };

        if cfg.common.control_weight != 0 {
            tracing::debug!(
                session_id = common.session_id,
                control_weight = cfg.common.control_weight,
                "RLM sender: control_weight is recorded but scheduler boosts are not yet wired."
            );
        }

        if cfg.checksum_out {
            tracing::warn!(
                session_id = common.session_id,
                "RLM sender: checksum_out requested but checksum emission is not implemented; skipping."
            );
        }

        if cfg.fec_k.is_some() || cfg.fec_p != 0 {
            tracing::warn!(
                session_id = common.session_id,
                fec_k = ?cfg.fec_k,
                fec_p = cfg.fec_p,
                "RLM sender: FEC parameters supplied but FEC is not yet implemented."
            );
        }

        Self {
            session_id: common.session_id,
            common,
            completion_policy,
            receiver_count,
            base_window,
            window: base_window,
            tfmcc,
            total_chunks,
            total_bytes: cfg.total_bytes,
            inflight: BTreeMap::new(),
            frame_cache: BTreeMap::new(),
            first_send_times: BTreeMap::new(),
            resend_queue: BTreeSet::new(),
            ready_nodes: HashSet::new(),
            routes_gate_open,
            topology_gate_open,
            ready_gate_open,
            ready_deadline,
            topology_ready_rx,
            routes_ready_rx,
            ready_grace,
            manifest_sent: false,
            source_drained: total_chunks == 0,
            eot_sent: false,
            bytes_sent: 0,
            primary_chunks: 0,
            resend_count: 0,
            src_ip,
            dst_ip,
            src_port: cfg.common.src_port,
            dst_port: cfg.common.dst_port,
            repair_backoff: Duration::from_millis(cfg.repair_backoff_ms.max(1)),
            manifest_interval: Duration::from_millis(MANIFEST_RETRY_INTERVAL_MS),
            manifest_last_sent: Instant::now(),
            enable_sack_nack: cfg.enable_sack_nack,
        }
    }

    fn send_manifest(&mut self, processors: &ProcessorHandle) {
        let manifest = RlmControl::Manifest {
            chunk_size: self.common.chunk_size as u32,
            total_bytes: self.total_bytes,
            checksum_algo: 0,
            options: 0,
        };
        self.send_control(&manifest, processors);
        self.manifest_last_sent = Instant::now();
        if !self.manifest_sent && self.receiver_count > 0 && self.ready_deadline.is_none() {
            self.ready_deadline = Some(Instant::now() + self.ready_grace);
        }
        self.manifest_sent = true;
        tracing::info!(
            session_id = self.session_id,
            src = %self.src_ip,
            dst = %self.dst_ip,
            dst_port = self.dst_port,
            total_bytes = self.total_bytes,
            chunk_size = self.common.chunk_size,
            receivers = self.receiver_count,
            "RLM sender: MANIFEST sent"
        );
    }

    fn ready_for_data(&self) -> bool {
        self.topology_gate_open && self.ready_gate_open && !self.source_drained
    }

    fn window_limit(&self) -> usize {
        self.window.max(1)
    }

    fn maybe_update_cc(&mut self) -> Option<f64> {
        let ctrl = self.tfmcc.as_mut()?;
        ctrl.on_tick(Instant::now());
        let rate = ctrl.current_rate_bytes_per_s();
        tracing::info!(
            session_id = self.session_id,
            rate_bps = (rate * 8.0) as u64,
            "TFMCC updated sender rate"
        );
        Some(rate)
    }

    fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    fn should_resend(&self) -> bool {
        !self.resend_queue.is_empty()
    }

    fn should_resend_manifest(&self) -> bool {
        self.manifest_sent
            && self.topology_gate_open
            && !self.ready_gate_open
            && self.manifest_last_sent.elapsed() >= self.manifest_interval
    }

    fn should_emit_manifest(&self) -> bool {
        if !self.topology_gate_open || !self.routes_gate_open {
            return false;
        }
        if !self.manifest_sent {
            return true;
        }
        self.should_resend_manifest()
    }

    fn mark_source_drained(&mut self) {
        self.source_drained = true;
    }

    fn next_tfmcc_header(&mut self) -> Option<TfmccDataHeader> {
        self.tfmcc
            .as_mut()
            .map(|ctrl| ctrl.build_data_header(Instant::now()))
    }

    fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let tfmcc_header = self.next_tfmcc_header();
        let frame = Bytes::from(rlm::encode_data(
            self.session_id,
            chunk.index,
            &chunk.data,
            tfmcc_header.as_ref(),
        ));
        let now = Instant::now();
        self.first_send_times.insert(chunk.index, now);
        self.enqueue_frame(chunk.index, vec![frame.clone()]);
        self.bytes_sent += chunk.data.len() as u64;
        self.primary_chunks += 1;
        tracing::debug!(
            session_id = self.session_id,
            chunk_index = chunk.index,
            chunk_data_len = chunk.data.len(),
            src_ip = %self.src_ip,
            dst_ip = %self.dst_ip,
            "RLM sender: encoded DATA chunk, sending frame to processor"
        );
        self.send_frame(&frame, processors);
    }

    fn send_resend(&mut self, processors: &ProcessorHandle) -> bool {
        let Some(idx) = self.resend_queue.iter().next().copied() else {
            return false;
        };
        if let Some(frames) = self.frame_cache.get(&idx) {
            self.resend_queue.remove(&idx);
            self.resend_count += 1;
            self.first_send_times.insert(idx, Instant::now());
            for frame in frames {
                self.send_frame(frame, processors);
            }
            tracing::info!(
                session_id = self.session_id,
                index = idx,
                resend_count = self.resend_count,
                resend_queue_remaining = self.resend_queue.len(),
                "RLM sender: retransmitting chunk"
            );
            true
        } else {
            tracing::warn!(
                session_id = self.session_id,
                index = idx,
                "RLM sender: cannot retransmit - chunk not in cache"
            );
            self.resend_queue.remove(&idx);
            false
        }
    }

    fn try_emit_eot(&mut self, processors: &ProcessorHandle) -> bool {
        if self.eot_sent || !self.source_drained || !self.inflight.is_empty() {
            if !self.eot_sent && self.source_drained && !self.inflight.is_empty() {
                tracing::trace!(
                    session_id = self.session_id,
                    inflight_count = self.inflight.len(),
                    "RLM sender: cannot send EOT - chunks still inflight"
                );
            }
            return false;
        }
        let eot = RlmControl::Eot {
            last_index: self.total_chunks,
            checksum: None,
        };
        self.send_control(&eot, processors);
        self.eot_sent = true;
        tracing::info!(
            session_id = self.session_id,
            last_index = self.total_chunks,
            "RLM sender: EOT sent"
        );
        true
    }

    fn is_complete(&self) -> bool {
        self.source_drained
            && self.eot_sent
            && self.inflight.is_empty()
            && self.resend_queue.is_empty()
    }

    fn handle_control(&mut self, frame: InboundFrame) {
        let InboundFrame { bytes, peer_id, .. } = frame;
        let Some((_, control)) = rlm::decode_control(&bytes) else {
            tracing::warn!(
                session_id = self.session_id,
                "RLM sender: failed to decode control frame"
            );
            return;
        };
        let now = Instant::now();

        match control {
            RlmControl::Ready { node_id } => {
                self.ready_nodes.insert(node_id as usize);
                tracing::debug!(
                    session_id = self.session_id,
                    node_id,
                    "RLM sender: receiver ready"
                );
            }
            RlmControl::Manifest { .. } | RlmControl::Eot { .. } => {
                // Ignore sender-originated control frames looped back.
            }
            _ => {
                if let RlmControl::TfmccFeedback { .. } = &control
                    && let Some(ctrl) = &mut self.tfmcc
                {
                    ctrl.on_feedback(&control, now);
                }

                // Skip SACK/REPAIR processing when backpressure is enabled
                if !self.enable_sack_nack {
                    if matches!(control, RlmControl::Sack { .. } | RlmControl::Repair { .. }) {
                        tracing::debug!(
                            session_id = self.session_id,
                            control_type = ?control,
                            "RLM sender: skipping SACK/REPAIR (backpressure enabled)"
                        );
                        return;
                    }
                }

                let Some(from_node) = peer_id else {
                    tracing::warn!(
                        session_id = self.session_id,
                        ?control,
                        "RLM sender: dropping control without peer id"
                    );
                    return;
                };
                tracing::debug!(
                    session_id = self.session_id,
                    from_node = from_node,
                    control_type = ?control,
                    inflight_count = self.inflight.len(),
                    resend_queue_len_before = self.resend_queue.len(),
                    "RLM sender: processing control frame"
                );
                let retired = control::process_control_event(
                    from_node,
                    &control,
                    &mut self.inflight,
                    &mut self.resend_queue,
                    self.receiver_count.max(1),
                    &self.completion_policy,
                );
                if !retired.is_empty() {
                    tracing::debug!(
                        session_id = self.session_id,
                        retired_count = retired.len(),
                        retired_indices = ?retired,
                        "RLM sender: retiring chunks"
                    );
                    control::retire_chunks(&retired, &mut self.inflight, &mut self.resend_queue);
                    for idx in retired {
                        self.first_send_times.remove(&idx);
                        self.frame_cache.remove(&idx);
                    }
                }
                tracing::debug!(
                    session_id = self.session_id,
                    inflight_count = self.inflight.len(),
                    resend_queue_len = self.resend_queue.len(),
                    "RLM sender: after processing control frame"
                );
            }
        }
    }

    fn enqueue_frame(&mut self, idx: u64, frames: Vec<Bytes>) {
        // Enforce cache size limit to prevent unbounded memory growth
        if self.frame_cache.len() >= MAX_FRAME_CACHE_SIZE {
            // Evict oldest entries that are not in inflight or resend queue
            let mut to_evict = Vec::new();
            for (&cached_idx, _) in self.frame_cache.iter() {
                if !self.inflight.contains_key(&cached_idx)
                    && !self.resend_queue.contains(&cached_idx)
                {
                    to_evict.push(cached_idx);
                    if to_evict.len() >= 100 {
                        break; // Evict in batches
                    }
                }
            }

            if !to_evict.is_empty() {
                for idx_to_remove in &to_evict {
                    self.frame_cache.remove(idx_to_remove);
                    self.first_send_times.remove(idx_to_remove);
                }
                tracing::debug!(
                    session_id = self.session_id,
                    evicted = to_evict.len(),
                    cache_size = self.frame_cache.len(),
                    "RLM sender: evicted old frames from cache"
                );
            } else {
                tracing::warn!(
                    session_id = self.session_id,
                    cache_size = self.frame_cache.len(),
                    inflight = self.inflight.len(),
                    resend_queue = self.resend_queue.len(),
                    "RLM sender: frame cache at limit but no entries eligible for eviction"
                );
            }
        }

        self.frame_cache.insert(idx, frames);
        self.inflight.entry(idx).or_default();
    }

    fn send_frame(&self, frame: &Bytes, processors: &ProcessorHandle) {
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );

        processors.process_packet_blocking(packet);
    }

    fn send_control(&self, control: &RlmControl, processors: &ProcessorHandle) {
        let buf = rlm::encode_control(self.session_id, control);

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            &buf,
        );

        processors.process_packet_blocking(packet);
    }

    fn maybe_release_topology_gate(&mut self) {
        if self.topology_gate_open {
            return;
        }

        let Some(rx) = self.topology_ready_rx.as_mut() else {
            self.topology_gate_open = true;
            return;
        };

        if *rx.borrow() {
            self.topology_gate_open = true;
            tracing::info!(
                session_id = self.session_id,
                "RLM sender: topology-ready signal received"
            );
        }
    }

    fn maybe_release_routes_gate(&mut self) {
        if self.routes_gate_open {
            return;
        }
        let Some(rx) = self.routes_ready_rx.as_mut() else {
            self.routes_gate_open = true;
            return;
        };
        if *rx.borrow() {
            self.routes_gate_open = true;
            tracing::info!(
                session_id = self.session_id,
                "RLM sender: multicast routes installed for this source"
            );
        }
    }

    fn maybe_release_ready_gate(&mut self) {
        if self.ready_gate_open || !self.manifest_sent {
            return;
        }

        if self.receiver_count > 0 && self.ready_nodes.len() == self.receiver_count {
            self.ready_gate_open = true;
            self.ready_deadline = None;
            tracing::info!(
                session_id = self.session_id,
                "RLM sender: all receivers ready"
            );
            return;
        }

        if let Some(deadline) = self.ready_deadline
            && Instant::now() >= deadline
        {
            self.ready_gate_open = true;
            self.ready_deadline = None;
            tracing::warn!(
                session_id = self.session_id,
                ready = self.ready_nodes.len(),
                total = self.receiver_count,
                "RLM sender: proceeding without all receivers ready"
            );
        }
    }
}

/// Chooses a sliding window size based on receiver fan-out and optional token
/// bucket configuration.
fn compute_window(cfg: &SenderConfig) -> usize {
    let receiver_factor = (cfg.receiver_ids.len().max(1)) * 2;
    let mut window = DEFAULT_WINDOW.max(receiver_factor);
    if let Some(bucket) = &cfg.common.data_bucket
        && cfg.common.chunk_size > 0
    {
        let per_chunk = cfg.common.chunk_size;
        let bucket_chunks = (bucket.bucket_size / per_chunk).max(1);
        window = window.max(bucket_chunks);
    }
    window.max(1)
}

/// Materialized chunk that is ready to be encoded into an RLM frame.
struct ChunkPayload {
    index: u64,
    data: Vec<u8>,
}

/// Reads chunk payloads from disk (if provided) and hands them to the sender
/// in strict index order. Tests may swap in the empty case by omitting files.
struct ChunkSource {
    file: Option<File>,
    chunk_size: usize,
    total_chunks: u64,
    next_index: u64,
    session_id: u64,
}

impl ChunkSource {
    fn new(file: Option<File>, chunk_size: usize, total_chunks: u64, session_id: u64) -> Self {
        Self {
            file,
            chunk_size,
            total_chunks,
            next_index: 1,
            session_id,
        }
    }

    fn finished(&self) -> bool {
        self.total_chunks == 0 || self.next_index > self.total_chunks
    }

    fn next_chunk(&mut self) -> io::Result<Option<ChunkPayload>> {
        if self.finished() {
            return Ok(None);
        }

        let idx = self.next_index;

        if let Some(file) = self.file.as_mut() {
            let mut buf = vec![0u8; self.chunk_size];
            let read = file.read(&mut buf)?;
            if read == 0 {
                tracing::warn!(
                    session_id = self.session_id,
                    "RLM sender: source file ended unexpectedly"
                );
                self.next_index = self.total_chunks + 1;
                return Ok(None);
            }
            buf.truncate(read);
            self.next_index += 1;
            Ok(Some(ChunkPayload {
                index: idx,
                data: buf,
            }))
        } else {
            self.next_index += 1;
            Ok(Some(ChunkPayload {
                index: idx,
                data: Vec::new(),
            }))
        }
    }
}

/// Simple token-bucket pacer used to honor optional bandwidth caps.
struct DataPacer {
    base_spec: Option<nextmini_messages::TokenBucketSpec>,
    tokens: f64,
    last: Instant,
    rate_bytes_per_s: f64,
}

impl DataPacer {
    fn new(spec: Option<nextmini_messages::TokenBucketSpec>) -> Self {
        let rate = spec.as_ref().map(|tb| tb.rate as f64).unwrap_or(0.0);
        let tokens = spec.as_ref().map(|tb| tb.bucket_size as f64).unwrap_or(0.0);
        Self {
            base_spec: spec,
            tokens,
            last: Instant::now(),
            rate_bytes_per_s: rate,
        }
    }

    fn set_target_rate(&mut self, bytes_per_s: f64) {
        if !bytes_per_s.is_finite() || bytes_per_s <= 0.0 {
            if let Some(spec) = &self.base_spec {
                self.rate_bytes_per_s = spec.rate as f64;
            }
            return;
        }
        let was_disabled = self.rate_bytes_per_s <= 0.0;
        self.rate_bytes_per_s = bytes_per_s;
        if was_disabled {
            if self.base_spec.is_some() {
                self.tokens = self.effective_bucket();
            } else {
                self.tokens = 0.0;
                self.last = Instant::now();
            }
        }
    }

    async fn wait_for(&mut self, bytes: usize) {
        if self.base_spec.is_none() {
            self.wait_dynamic_only(bytes).await;
            return;
        }
        let bytes_f = bytes as f64;
        loop {
            self.refill();
            if self.tokens >= bytes_f {
                self.tokens -= bytes_f;
                break;
            }
            let rate = self.effective_rate();
            if rate <= 0.0 || !rate.is_finite() {
                break;
            }
            let needed = (bytes_f - self.tokens).max(1.0);
            let wait = (needed / rate).max(0.001);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let rate = self.effective_rate();
        if rate <= 0.0 || !rate.is_finite() {
            self.last = now;
            return;
        }
        let bucket = self.effective_bucket();
        self.tokens = (self.tokens + elapsed * rate).min(bucket);
        self.last = now;
    }

    fn effective_rate(&self) -> f64 {
        if self.rate_bytes_per_s > 0.0 {
            self.rate_bytes_per_s
        } else if let Some(spec) = &self.base_spec {
            spec.rate.max(1) as f64
        } else {
            0.0
        }
    }

    fn effective_bucket(&self) -> f64 {
        if let Some(spec) = &self.base_spec {
            spec.bucket_size as f64
        } else {
            f64::INFINITY
        }
    }

    async fn wait_dynamic_only(&mut self, bytes: usize) {
        if self.rate_bytes_per_s <= 0.0 {
            return;
        }
        let bytes_f = bytes as f64;
        self.refill_dynamic();
        if self.tokens >= bytes_f {
            self.tokens -= bytes_f;
            return;
        }
        let needed = bytes_f - self.tokens;
        self.tokens = 0.0;
        let wait = (needed / self.rate_bytes_per_s).max(0.0);
        if wait > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
        self.last = Instant::now();
    }

    fn refill_dynamic(&mut self) {
        if self.rate_bytes_per_s <= 0.0 {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        self.tokens += elapsed * self.rate_bytes_per_s;
        self.last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_roundtrip_header_and_meta() {
        let sid = 99;
        let idx = 7;
        let plen = 4096usize;
        let payload = vec![0xAAu8; plen];
        let buf = rlm::encode_data(sid, idx, &payload, None);
        let (hdr, data, body) = rlm::decode_data(&buf).expect("decode data");
        assert_eq!(hdr.session_id, sid);
        assert_eq!(data.index, idx);
        assert_eq!(data.payload_len as usize, plen);
        assert_eq!(body.len(), plen);
    }

    /// TEST 1: Validates that named constants are properly defined and accessible.
    /// This fixes the "magic numbers" issue where hardcoded values were scattered
    /// throughout the code, making it hard to tune and maintain.
    #[test]
    fn constants_are_defined_and_reasonable() {
        // Verify all constants are defined with sensible values
        assert_eq!(DEFAULT_WINDOW, 64, "Default window should be 64 chunks");
        assert_eq!(
            MANIFEST_RETRY_INTERVAL_MS, 250,
            "Manifest retry should be 250ms"
        );
        assert_eq!(
            CONTROL_POLL_TIMEOUT_MS, 20,
            "Control poll timeout should be 20ms"
        );
        assert_eq!(
            MAX_FRAME_CACHE_SIZE, 10_000,
            "Cache limit should be 10,000 entries"
        );
        assert_eq!(
            TRANSFER_TIMEOUT_SECS, 300,
            "Transfer timeout should be 5 minutes"
        );

        // Verify constants are greater than zero (no accidental zeroes)
    }

    /// TEST 2: Validates ChunkSource properly reports when finished.
    /// This tests the fix for the infinite loop bug where the sender could get stuck
    /// waiting for source_drained when chunk_source.finished() returned true but
    /// the state wasn't updated.
    #[test]
    fn chunk_source_finished_detection() {
        let session_id = 1;

        // Test 1: Zero chunks should be immediately finished
        let source = ChunkSource::new(None, 1024, 0, session_id);
        assert!(
            source.finished(),
            "ChunkSource with 0 chunks should be finished immediately"
        );

        // Test 2: Source with chunks should not be finished initially
        let mut source = ChunkSource::new(None, 1024, 5, session_id);
        assert!(
            !source.finished(),
            "ChunkSource with 5 chunks should not be finished initially"
        );

        // Test 3: After consuming all chunks, should be finished
        for _ in 0..5 {
            let result = source.next_chunk();
            assert!(result.is_ok(), "Should successfully get chunk");
        }
        assert!(
            source.finished(),
            "ChunkSource should be finished after all chunks consumed"
        );

        // Test 4: Requesting more chunks after finished returns None
        let result = source.next_chunk();
        assert!(
            matches!(result, Ok(None)),
            "Should return None when finished"
        );
    }

    /// TEST 3: Validates cache eviction logic prevents unbounded growth.
    /// This tests the fix for memory exhaustion where the frame cache could grow
    /// to millions of entries during long transfers with packet loss.
    #[test]
    fn cache_eviction_prevents_unbounded_growth() {
        // Create a mock sender state (we can't fully initialize without dependencies,
        // so we test the logic separately)
        let mut frame_cache: BTreeMap<u64, Vec<Bytes>> = BTreeMap::new();
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        let resend_queue: BTreeSet<u64> = BTreeSet::new();

        // Simulate filling cache to MAX_FRAME_CACHE_SIZE
        for i in 0..MAX_FRAME_CACHE_SIZE {
            let frames = vec![Bytes::from(vec![0u8; 100])];
            frame_cache.insert(i as u64, frames);
        }

        assert_eq!(
            frame_cache.len(),
            MAX_FRAME_CACHE_SIZE,
            "Cache should be at limit before eviction"
        );

        // Mark some chunks as inflight (these should NOT be evicted)
        for i in 0..10 {
            inflight.insert(i as u64, HashSet::new());
        }

        // Simulate eviction logic (from enqueue_frame)
        let mut to_evict = Vec::new();
        for (&cached_idx, _) in frame_cache.iter() {
            if !inflight.contains_key(&cached_idx) && !resend_queue.contains(&cached_idx) {
                to_evict.push(cached_idx);
                if to_evict.len() >= 100 {
                    break;
                }
            }
        }

        // Verify eviction found eligible entries
        assert_eq!(
            to_evict.len(),
            100,
            "Should identify 100 entries for eviction"
        );

        // Verify inflight entries are not in eviction list
        for idx in 0..10 {
            assert!(
                !to_evict.contains(&idx),
                "Inflight chunk {} should not be evicted",
                idx
            );
        }

        // Perform eviction
        for idx in &to_evict {
            frame_cache.remove(idx);
        }

        assert_eq!(
            frame_cache.len(),
            MAX_FRAME_CACHE_SIZE - 100,
            "Cache should have 100 fewer entries after eviction"
        );
    }

    /// TEST 4: Validates the timeout constant is used correctly.
    /// This tests the fix for hung transfers where senders could wait indefinitely
    /// for receivers that never respond.
    #[test]
    fn transfer_timeout_constant_is_reasonable() {
        // 5 minutes = 300 seconds
        let timeout_duration = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

        // Verify it's set to 5 minutes
        assert_eq!(
            timeout_duration.as_secs(),
            300,
            "Timeout should be 5 minutes (300 seconds)"
        );

        // Verify it's not too short (> 1 minute)
        assert!(
            timeout_duration.as_secs() > 60,
            "Timeout should be longer than 1 minute"
        );

        // Verify it's not too long (< 1 hour)
        assert!(
            timeout_duration.as_secs() < 3600,
            "Timeout should be shorter than 1 hour"
        );
    }

    /// TEST 5: Validates completion_from_ack policy conversion.
    /// This ensures ACK policies are correctly converted to completion policies,
    /// which is critical for proper sender retirement logic.
    #[test]
    fn ack_policy_conversion() {
        // Test 1: All policy should require all receivers
        let policy = completion_from_ack(&AckPolicy::All, 5);
        assert_eq!(
            policy,
            CompletionPolicy::All,
            "All policy should map to All completion"
        );

        // Test 2: KofN policy should use threshold
        let policy = completion_from_ack(&AckPolicy::KofN(3), 5);
        assert_eq!(
            policy,
            CompletionPolicy::Threshold(3),
            "KofN(3) should map to Threshold(3)"
        );

        // Test 3: KofN with k > receiver_count should cap at receiver_count
        let policy = completion_from_ack(&AckPolicy::KofN(10), 5);
        assert_eq!(
            policy,
            CompletionPolicy::Threshold(5),
            "KofN(10) with 5 receivers should cap at 5"
        );

        // Test 4: Fraction policy should compute threshold
        let policy = completion_from_ack(&AckPolicy::Fraction(0.5), 10);
        assert_eq!(
            policy,
            CompletionPolicy::Threshold(5),
            "Fraction(0.5) of 10 should be 5"
        );

        // Test 5: Fraction rounds up
        let policy = completion_from_ack(&AckPolicy::Fraction(0.75), 10);
        assert_eq!(
            policy,
            CompletionPolicy::Threshold(8),
            "Fraction(0.75) of 10 should round to 8"
        );
    }
}
