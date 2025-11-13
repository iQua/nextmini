use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::python::payload::{build_py_payload_segments, python_payload_budget};
use crate::node::{NodeId, NodeIdExt};

use super::api::{InboundFrame, SessionId};
use super::control::{self, CompletionPolicy};
use super::pgmcc::PgmccController;
use super::session::{AckPolicy, CommonConfig, CongestionControl, SenderConfig};

const DEFAULT_WINDOW: usize = 64;

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

    state.send_manifest(&processors);

    loop {
        // Drain any immediately-available control frames so resend/retire
        // decisions reflect fresh receiver state before we transmit more data.
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_ready_gate();
        if let Some(rate) = state.maybe_pgmcc_recompute() {
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

        if state.should_resend_manifest() {
            state.send_manifest(&processors);
            progressed = true;
        }

        // Fix: Explicitly mark source as drained when chunk source finishes
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

        // FIX: makes retransmitting chunk possible; removes ready_for_data() check
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
            if let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_millis(20), ctrl_rx.recv()).await
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
    pgmcc: Option<PgmccController>,
    total_chunks: u64,
    total_bytes: u64,
    inflight: BTreeMap<u64, HashSet<usize>>,
    frame_cache: BTreeMap<u64, Vec<Bytes>>,
    first_send_times: BTreeMap<u64, Instant>,
    resend_queue: BTreeSet<u64>,
    ready_nodes: HashSet<usize>,
    ready_gate_open: bool,
    ready_deadline: Option<Instant>,
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
    fragmenter: Option<FrameFragmenter>,
}

impl SenderState {
    fn new(cfg: SenderConfig, total_chunks: u64, completion_policy: CompletionPolicy) -> Self {
        let common = cfg.common.clone();
        let receiver_count = cfg.receiver_ids.len();
        let ready_gate_open = receiver_count == 0;
        let ready_grace_ms = cfg.ready_grace_ms.max(1);
        let ready_deadline = if ready_gate_open {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(ready_grace_ms))
        };
        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let dst_ip = common.group_ip;
        let base_window = compute_window(&cfg);
        let (window, pgmcc) = match &cfg.cc {
            CongestionControl::Static => (base_window, None),
            CongestionControl::Pgmcc(pcfg) => {
                let mut controller = PgmccController::new(pcfg.clone(), cfg.receiver_ids.clone());
                let init = pcfg
                    .init_cwnd_chunks
                    .max(pcfg.min_cwnd_chunks)
                    .min(pcfg.max_cwnd_chunks)
                    .min(base_window)
                    .max(1);
                controller.set_cwnd(init as f64);
                (init, Some(controller))
            }
        };
        let fragmenter = FrameFragmenter::new(&common);

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
            window,
            pgmcc,
            total_chunks,
            total_bytes: cfg.total_bytes,
            inflight: BTreeMap::new(),
            frame_cache: BTreeMap::new(),
            first_send_times: BTreeMap::new(),
            resend_queue: BTreeSet::new(),
            ready_nodes: HashSet::new(),
            ready_gate_open,
            ready_deadline,
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
            manifest_interval: Duration::from_millis(250),
            manifest_last_sent: Instant::now(),
            fragmenter,
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
        self.ready_gate_open && !self.source_drained
    }

    fn window_limit(&self) -> usize {
        self.window.max(1)
    }

    fn maybe_pgmcc_recompute(&mut self) -> Option<f64> {
        let controller = self.pgmcc.as_mut()?;
        let chunk_size = self.common.chunk_size;
        let now = Instant::now();
        let update = controller.maybe_recompute(now, self.base_window, chunk_size)?;
        self.window = update.window_chunks.max(1);
        tracing::debug!(
            session_id = self.session_id,
            acker = update.acker,
            window_chunks = update.window_chunks,
            rate_bps = (update.rate_bytes_per_s * 8.0) as u64,
            "RLM sender: PGMCC updated congestion window"
        );
        Some(update.rate_bytes_per_s)
    }

    fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    fn should_resend(&self) -> bool {
        !self.resend_queue.is_empty()
    }

    fn should_resend_manifest(&self) -> bool {
        !self.ready_gate_open && self.manifest_last_sent.elapsed() >= self.manifest_interval
    }

    fn mark_source_drained(&mut self) {
        self.source_drained = true;
    }

    fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let frame = rlm::encode_data(self.session_id, chunk.index, &chunk.data);
        let fragments = self.fragment_frame(frame);
        let now = Instant::now();
        self.first_send_times.entry(chunk.index).or_insert(now);
        self.enqueue_frame(chunk.index, fragments.clone());
        self.bytes_sent += chunk.data.len() as u64;
        self.primary_chunks += 1;
        tracing::debug!(
            session_id = self.session_id,
            chunk_index = chunk.index,
            chunk_data_len = chunk.data.len(),
            fragments_count = fragments.len(),
            src_ip = %self.src_ip,
            dst_ip = %self.dst_ip,
            "RLM sender: encoded DATA chunk, sending fragments to processor"
        );
        for (frag_idx, bytes) in fragments.iter().enumerate() {
            tracing::trace!(
                session_id = self.session_id,
                chunk_index = chunk.index,
                fragment_index = frag_idx,
                fragment_len = bytes.len(),
                "RLM sender: sending fragment to send_frame"
            );
            self.send_frame(bytes, processors);
        }
    }

    fn fragment_frame(&mut self, frame: Vec<u8>) -> Vec<Bytes> {
        if let Some(fragmenter) = self.fragmenter.as_mut() {
            fragmenter.fragment(frame)
        } else {
            vec![Bytes::from(frame)]
        }
    }

    fn send_resend(&mut self, processors: &ProcessorHandle) -> bool {
        let Some(idx) = self.resend_queue.iter().next().copied() else {
            return false;
        };
        if let Some(frames) = self.frame_cache.get(&idx) {
            self.resend_queue.remove(&idx);
            self.resend_count += 1;
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
                let Some(from_node) = peer_id else {
                    tracing::warn!(
                        session_id = self.session_id,
                        ?control,
                        "RLM sender: dropping control without peer id"
                    );
                    return;
                };
                if let Some(controller) = &mut self.pgmcc {
                    match &control {
                        RlmControl::Ack { up_to } => {
                            controller.on_ack(from_node, *up_to, &self.first_send_times, now);
                        }
                        RlmControl::Sack { base, runs } => {
                            controller.on_sack(from_node, *base, runs);
                        }
                        RlmControl::Repair { indices } => {
                            controller.on_repair(from_node, indices);
                        }
                        _ => {}
                    }
                }
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
        self.frame_cache.insert(idx, frames);
        self.inflight.entry(idx).or_default();
    }

    fn send_frame(&self, frame: &Bytes, processors: &ProcessorHandle) {
        tracing::debug!(
            session_id = self.session_id,
            frame_len = frame.len(),
            src_ip = %self.src_ip,
            src_port = self.src_port,
            dst_ip = %self.dst_ip,
            dst_port = self.dst_port,
            "RLM sender: send_frame building packet and calling processors.process_packet"
        );
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );
        tracing::debug!(
            session_id = self.session_id,
            flow_id = %packet.flow_id,
            packet_len = packet.packet_size,
            "RLM sender: packet built, calling processors.process_packet NOW"
        );
        processors.process_packet(packet);
        tracing::debug!(
            session_id = self.session_id,
            "RLM sender: processors.process_packet returned"
        );
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
        processors.process_packet(packet);
    }

    fn maybe_release_ready_gate(&mut self) {
        if self.ready_gate_open {
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

struct FrameFragmenter {
    session_id: SessionId,
    chunk_budget: usize,
    max_message_bytes: usize,
    next_message_id: u64,
}

impl FrameFragmenter {
    fn new(common: &CommonConfig) -> Option<Self> {
        if !common.fragmentation.enabled {
            return None;
        }
        let budget = match python_payload_budget(common.mtu) {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!(
                    session_id = common.session_id,
                    error = %err,
                    "RLM sender: disabling fragmentation due to MTU configuration"
                );
                return None;
            }
        };
        Some(Self {
            session_id: common.session_id,
            chunk_budget: budget,
            max_message_bytes: common.fragmentation.max_message_bytes,
            next_message_id: 1,
        })
    }

    fn fragment(&mut self, frame: Vec<u8>) -> Vec<Bytes> {
        if frame.len() <= self.chunk_budget {
            return vec![Bytes::from(frame)];
        }

        match build_py_payload_segments(
            &frame,
            self.chunk_budget,
            self.max_message_bytes,
            self.next_message_id(),
        ) {
            Ok(chunks) => chunks.into_iter().map(Bytes::from).collect(),
            Err(err) => {
                tracing::warn!(
                    session_id = self.session_id,
                    error = %err,
                    "RLM sender: fragmentation failed; falling back to single frame"
                );
                vec![Bytes::from(frame)]
            }
        }
    }

    fn next_message_id(&mut self) -> u64 {
        let id = self.next_message_id;
        self.next_message_id = self.next_message_id.wrapping_add(1).max(1);
        id
    }
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
        let buf = rlm::encode_data(sid, idx, &payload);
        let (hdr, data, body) = rlm::decode_data(&buf).expect("decode data");
        assert_eq!(hdr.session_id, sid);
        assert_eq!(data.index, idx);
        assert_eq!(data.payload_len as usize, plen);
        assert_eq!(body.len(), plen);
    }
}
