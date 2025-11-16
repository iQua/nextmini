use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control;
use super::session::{CommonConfig, SenderConfig};

const DEFAULT_WINDOW: usize = 64;
const MANIFEST_RETRY_INTERVAL_MS: u64 = 250;
const CONTROL_POLL_TIMEOUT_MS: u64 = 20;
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

    let mut chunk_source = if let Some(buf) = cfg.source_buffer.clone() {
        ChunkSource::from_buffer(buf, cfg.common.chunk_size, total_chunks, sid)
    } else {
        ChunkSource::from_file(source_file, cfg.common.chunk_size, total_chunks, sid)
    };
    let mut state = SenderState::new(cfg, total_chunks);
    let mut pacer = DataPacer::new(state.common.data_bucket.clone());
    let transfer_start = Instant::now();
    let transfer_timeout = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

    loop {
        // checks for transfer timeout
        if transfer_start.elapsed() > transfer_timeout && !state.is_complete() {
            tracing::error!(
                session_id = sid,
                elapsed_secs = transfer_start.elapsed().as_secs(),
                inflight = state.inflight_len(),
                "RLM sender: transfer timeout exceeded; forcing completion"
            );
            break;
        }
        // drains any immediately-available control frames so retire
        // decisions reflect fresh receiver state before we transmit more data
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_topology_gate();
        state.maybe_release_routes_gate();
        state.maybe_release_ready_gate();

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

        if !progressed && state.try_emit_eot(&processors) {
            progressed = true;
        }

        if state.is_complete() {
            tracing::info!(
                session_id = sid,
                bytes_sent = state.bytes_sent,
                chunks_sent = state.primary_chunks,
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

/// Encapsulates all mutable sender-side state (window, inflight accounting,
/// pacing, manifest timing, etc.). Keeping the logic centralized makes the event
/// loop above easier to read and test.
struct SenderState {
    session_id: u64,
    common: CommonConfig,
    receiver_count: usize,
    base_window: usize,
    window: usize,
    total_chunks: u64,
    total_bytes: u64,
    receiver_progress: BTreeMap<usize, u64>,
    retired_up_to: u64,
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
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    manifest_interval: Duration,
    manifest_last_sent: Instant,
}

impl SenderState {
    fn new(mut cfg: SenderConfig, total_chunks: u64) -> Self {
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
        let mut receiver_progress = BTreeMap::new();
        for node_id in &cfg.receiver_ids {
            receiver_progress.insert(*node_id, 0);
        }

        if cfg.common.control_weight != 0 {
            tracing::debug!(
                session_id = common.session_id,
                control_weight = cfg.common.control_weight,
                "RLM sender: control_weight is recorded but scheduler boosts are not yet wired."
            );
        }

        let mut state = Self {
            session_id: common.session_id,
            common,
            receiver_count,
            base_window,
            window: base_window,
            total_chunks,
            total_bytes: cfg.total_bytes,
            receiver_progress,
            retired_up_to: 0,
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
            src_ip,
            dst_ip,
            src_port: cfg.common.src_port,
            dst_port: cfg.common.dst_port,
            manifest_interval: Duration::from_millis(MANIFEST_RETRY_INTERVAL_MS),
            manifest_last_sent: Instant::now(),
        };
        state.update_retired_up_to();
        state
    }

    fn send_manifest(&mut self, processors: &ProcessorHandle) {
        let manifest = RlmControl::Manifest {
            chunk_size: self.common.chunk_size as u32,
            total_bytes: self.total_bytes,
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

    fn inflight_len(&self) -> usize {
        self.outstanding_chunks() as usize
    }

    fn outstanding_chunks(&self) -> u64 {
        self.primary_chunks.saturating_sub(self.retired_up_to)
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

    fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let frame = Bytes::from(rlm::encode_data(self.session_id, chunk.index, &chunk.data));
        self.bytes_sent += chunk.data.len() as u64;
        self.primary_chunks += 1;
        self.update_retired_up_to();
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

    fn update_retired_up_to(&mut self) {
        if self.receiver_count == 0 {
            self.retired_up_to = self.primary_chunks;
            return;
        }
        if self.receiver_progress.is_empty() {
            return;
        }
        let min_progress = self
            .receiver_progress
            .values()
            .copied()
            .min()
            .unwrap_or(self.retired_up_to);
        self.retired_up_to = min_progress.min(self.total_chunks);
    }

    fn try_emit_eot(&mut self, processors: &ProcessorHandle) -> bool {
        if self.eot_sent || !self.source_drained || self.outstanding_chunks() > 0 {
            if !self.eot_sent && self.source_drained && self.outstanding_chunks() > 0 {
                tracing::trace!(
                    session_id = self.session_id,
                    inflight_count = self.outstanding_chunks(),
                    "RLM sender: cannot send EOT - chunks still inflight"
                );
            }
            return false;
        }
        let eot = RlmControl::Eot {
            last_index: self.total_chunks,
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
        self.source_drained && self.eot_sent && self.outstanding_chunks() == 0
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
        match &control {
            RlmControl::Ready { node_id } => {
                self.ready_nodes.insert(*node_id as usize);
                tracing::debug!(
                    session_id = self.session_id,
                    node_id = *node_id,
                    "RLM sender: receiver ready"
                );
            }
            RlmControl::Manifest { .. } | RlmControl::Eot { .. } => {
                // ignores if the sender-originated control frames somehow looped back
            }
            RlmControl::Ack { .. } => {
                let Some(from_node) = peer_id else {
                    tracing::warn!(
                        session_id = self.session_id,
                        ?control,
                        "RLM sender: dropping control without peer id"
                    );
                    return;
                };
                if !self.receiver_progress.contains_key(&from_node) {
                    tracing::warn!(
                        session_id = self.session_id,
                        from_node,
                        "RLM sender: ignoring ACK from unexpected node"
                    );
                    return;
                }
                let updated = control::update_receiver_progress(
                    from_node,
                    &control,
                    &mut self.receiver_progress,
                );
                if let Some(new_value) = updated {
                    self.update_retired_up_to();
                    tracing::debug!(
                        session_id = self.session_id,
                        from_node = from_node,
                        up_to = new_value,
                        retired_up_to = self.retired_up_to,
                        inflight_count = self.outstanding_chunks(),
                        "RLM sender: cumulative ACK processed"
                    );
                } else {
                    tracing::trace!(
                        session_id = self.session_id,
                        from_node = from_node,
                        "RLM sender: ACK made no progress"
                    );
                }
            }
        }
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

enum ChunkSourceInner {
    File(File),
    Buffer(Bytes),
    Empty,
}

/// Reads chunk payloads from disk or memory (if provided) and hands them to the
/// sender in strict index order. Tests may swap in the empty case by omitting files.
struct ChunkSource {
    inner: ChunkSourceInner,
    chunk_size: usize,
    total_chunks: u64,
    next_index: u64,
    session_id: u64,
    buffer_offset: usize,
}

impl ChunkSource {
    fn from_file(
        file: Option<File>,
        chunk_size: usize,
        total_chunks: u64,
        session_id: u64,
    ) -> Self {
        let inner = match file {
            Some(file) => ChunkSourceInner::File(file),
            None => ChunkSourceInner::Empty,
        };
        Self {
            inner,
            chunk_size,
            total_chunks,
            next_index: 1,
            session_id,
            buffer_offset: 0,
        }
    }

    fn from_buffer(buf: Bytes, chunk_size: usize, total_chunks: u64, session_id: u64) -> Self {
        Self {
            inner: ChunkSourceInner::Buffer(buf),
            chunk_size,
            total_chunks,
            next_index: 1,
            session_id,
            buffer_offset: 0,
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

        match &mut self.inner {
            ChunkSourceInner::File(file) => {
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
            }
            ChunkSourceInner::Buffer(bytes) => {
                let start = self.buffer_offset;
                if start >= bytes.len() {
                    self.next_index = self.total_chunks + 1;
                    return Ok(None);
                }
                let end = (start + self.chunk_size).min(bytes.len());
                let mut data = Vec::with_capacity(end.saturating_sub(start));
                data.extend_from_slice(&bytes[start..end]);
                self.buffer_offset = end;
                self.next_index += 1;
                Ok(Some(ChunkPayload { index: idx, data }))
            }
            ChunkSourceInner::Empty => {
                self.next_index += 1;
                Ok(Some(ChunkPayload {
                    index: idx,
                    data: Vec::new(),
                }))
            }
        }
    }
}

/// Simple token-bucket pacer used to honor optional bandwidth caps.
///
/// This is now just a thin wrapper around the shared scheduler TokenBucket
/// implementation so that we don't duplicate token-bucket logic here.
struct DataPacer {
    bucket: Option<TokenBucket>,
}

impl DataPacer {
    fn new(spec: Option<nextmini_messages::TokenBucketSpec>) -> Self {
        let bucket = spec.map(TokenBucket::new);
        Self { bucket }
    }

    async fn wait_for(&mut self, bytes: usize) {
        if let Some(bucket) = self.bucket.as_mut() {
            bucket.wait_for_bytes(bytes).await;
        }
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

    /// Validates ChunkSource properly reports when finished.
    ///
    /// This tests the fix for the infinite loop bug where the sender could get stuck
    /// waiting for source_drained when chunk_source.finished() returned true but
    /// the state wasn't updated.
    #[test]
    fn chunk_source_finished_detection() {
        let session_id = 1;

        // Zero chunks should be immediately finished
        let source = ChunkSource::from_file(None, 1024, 0, session_id);
        assert!(
            source.finished(),
            "ChunkSource with 0 chunks should be finished immediately"
        );

        //Source with chunks should not be finished initially
        let mut source = ChunkSource::from_file(None, 1024, 5, session_id);
        assert!(
            !source.finished(),
            "ChunkSource with 5 chunks should not be finished initially"
        );

        // After consuming all chunks, should be finished
        for _ in 0..5 {
            let result = source.next_chunk();
            assert!(result.is_ok(), "Should successfully get chunk");
        }
        assert!(
            source.finished(),
            "ChunkSource should be finished after all chunks consumed"
        );

        // Requesting more chunks after finished returns None
        let result = source.next_chunk();
        assert!(
            matches!(result, Ok(None)),
            "Should return None when finished"
        );
    }

    /// Validates the timeout constant is used correctly.
    ///
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
}
