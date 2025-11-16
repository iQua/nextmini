use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, trace, warn};

use nextmini_messages::reliable_session::{self, ReliableSessionControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control;
use super::session::{CommonConfig, SenderConfig};

pub(super) const DEFAULT_WINDOW: usize = 512;
const MANIFEST_RETRY_INTERVAL_MS: u64 = 250;
const CONTROL_POLL_TIMEOUT_MS: u64 = 20;
const TRANSFER_TIMEOUT_SECS: u64 = 300;

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

    info!(
        session_id = sid,
        total_bytes,
        total_chunks,
        receivers = cfg.receiver_ids.len(),
        "Reliable sender started"
    );

    let chunk_bytes = cfg.common.chunk_size;
    let source_buffer = cfg.source_buffer.clone();
    let mut state = SenderState::new(cfg, total_chunks);
    let mut chunk_source = ChunkSource::new(source_buffer, chunk_bytes, total_chunks);
    let mut pacer = DataPacer::new(state.common.data_bucket.clone());
    let transfer_start = Instant::now();
    let transfer_timeout = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

    loop {
        // checks for transfer timeout
        if transfer_start.elapsed() > transfer_timeout && !state.is_complete() {
            error!(
                session_id = sid,
                elapsed_secs = transfer_start.elapsed().as_secs(),
                inflight = state.inflight_len(),
                "Reliable sender: transfer timeout exceeded; forcing completion"
            );
            break;
        }
        // drains any immediately-available control frames so retire
        // decisions reflect fresh receiver state before we transmit more data
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_topology_gate();
        state.maybe_release_ready_gate();

        let mut progressed = false;

        trace!(
            session_id = sid,
            ready_gate_open = state.ready_gate_open,
            source_drained = state.source_drained,
            ready_for_data = state.ready_for_data(),
            chunk_source_finished = chunk_source.finished(),
            inflight_len = state.inflight_len(),
            window = state.window_limit(),
            base_window = state.base_window,
            "Reliable sender: loop iteration"
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
            debug!(
                session_id = sid,
                ready_for_data = state.ready_for_data(),
                chunk_source_finished = chunk_source.finished(),
                inflight_len = state.inflight_len(),
                window = state.window_limit(),
                "Reliable sender: attempting to send next chunk"
            );
            match chunk_source.next_chunk() {
                Some(chunk) => {
                    debug!(
                        session_id = sid,
                        chunk_index = chunk.index,
                        chunk_size = chunk.data.len(),
                        "Reliable sender: sending data chunk"
                    );
                    pacer.wait_for(state.common.chunk_size).await;
                    state.send_data_chunk(chunk, &processors);
                    progressed = true;
                }
                None => {
                    state.mark_source_drained();
                    progressed = true;
                }
            }
        }

        if !progressed && state.try_emit_eot(&processors) {
            progressed = true;
        }

        if state.is_complete() {
            info!(
                session_id = sid,
                bytes_sent = state.bytes_sent,
                chunks_sent = state.primary_chunks,
                "Reliable sender finished with reliable delivery guarantees"
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
    topology_gate_open: bool,
    ready_gate_open: bool,
    ready_deadline: Option<Instant>,
    topology_ready_rx: Option<watch::Receiver<bool>>,
    ready_grace: Duration,
    manifest_sent: bool,
    manifest_last_sent: Instant,
    manifest_interval: Duration,
    source_drained: bool,
    eot_sent: bool,
    primary_chunks: u64,
    bytes_sent: u64,
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    // Throughput tracking
    throughput_start: Instant,
    throughput_last_report: Instant,
    bytes_since_last_report: u64,
}

impl SenderState {
    /// Builds a fresh state tracker for a sender session, wiring gate watchers
    /// and initializing per-receiver progress counters.
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

        let ready_deadline = None;

        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let dst_ip = common.dest_ip;
        let base_window = compute_window(&cfg);

        let mut receiver_progress = BTreeMap::new();
        for node_id in &cfg.receiver_ids {
            receiver_progress.insert(*node_id, 0);
        }

        if cfg.common.control_weight != 0 {
            debug!(
                session_id = common.session_id,
                control_weight = cfg.common.control_weight,
                "Reliable sender: control_weight is recorded but scheduler boosts are not yet wired."
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
            topology_gate_open,
            ready_gate_open,
            ready_deadline,
            topology_ready_rx,
            ready_grace,
            manifest_sent: false,
            manifest_last_sent: Instant::now(),
            manifest_interval: Duration::from_millis(MANIFEST_RETRY_INTERVAL_MS),
            source_drained: total_chunks == 0,
            eot_sent: false,
            primary_chunks: 0,
            bytes_sent: 0,
            src_ip,
            dst_ip,
            src_port: cfg.common.src_port,
            dst_port: cfg.common.dst_port,
            throughput_start: Instant::now(),
            throughput_last_report: Instant::now(),
            bytes_since_last_report: 0,
        };
        state.update_retired_up_to();
        state
    }

    /// Emit a MANIFEST describing the file transfer so receivers can prime their state.
    fn send_manifest(&mut self, processors: &ProcessorHandle) {
        let manifest = ReliableSessionControl::Manifest {
            chunk_size: self.common.chunk_size as u32,
            total_bytes: self.total_bytes,
        };
        self.send_control(&manifest, processors);
        self.manifest_last_sent = Instant::now();
        if !self.manifest_sent && self.receiver_count > 0 && self.ready_deadline.is_none() {
            self.ready_deadline = Some(Instant::now() + self.ready_grace);
        }
        self.manifest_sent = true;
        info!(
            session_id = self.session_id,
            src = %self.src_ip,
            dst = %self.dst_ip,
            dst_port = self.dst_port,
            total_bytes = self.total_bytes,
            chunk_size = self.common.chunk_size,
            receivers = self.receiver_count,
            "Reliable sender: MANIFEST sent"
        );
    }

    /// Determines whether the sender is allowed to transmit data frames.
    fn ready_for_data(&self) -> bool {
        self.topology_gate_open && self.ready_gate_open && !self.source_drained
    }

    /// Ensures the window never collapses to zero (which would deadlock the loop).
    fn window_limit(&self) -> usize {
        self.window.max(1)
    }

    /// Returns the number of outstanding chunks still waiting for ACKs.
    fn inflight_len(&self) -> usize {
        self.outstanding_chunks() as usize
    }

    /// Returns the number of chunks currently outside of the retired window.
    fn outstanding_chunks(&self) -> u64 {
        self.primary_chunks.saturating_sub(self.retired_up_to)
    }

    /// Decide whether we should re-send the MANIFEST while the ready gate stays closed.
    fn should_resend_manifest(&self) -> bool {
        self.manifest_sent
            && self.topology_gate_open
            && !self.ready_gate_open
            && self.manifest_last_sent.elapsed() >= self.manifest_interval
    }

    /// Determines if the sender should emit a MANIFEST based on topology/gate state.
    fn should_emit_manifest(&self) -> bool {
        if !self.topology_gate_open {
            return false;
        }
        if !self.manifest_sent {
            return true;
        }
        self.should_resend_manifest()
    }

    /// Marks the source buffer as fully drained (preventing redundant reads).
    fn mark_source_drained(&mut self) {
        self.source_drained = true;
    }

    // reports throughput every 1 second
    fn report_throughput(&mut self) {
        let now = Instant::now();
        let elapsed = now
            .duration_since(self.throughput_last_report)
            .as_secs_f64();

        if elapsed >= 1.0 && self.bytes_since_last_report > 0 {
            let throughput_gbps =
                (self.bytes_since_last_report as f64 * 8.0) / (elapsed * 1_000_000_000.0);
            let total_elapsed = now.duration_since(self.throughput_start).as_secs_f64();

            info!(
                session_id = self.session_id,
                throughput_gbps = format!("{:.3}", throughput_gbps),
                bytes = self.bytes_since_last_report,
                elapsed_s = format!("{:.3}", elapsed),
                total_sent = self.bytes_sent,
                total_elapsed_s = format!("{:.3}", total_elapsed),
                "Reliable sender: throughput"
            );

            self.bytes_since_last_report = 0;
            self.throughput_last_report = now;
        }
    }

    /// Encode and hand off a chunk to the processor, updating accounting.
    fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let frame = Bytes::from(reliable_session::encode_data(
            self.session_id,
            chunk.index,
            &chunk.data,
        ));

        let chunk_bytes = chunk.data.len() as u64;
        self.bytes_sent += chunk_bytes;
        self.bytes_since_last_report += chunk_bytes;
        self.primary_chunks += 1;
        self.update_retired_up_to();
        self.report_throughput();
        debug!(
            session_id = self.session_id,
            chunk_index = chunk.index,
            chunk_data_len = chunk.data.len(),
            src_ip = %self.src_ip,
            dst_ip = %self.dst_ip,
            "Reliable sender: encoded DATA chunk, sending frame to processor"
        );
        self.send_frame(&frame, processors);
    }

    /// Advance the retired watermark based on the slowest receiver.
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

    /// Emit an End-of-Transfer once all chunks have been acknowledged.
    fn try_emit_eot(&mut self, processors: &ProcessorHandle) -> bool {
        if self.eot_sent || !self.source_drained || self.outstanding_chunks() > 0 {
            if !self.eot_sent && self.source_drained && self.outstanding_chunks() > 0 {
                trace!(
                    session_id = self.session_id,
                    inflight_count = self.outstanding_chunks(),
                    "Reliable sender: cannot send EOT - chunks still inflight"
                );
            }
            return false;
        }
        let eot = ReliableSessionControl::Eot {
            last_index: self.total_chunks,
        };
        self.send_control(&eot, processors);
        self.eot_sent = true;

        info!(
            session_id = self.session_id,
            last_index = self.total_chunks,
            "Reliable sender: EOT sent"
        );
        true
    }

    /// Returns true when the sender drained the source and all acknowledgements were processed.
    fn is_complete(&self) -> bool {
        self.source_drained && self.eot_sent && self.outstanding_chunks() == 0
    }

    /// Handle READY/ACK/EOT control frames coming from receivers.
    fn handle_control(&mut self, frame: InboundFrame) {
        let InboundFrame { bytes, peer_id, .. } = frame;
        let Some((_, control)) = reliable_session::decode_control(&bytes) else {
            warn!(
                session_id = self.session_id,
                "Reliable sender: failed to decode control frame"
            );
            return;
        };
        match &control {
            ReliableSessionControl::Ready { node_id } => {
                self.ready_nodes.insert(*node_id as usize);
                debug!(
                    session_id = self.session_id,
                    node_id = *node_id,
                    "Reliable sender: receiver ready"
                );
            }
            ReliableSessionControl::Manifest { .. } | ReliableSessionControl::Eot { .. } => {
                // ignores if the sender-originated control frames somehow looped back
            }
            ReliableSessionControl::Ack { .. } => {
                let Some(from_node) = peer_id else {
                    warn!(
                        session_id = self.session_id,
                        ?control,
                        "Reliable sender: dropping control without peer id"
                    );
                    return;
                };
                if !self.receiver_progress.contains_key(&from_node) {
                    warn!(
                        session_id = self.session_id,
                        from_node, "Reliable sender: ignoring ACK from unexpected node"
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
                    debug!(
                        session_id = self.session_id,
                        from_node = from_node,
                        up_to = new_value,
                        retired_up_to = self.retired_up_to,
                        inflight_count = self.outstanding_chunks(),
                        "Reliable sender: cumulative ACK processed"
                    );
                } else {
                    trace!(
                        session_id = self.session_id,
                        from_node = from_node,
                        "Reliable sender: ACK made no progress"
                    );
                }
            }
        }
    }

    /// Serialize the already-encoded payload into a packet and enqueue it.
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

    /// Convenience helper for building and sending control packets.
    fn send_control(&self, control: &ReliableSessionControl, processors: &ProcessorHandle) {
        let buf = reliable_session::encode_control(self.session_id, control);

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            &buf,
        );

        processors.process_packet_blocking(packet);
    }

    /// Open the topology gate once the watch channel signals readiness.
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

            info!(
                session_id = self.session_id,
                "Reliable sender: topology-ready signal received"
            );
        }
    }

    /// Once every receiver signals READY (or we time out), unblock data transfer.
    fn maybe_release_ready_gate(&mut self) {
        if self.ready_gate_open || !self.manifest_sent {
            return;
        }

        if self.receiver_count > 0 && self.ready_nodes.len() == self.receiver_count {
            self.ready_gate_open = true;
            self.ready_deadline = None;

            info!(
                session_id = self.session_id,
                "Reliable sender: all receivers ready"
            );

            return;
        }

        if let Some(deadline) = self.ready_deadline
            && Instant::now() >= deadline
        {
            self.ready_gate_open = true;
            self.ready_deadline = None;
            warn!(
                session_id = self.session_id,
                ready = self.ready_nodes.len(),
                total = self.receiver_count,
                "Reliable sender: proceeding without all receivers ready"
            );
        }
    }
}

/// Compute a sliding window size based on the default limit and, if present,
/// the token-bucket shaper so we never admit more inflight bytes than the
/// pacer can service.
fn compute_window(cfg: &SenderConfig) -> usize {
    let mut window = DEFAULT_WINDOW;

    if let Some(bucket) = &cfg.common.data_bucket
        && cfg.common.chunk_size > 0
    {
        let per_chunk = cfg.common.chunk_size;
        let bucket_chunks = (bucket.bucket_size / per_chunk).max(1);
        window = window.min(bucket_chunks);
    }

    info!("The sliding window size is {window} on the source.");

    window
}

/// Materialized chunk that is ready to be encoded into an reliable session frame.
struct ChunkPayload {
    index: u64,
    data: Bytes,
}

/// Reads chunk payloads from memory and hands them to the sender in strict index order.
struct ChunkSource {
    bytes: Bytes,
    chunk_size: usize,
    total_chunks: u64,
    next_index: u64,
    buffer_offset: usize,
}

impl ChunkSource {
    /// Construct a new chunk source that will walk through the shared buffer.
    fn new(bytes: Bytes, chunk_size: usize, total_chunks: u64) -> Self {
        Self {
            bytes,
            chunk_size,
            total_chunks,
            next_index: 1,
            buffer_offset: 0,
        }
    }

    /// Returns true when every chunk has either been produced or the transfer was zero-length.
    fn finished(&self) -> bool {
        self.total_chunks == 0 || self.next_index > self.total_chunks
    }

    /// Returns the next chunk, advancing the internal cursor and sharing the
    /// underlying buffer instead of copying bytes.
    fn next_chunk(&mut self) -> Option<ChunkPayload> {
        if self.finished() {
            return None;
        }

        let start = self.buffer_offset;
        if start >= self.bytes.len() {
            self.next_index = self.total_chunks + 1;
            return None;
        }

        let end = (start + self.chunk_size).min(self.bytes.len());

        // Zero-copy slice – O(1), shares underlying buffer
        let data = self.bytes.slice(start..end);

        self.buffer_offset = end;
        let idx = self.next_index;
        self.next_index += 1;

        Some(ChunkPayload { index: idx, data })
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
    /// Builds a pacer backed by the runtime token-bucket implementation.
    fn new(spec: Option<nextmini_messages::TokenBucketSpec>) -> Self {
        let bucket = spec.map(TokenBucket::new);
        Self { bucket }
    }

    /// Await scheduling tokens before sending `bytes` worth of payload.
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
        let buf = reliable_session::encode_data(sid, idx, &payload);
        let (hdr, data, body) = reliable_session::decode_data(&buf).expect("decode data");

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
        // Zero chunks should be immediately finished
        let source = ChunkSource::new(Bytes::new(), 1024, 0);
        assert!(
            source.finished(),
            "ChunkSource with 0 chunks should be finished immediately"
        );

        //Source with chunks should not be finished initially
        let mut source = ChunkSource::new(Bytes::from(vec![0u8; 1024 * 5]), 1024, 5);
        assert!(
            !source.finished(),
            "ChunkSource with 5 chunks should not be finished initially"
        );

        // After consuming all chunks, should be finished
        for _ in 0..5 {
            let result = source.next_chunk();
            assert!(result.is_some(), "Should successfully get chunk");
        }
        assert!(
            source.finished(),
            "ChunkSource should be finished after all chunks consumed"
        );

        // Requesting more chunks after finished returns None
        let result = source.next_chunk();
        assert!(result.is_none(), "Should return None when finished");
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
