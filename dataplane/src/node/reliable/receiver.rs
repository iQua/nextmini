use bytes::Bytes;
use tokio::sync::mpsc;

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::session::ReceiverConfig;

const ACK_EVERY_CHUNKS: u64 = 16; // ensure <= sender DEFAULT_WINDOW

/// Utility for emitting completion control traffic (ACKs) via the node
/// processor stack using the same addressing the sender expects.
struct ControlEmitter {
    session_id: u64,
    src_ip: std::net::Ipv4Addr,
    src_port: u16,
    dst_ip: std::net::Ipv4Addr,
    dst_port: u16,
    processors: ProcessorHandle,
}

impl ControlEmitter {
    fn new(
        session_id: u64,
        src_ip: std::net::Ipv4Addr,
        src_port: u16,
        dst_ip: std::net::Ipv4Addr,
        dst_port: u16,
        processors: ProcessorHandle,
    ) -> Self {
        Self {
            session_id,
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            processors,
        }
    }

    fn send(&self, control: &RlmControl) {
        let buf = rlm::encode_control(self.session_id, control);

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            &buf,
        );

        self.processors.process_packet_blocking(packet);
    }
}

/// Drives a receiver session: consumes inbound frames, persists payloads in
/// order, and sends acknowledgement signals back to the sender.
pub async fn run(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let sid = cfg.common.session_id;
    tracing::info!(
        session_id = sid,
        expected_bytes = cfg.expected_bytes,
        "RLM receiver started"
    );

    // Stream bookkeeping: RLM chunk indices start at 1.
    let mut expected: u64 = 1;
    let per_chunk = cfg.common.chunk_size.max(1);

    // Uses a sliding window size corresponding to the burst size in the token bucket
    // If the token bucket shaper is not configured, use the default window size
    let window_size = cfg
        .common
        .data_bucket
        .as_ref()
        .map(|bucket| (bucket.bucket_size / per_chunk).max(1))
        .unwrap_or(super::sender::DEFAULT_WINDOW);

    let mut pending = PendingWindow::new(window_size, expected);
    let mut bytes_received: u64 = 0;
    let sink_buffer = cfg.sink_buffer.clone();

    let src_ip = (cfg.common.local_node_id as NodeId)
        .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
    let dst_ip = (cfg.source_node_id as NodeId)
        .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);

    // Source control traffic from the client (src) port to match sender expectations.
    let ctrl_src_port = cfg.common.src_port;
    let ctrl_dst_port = cfg.common.dst_port;

    let control_io = ControlEmitter::new(
        sid,
        src_ip,
        ctrl_src_port,
        dst_ip,
        ctrl_dst_port,
        processors.clone(),
    );

    control_io.send(&RlmControl::Ready {
        node_id: cfg.common.local_node_id as u64,
    });
    let mut last_ack_up_to: u64 = 0;
    let mut eot_index: Option<u64> = None;

    while let Some(frame) = rx.recv().await {
        tracing::trace!(
            session_id = sid,
            frame_len = frame.bytes.len(),
            "RLM receiver: received inbound frame"
        );
        if let Some((_, data, body)) = rlm::decode_data(&frame.bytes) {
            let ctx = FrameCtx {
                data: &data,
                body,
                expected: &mut expected,
                pending: &mut pending,
                bytes_received: &mut bytes_received,
            };
            let outcome = handle_data_frame(ctx);
            if !outcome.ready_chunks.is_empty()
                && let Some(buf) = &sink_buffer
            {
                let mut guard = buf.lock().await;
                for chunk in &outcome.ready_chunks {
                    guard.extend_from_slice(chunk);
                }
            }
            if outcome.advanced {
                let base = expected.saturating_sub(1);
                if base > last_ack_up_to {
                    let advanced_chunks = base - last_ack_up_to;
                    let final_chunk_reached = matches!(eot_index, Some(last) if last == base);
                    let received_all_bytes = bytes_received >= cfg.expected_bytes;
                    if advanced_chunks >= ACK_EVERY_CHUNKS
                        || final_chunk_reached
                        || received_all_bytes
                    {
                        tracing::debug!(
                            session_id = sid,
                            up_to = base,
                            expected = expected,
                            "RLM receiver: sending batched ACK"
                        );
                        control_io.send(&RlmControl::Ack { up_to: base });
                        last_ack_up_to = base;
                    }
                }
            }
            continue;
        }

        if handle_control_frame(&frame, &cfg, &control_io, &mut eot_index) {
            if let Some(last) = eot_index
                && expected.saturating_sub(1) >= last
            {
                break;
            }
            continue;
        }

        tracing::warn!(
            session_id = sid,
            "RLM receiver: received frame that was neither DATA nor CONTROL"
        );
    }

    tracing::info!(
        session_id = sid,
        bytes_received,
        last_index = expected.saturating_sub(1),
        "RLM receiver finished"
    );
}

/// Fixed-size buffer that keeps track of out-of-order chunks within the current
/// receiver window.
struct PendingWindow {
    base_index: u64,
    head: usize,
    slots: Vec<Option<Bytes>>,
}

impl PendingWindow {
    fn new(window_size: usize, base_index: u64) -> Self {
        let size = window_size.max(1);
        Self {
            base_index,
            head: 0,
            slots: vec![None; size],
        }
    }

    fn insert(&mut self, index: u64, payload: Bytes) -> bool {
        if index < self.base_index {
            return false;
        }
        let offset = index - self.base_index;
        if offset >= self.slots.len() as u64 {
            tracing::warn!(
                chunk_index = index,
                base_index = self.base_index,
                window = self.slots.len(),
                "RLM receiver: chunk outside pending window, dropping"
            );
            return false;
        }
        let slot_idx = self.slot_index(offset);
        if self.slots[slot_idx].is_none() {
            self.slots[slot_idx] = Some(payload);
            true
        } else {
            false
        }
    }

    fn take_contiguous_from(&mut self, expected: &mut u64) -> Vec<Bytes> {
        let mut ready = Vec::new();
        loop {
            if *expected < self.base_index {
                break;
            }
            let offset = *expected - self.base_index;
            if offset >= self.slots.len() as u64 {
                break;
            }
            let idx = self.slot_index(offset);
            match self.slots[idx].take() {
                Some(bytes) => {
                    ready.push(bytes);
                    *expected += 1;
                    self.advance_window();
                }
                None => break,
            }
        }
        ready
    }

    fn slot_index(&self, offset: u64) -> usize {
        if self.slots.is_empty() {
            return 0;
        }
        (self.head + offset as usize) % self.slots.len()
    }

    fn advance_window(&mut self) {
        self.base_index = self.base_index.saturating_add(1);
        if !self.slots.is_empty() {
            self.head = (self.head + 1) % self.slots.len();
        }
    }
}

/// Returns ordering updates and ready chunks when a DATA frame is processed.
struct FrameCtx<'a> {
    data: &'a rlm::RlmData,
    body: &'a [u8],
    expected: &'a mut u64,
    pending: &'a mut PendingWindow,
    bytes_received: &'a mut u64,
}

struct DataOutcome {
    ready_chunks: Vec<Bytes>,
    advanced: bool,
}

fn handle_data_frame(ctx: FrameCtx<'_>) -> DataOutcome {
    let idx = ctx.data.index;
    tracing::debug!(
        chunk_index = idx,
        body_len = ctx.body.len(),
        expected = *ctx.expected,
        "RLM receiver: DATA chunk received"
    );
    if idx < *ctx.expected {
        tracing::trace!(
            chunk_index = idx,
            expected = *ctx.expected,
            "RLM receiver: ignoring duplicate/old chunk"
        );
        return DataOutcome {
            ready_chunks: Vec::new(),
            advanced: false,
        };
    }

    let payload = Bytes::copy_from_slice(ctx.body);
    if ctx.pending.insert(idx, payload) {
        tracing::trace!(chunk_index = idx, "RLM receiver: chunk stored for ordering");
    }

    let ready_chunks = ctx.pending.take_contiguous_from(ctx.expected);
    if !ready_chunks.is_empty() {
        let ready_bytes: u64 = ready_chunks.iter().map(|chunk| chunk.len() as u64).sum();
        *ctx.bytes_received += ready_bytes;
    }
    let advanced = !ready_chunks.is_empty();
    DataOutcome {
        ready_chunks,
        advanced,
    }
}

/// Handles receiver-side control frames (Manifest/EOT/etc.).
fn handle_control_frame(
    frame: &InboundFrame,
    cfg: &ReceiverConfig,
    ctrl_io: &ControlEmitter,
    eot_index: &mut Option<u64>,
) -> bool {
    let Some((_, control)) = rlm::decode_control(&frame.bytes) else {
        return false;
    };
    match control {
        RlmControl::Manifest { .. } => {
            ctrl_io.send(&RlmControl::Ready {
                node_id: cfg.common.local_node_id as u64,
            });
            true
        }
        RlmControl::Eot { last_index } => {
            tracing::info!(
                session_id = cfg.common.session_id,
                last_index = last_index,
                "RLM receiver: EOT received"
            );
            *eot_index = Some(last_index);
            true
        }
        _ => true,
    }
}
