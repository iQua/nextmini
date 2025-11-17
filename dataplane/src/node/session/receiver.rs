use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use nextmini_messages::reliable_session::{self, ReliableSessionControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::InboundFrame;
use crate::node::session::runtime::ReceiverConfig;
use crate::node::{NodeId, NodeIdExt};

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
    /// Prepare an emitter that can forward reliable session control traffic back through the
    /// node's processor pipeline.
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

    /// Encode and inject a single control frame.
    fn send(&self, control: &ReliableSessionControl) {
        // Use stack-allocated buffer to avoid heap allocation for small control frames
        let mut buf = [0u8; reliable_session::MAX_CONTROL_FRAME_SIZE];
        let frame = reliable_session::encode_control_into(&mut buf, self.session_id, control);

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
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
    info!(
        session_id = sid,
        expected_bytes = cfg.expected_bytes,
        "Reliable receiver started"
    );

    // Stream bookkeeping: reliable session chunk indices start at 1.
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

    control_io.send(&ReliableSessionControl::Ready {
        node_id: cfg.common.local_node_id as u64,
    });
    let mut last_ack_up_to: u64 = 0;
    let mut eot_index: Option<u64> = None;

    while let Some(frame) = rx.recv().await {
        trace!(
            session_id = sid,
            frame_len = frame.bytes.len(),
            "Reliable receiver: received inbound frame"
        );
        if let Some((_, data, body)) = reliable_session::decode_data(&frame.bytes) {
            // Map the payload slice onto the underlying Vec so we can take a zero-copy Bytes view.
            let payload_range = {
                let base_ptr = frame.bytes.as_ptr() as usize;
                let start = body.as_ptr() as usize - base_ptr;
                let end = start + body.len();
                start..end
            };
            let frame_bytes = Bytes::from(frame.bytes);
            let payload = frame_bytes.slice(payload_range);

            let ctx = FrameCtx {
                data: &data,
                payload,
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
                        debug!(
                            session_id = sid,
                            up_to = base,
                            expected = expected,
                            "Reliable receiver: sending batched ACK"
                        );
                        control_io.send(&ReliableSessionControl::Ack { up_to: base });
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

        warn!(
            session_id = sid,
            "Reliable receiver: received frame that was neither DATA nor CONTROL"
        );
    }

    info!(
        session_id = sid,
        bytes_received,
        last_index = expected.saturating_sub(1),
        "Reliable receiver finished"
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
    /// Create a pending window sized to the configured sliding window.
    fn new(window_size: usize, base_index: u64) -> Self {
        let size = window_size.max(1);
        Self {
            base_index,
            head: 0,
            slots: vec![None; size],
        }
    }

    /// Attempt to store a chunk for later delivery; returns true if it landed in
    /// the buffer and false if it was out of range or a duplicate.
    fn insert(&mut self, index: u64, payload: Bytes) -> bool {
        if index < self.base_index {
            return false;
        }
        let offset = index - self.base_index;
        if offset >= self.slots.len() as u64 {
            warn!(
                chunk_index = index,
                base_index = self.base_index,
                window = self.slots.len(),
                "Reliable receiver: chunk outside pending window, dropping"
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

    /// Drain any contiguous payloads starting at `expected`, advancing the base
    /// index so future inserts can land.
    fn take_contiguous_from(&mut self, expected: &mut u64) -> Vec<Bytes> {
        let mut ready = Vec::new();
        let mut chunks_to_advance = 0u64;

        // First pass: collect all contiguous chunks
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
                    chunks_to_advance += 1;
                }
                None => break,
            }
        }

        // Bulk advance the window (if we collected any chunks)
        if chunks_to_advance > 0 {
            self.advance_window_by(chunks_to_advance);
        }

        ready
    }

    /// Translate a logical offset relative to `base_index` into a circular slot.
    fn slot_index(&self, offset: u64) -> usize {
        if self.slots.is_empty() {
            return 0;
        }
        (self.head + offset as usize) % self.slots.len()
    }

    /// Advance the window by the specified number of slots.
    /// This efficiently handles both single and bulk advances.
    fn advance_window_by(&mut self, count: u64) {
        if count == 0 {
            return;
        }

        self.base_index = self.base_index.saturating_add(count);
        if !self.slots.is_empty() {
            // For large advances, use modulo to avoid overflow
            let count_usize = count as usize;
            self.head = (self.head + count_usize) % self.slots.len();
        }
    }
}

/// Borrowed state required to evaluate a DATA frame.
struct FrameCtx<'a> {
    data: &'a reliable_session::ReliableSessionData,
    payload: Bytes,
    expected: &'a mut u64,
    pending: &'a mut PendingWindow,
    bytes_received: &'a mut u64,
}

/// Outcome describing whether the new frame unlocked bytes for delivery.
struct DataOutcome {
    ready_chunks: Vec<Bytes>,
    advanced: bool,
}

/// Handles ordering/bookkeeping for a single reliable DATA frame.
fn handle_data_frame(ctx: FrameCtx<'_>) -> DataOutcome {
    let FrameCtx {
        data,
        payload,
        expected,
        pending,
        bytes_received,
    } = ctx;

    let idx = data.index;

    if idx < *expected {
        return DataOutcome {
            ready_chunks: Vec::new(),
            advanced: false,
        };
    }

    if pending.insert(idx, payload) {
        trace!(
            chunk_index = idx,
            "Reliable receiver: chunk stored for ordering"
        );
    }

    let ready_chunks = pending.take_contiguous_from(expected);
    if !ready_chunks.is_empty() {
        let ready_bytes: u64 = ready_chunks.iter().map(|chunk| chunk.len() as u64).sum();
        *bytes_received += ready_bytes;
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
    let Some((_, control)) = reliable_session::decode_control(&frame.bytes) else {
        return false;
    };
    match control {
        ReliableSessionControl::Manifest { .. } => {
            ctrl_io.send(&ReliableSessionControl::Ready {
                node_id: cfg.common.local_node_id as u64,
            });
            true
        }
        ReliableSessionControl::Eot { last_index } => {
            info!(
                session_id = cfg.common.session_id,
                last_index = last_index,
                "Reliable receiver: EOT received"
            );
            *eot_index = Some(last_index);
            true
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_window_bulk_advance() {
        let mut window = PendingWindow::new(16, 1);
        let mut expected = 1u64;

        // Insert chunks 1-5 in order
        for i in 1..=5 {
            let payload = Bytes::from(vec![i as u8; 100]);
            assert!(window.insert(i, payload), "Should insert chunk {}", i);
        }

        // Drain all contiguous chunks (should advance by 5)
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 5, "Should have drained 5 chunks");
        assert_eq!(expected, 6, "Expected should advance to 6");
        assert_eq!(window.base_index, 6, "Base index should advance to 6");
        assert_eq!(window.head, 5, "Head should advance by 5");

        // Insert chunk 10 (out of order)
        let payload = Bytes::from(vec![10u8; 100]);
        assert!(window.insert(10, payload), "Should insert chunk 10");

        // Try to drain - should get nothing since 6-9 are missing
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 0, "Should not drain non-contiguous chunks");
        assert_eq!(expected, 6, "Expected should stay at 6");
        assert_eq!(window.base_index, 6, "Base index should stay at 6");

        // Fill in chunks 6-9
        for i in 6..=9 {
            let payload = Bytes::from(vec![i as u8; 100]);
            assert!(window.insert(i, payload), "Should insert chunk {}", i);
        }

        // Now drain should get 6-10 (5 chunks) in one bulk operation
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 5, "Should drain chunks 6-10");
        assert_eq!(expected, 11, "Expected should advance to 11");
        assert_eq!(window.base_index, 11, "Base index should advance to 11");
        // Head advanced by 5 from position 5: (5 + 5) % 16 = 10
        assert_eq!(window.head, 10, "Head should wrap correctly");
    }

    #[test]
    fn pending_window_wrapping() {
        let mut window = PendingWindow::new(8, 1);
        let mut expected = 1u64;

        // Insert and drain enough to wrap around
        for batch in 0..3 {
            let start = batch * 8 + 1;
            for i in start..start + 8 {
                let payload = Bytes::from(vec![i as u8; 100]);
                assert!(window.insert(i, payload));
            }
            let ready = window.take_contiguous_from(&mut expected);
            assert_eq!(ready.len(), 8);
            assert_eq!(expected, start + 8);
        }

        // After 3 batches of 8, we should have advanced 24 slots
        assert_eq!(window.base_index, 25);
        // Head should wrap: (0 + 24) % 8 = 0
        assert_eq!(window.head, 0);
    }

    #[test]
    fn pending_window_duplicate_insert() {
        let mut window = PendingWindow::new(16, 1);

        let payload1 = Bytes::from(vec![1u8; 100]);
        let payload2 = Bytes::from(vec![2u8; 100]);

        // First insert should succeed
        assert!(window.insert(5, payload1), "First insert should succeed");

        // Duplicate insert should fail
        assert!(!window.insert(5, payload2), "Duplicate insert should fail");
    }

    #[test]
    fn pending_window_out_of_range() {
        let mut window = PendingWindow::new(8, 10);

        // Below base_index
        let payload = Bytes::from(vec![1u8; 100]);
        assert!(
            !window.insert(5, payload),
            "Should reject chunk below base_index"
        );

        // Beyond window size
        let payload = Bytes::from(vec![2u8; 100]);
        assert!(
            !window.insert(20, payload),
            "Should reject chunk beyond window"
        );
    }
}
