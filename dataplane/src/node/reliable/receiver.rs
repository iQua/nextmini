use std::collections::BTreeMap;

use bytes::Bytes;
use tokio::sync::mpsc;

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::session::ReceiverConfig;

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
    let mut pending: BTreeMap<u64, Bytes> = BTreeMap::new();
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
            if !outcome.ready_chunks.is_empty() {
                if let Some(buf) = &sink_buffer {
                    let mut guard = buf.lock().await;
                    for chunk in &outcome.ready_chunks {
                        guard.extend_from_slice(chunk);
                    }
                }
            }
            if outcome.advanced {
                let base = expected.saturating_sub(1);
                if base > last_ack_up_to {
                    tracing::debug!(
                        session_id = sid,
                        up_to = base,
                        expected = expected,
                        "RLM receiver: sending ACK"
                    );
                    control_io.send(&RlmControl::Ack { up_to: base });
                    last_ack_up_to = base;
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

/// Returns ordering updates and ready chunks when a DATA frame is processed.
struct FrameCtx<'a> {
    data: &'a rlm::RlmData,
    body: &'a [u8],
    expected: &'a mut u64,
    pending: &'a mut BTreeMap<u64, Bytes>,
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
    if ctx.pending.insert(idx, payload).is_none() {
        tracing::trace!(chunk_index = idx, "RLM receiver: chunk stored for ordering");
    }

    let mut ready_chunks = Vec::new();
    while let Some(bytes) = ctx.pending.remove(ctx.expected) {
        *ctx.bytes_received += bytes.len() as u64;
        ready_chunks.push(bytes);
        *ctx.expected += 1;
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
