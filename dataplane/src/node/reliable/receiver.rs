use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{self, Sleep};

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control::{NackLimiter, SackScheduler, SackSnapshot};
use super::session::{CongestionControl, ReceiverConfig};
use super::tfmcc::TfmccReceiver;

/// Utility for emitting control traffic (ACK/SACK/NACK/etc.) via the node
/// processor stack using the same addressing the sender expects.
struct ControlEmitter<'a> {
    session_id: u64,
    src_ip: std::net::Ipv4Addr,
    src_port: u16,
    dst_ip: std::net::Ipv4Addr,
    dst_port: u16,
    processors: &'a ProcessorHandle,
}

impl<'a> ControlEmitter<'a> {
    fn new(
        session_id: u64,
        src_ip: std::net::Ipv4Addr,
        src_port: u16,
        dst_ip: std::net::Ipv4Addr,
        dst_port: u16,
        processors: &'a ProcessorHandle,
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
        self.processors.process_packet(packet);
    }
}

/// Drives a receiver session: consumes inbound frames, persists payloads in
/// order, and feeds back control signals so the sender can repair gaps.
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

    if cfg.verify_checksum {
        tracing::warn!(
            session_id = sid,
            "RLM receiver: checksum verification requested but not yet implemented."
        );
    }

    // Stream bookkeeping: RLM chunk indices start at 1.
    let mut expected: u64 = 1;
    let mut highest_seen: u64 = 0;
    let mut pending: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut received: BTreeSet<u64> = BTreeSet::new();
    let mut bytes_received: u64 = 0;
    let mut file = cfg
        .sink_path
        .as_ref()
        .and_then(|path| match std::fs::File::create(path) {
            Ok(f) => Some(f),
            Err(error) => {
                tracing::error!(
                    session_id = sid,
                    path = %path,
                    %error,
                    "RLM receiver: failed to create sink file"
                );
                None
            }
        });

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
        &processors,
    );

    control_io.send(&RlmControl::Ready {
        node_id: cfg.common.local_node_id as u64,
    });
    let mut tfmcc = match &cfg.cc {
        CongestionControl::Tfmcc(tcfg) => Some(TfmccReceiver::new(
            cfg.common.local_node_id as u32,
            tcfg.clone(),
            cfg.common.chunk_size,
        )),
        CongestionControl::Static => None,
    };
    let mut last_ack_up_to: u64 = 0;
    let mut eot_index: Option<u64> = None;
    let mut nack_limiter = NackLimiter::new(Duration::from_millis(cfg.nack_min_interval_ms.max(1)));
    let mut sack_scheduler = SackScheduler::new(Duration::from_millis(cfg.sack_interval_ms));
    // Tokio timer used to coalesce SACK traffic when the peer leaves gaps.
    let mut sack_timer: Option<Pin<Box<Sleep>>> = None;

    loop {
        tokio::select! {
            maybe_frame = rx.recv() => {
                let Some(frame) = maybe_frame else {
                    break;
                };

                tracing::trace!(
                    session_id = sid,
                    frame_len = frame.bytes.len(),
                    "RLM receiver: received inbound frame"
                );
                if let Some((_, data, body)) = rlm::decode_data(&frame.bytes) {
                    let now = Instant::now();
                    if let Some(state) = tfmcc.as_mut() {
                        if let Some(header) = data.tfmcc {
                            state.on_data_header(&header, now);
                        }
                        state.on_chunk(data.index, now);
                    }
                    let ctx = FrameCtx {
                        data: &data,
                        body,
                        expected: &mut expected,
                        highest_seen: &mut highest_seen,
                        pending: &mut pending,
                        received: &mut received,
                        bytes_received: &mut bytes_received,
                    };
                    if handle_data_frame(ctx, file.as_mut()) {
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
                        if highest_seen > base {
                            let (ack_base, runs) =
                                rlm::build_ack_and_sack(expected, &received, highest_seen);
                            if runs.is_empty() {
                                sack_scheduler.clear();
                                sack_timer = None;
                            } else {
                                sack_scheduler.record(ack_base, runs);
                                if let Some(snapshot) = sack_scheduler.take_ready(now) {
                                    emit_sack(&control_io, snapshot);
                                }
                                reset_sack_timer(&mut sack_timer, &sack_scheduler, now);
                            }
                        } else if sack_scheduler.has_snapshot() {
                            sack_scheduler.clear();
                            sack_timer = None;
                        }
                        if highest_seen >= expected
                            && nack_limiter.should_send(expected, now)
                        {
                            tracing::debug!(
                                session_id = sid,
                                expected = expected,
                                highest_seen = highest_seen,
                                gap_size = highest_seen - expected,
                                "RLM receiver: sending REPAIR/NACK request"
                            );
                            if cfg.nack_jitter_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(cfg.nack_jitter_ms)).await;
                            }
                            control_io.send(&RlmControl::Repair {
                                indices: vec![expected],
                            });
                        } else if highest_seen >= expected {
                            tracing::trace!(
                                session_id = sid,
                                expected = expected,
                                highest_seen = highest_seen,
                                "RLM receiver: gap detected but NACK limiter blocked send"
                            );
                        }
                        if let Some(state) = tfmcc.as_mut()
                            && let Some(feedback) = state.maybe_feedback(now) {
                                control_io.send(&feedback);
                            }
                        continue;
                    }
                }

                if handle_control_frame(
                    &frame,
                    &cfg,
                    &control_io,
                    &mut eot_index,
                ) {
                    if let Some(last) = eot_index && expected.saturating_sub(1) >= last {
                        break;
                    }
                    continue;
                }

                tracing::warn!(
                    session_id = sid,
                    "RLM receiver: received frame that was neither DATA nor CONTROL"
                );
            }
            _ = async {
                if let Some(timer) = &mut sack_timer {
                    timer.as_mut().await;
                }
            }, if sack_timer.is_some() => {
                sack_timer = None;
                let now = Instant::now();
                if let Some(snapshot) = sack_scheduler.take_ready(now) {
                    emit_sack(&control_io, snapshot);
                }
                reset_sack_timer(&mut sack_timer, &sack_scheduler, now);
            }
        }
    }

    if let Some(f) = file.as_mut() {
        let _ = f.sync_all();
    }

    tracing::info!(
        session_id = sid,
        bytes_received,
        last_index = expected.saturating_sub(1),
        "RLM receiver finished"
    );
}

/// Returns true when the frame decoded as DATA and updates ordering state.
struct FrameCtx<'a> {
    data: &'a rlm::RlmData,
    body: &'a [u8],
    expected: &'a mut u64,
    highest_seen: &'a mut u64,
    pending: &'a mut BTreeMap<u64, Bytes>,
    received: &'a mut BTreeSet<u64>,
    bytes_received: &'a mut u64,
}

fn handle_data_frame(ctx: FrameCtx<'_>, mut file: Option<&mut std::fs::File>) -> bool {
    let idx = ctx.data.index;
    tracing::debug!(
        chunk_index = idx,
        body_len = ctx.body.len(),
        expected = *ctx.expected,
        highest_seen = *ctx.highest_seen,
        "RLM receiver: DATA chunk received"
    );
    if idx < *ctx.expected {
        tracing::trace!(
            chunk_index = idx,
            expected = *ctx.expected,
            "RLM receiver: ignoring duplicate/old chunk"
        );
        return true;
    }

    let payload = Bytes::copy_from_slice(ctx.body);
    ctx.highest_seen.set_max(idx);
    if ctx.pending.insert(idx, payload).is_none() {
        ctx.received.insert(idx);
    }

    while let Some(bytes) = ctx.pending.remove(ctx.expected) {
        ctx.received.remove(ctx.expected);
        *ctx.bytes_received += bytes.len() as u64;
        if let Some(f) = file.as_mut() {
            let _ = (**f).write_all(&bytes);
        }
        *ctx.expected += 1;
    }
    true
}

/// Handles receiver-side control frames (Manifest/EOT/etc.).
fn handle_control_frame(
    frame: &InboundFrame,
    cfg: &ReceiverConfig,
    ctrl_io: &ControlEmitter<'_>,
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
        RlmControl::Eot { last_index, .. } => {
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

/// Serializes and emits the provided SACK snapshot.
fn emit_sack(control_io: &ControlEmitter, snapshot: SackSnapshot) {
    tracing::debug!(
        session_id = control_io.session_id,
        base = snapshot.base,
        runs_count = snapshot.runs.len(),
        "RLM receiver: sending SACK"
    );
    control_io.send(&RlmControl::Sack {
        base: snapshot.base,
        runs: snapshot.runs,
    });
}

/// Arms or clears the SACK timer based on the scheduler's next deadline.
fn reset_sack_timer(timer: &mut Option<Pin<Box<Sleep>>>, scheduler: &SackScheduler, now: Instant) {
    if let Some(deadline) = scheduler.next_deadline(now) {
        let when = time::Instant::from_std(deadline);
        *timer = Some(Box::pin(time::sleep_until(when)));
    } else {
        *timer = None;
    }
}

trait MaxAssign {
    fn set_max(&mut self, other: Self);
}

impl MaxAssign for u64 {
    /// Keeps the maximum observed chunk index without branching at call sites.
    fn set_max(&mut self, other: Self) {
        if other > *self {
            *self = other;
        }
    }
}
