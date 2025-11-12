use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::time::{self, Sleep};

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control::{NackLimiter, SackScheduler, SackSnapshot};
use super::session::ReceiverConfig;

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
    let ctrl_dst_port = cfg.common.src_port;

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
    let mut ready_sent = true;
    let mut last_ack_up_to: u64 = 0;
    let mut eot_index: Option<u64> = None;
    let mut nack_limiter = NackLimiter::new(Duration::from_millis(cfg.nack_min_interval_ms.max(1)));
    let mut sack_scheduler = SackScheduler::new(Duration::from_millis(cfg.sack_interval_ms));
    let mut sack_timer: Option<Pin<Box<Sleep>>> = None;

    loop {
        tokio::select! {
            maybe_frame = rx.recv() => {
                let Some(frame) = maybe_frame else {
                    break;
                };

                if handle_data_frame(
                    &frame,
                    &mut expected,
                    &mut highest_seen,
                    &mut pending,
                    &mut received,
                    &mut bytes_received,
                    file.as_mut(),
                ) {
                    let base = expected.saturating_sub(1);
                    if base > last_ack_up_to {
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
                            let now = Instant::now();
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
                        && nack_limiter.should_send(expected, Instant::now())
                    {
                        if cfg.nack_jitter_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(cfg.nack_jitter_ms)).await;
                        }
                        control_io.send(&RlmControl::Repair {
                            indices: vec![expected],
                        });
                    }
                    continue;
                }

                if handle_control_frame(
                    &frame,
                    &mut ready_sent,
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

fn handle_data_frame(
    frame: &InboundFrame,
    expected: &mut u64,
    highest_seen: &mut u64,
    pending: &mut BTreeMap<u64, Bytes>,
    received: &mut BTreeSet<u64>,
    bytes_received: &mut u64,
    mut file: Option<&mut std::fs::File>,
) -> bool {
    let Some((_, data, body)) = rlm::decode_data(&frame.bytes) else {
        return false;
    };
    let idx = data.index;
    if idx < *expected {
        return true;
    }

    let payload = Bytes::copy_from_slice(body);
    highest_seen.set_max(idx);
    if pending.insert(idx, payload).is_none() {
        received.insert(idx);
    }

    while let Some(bytes) = pending.remove(expected) {
        received.remove(expected);
        *bytes_received += bytes.len() as u64;
        if let Some(f) = file.as_mut() {
            let _ = (**f).write_all(&bytes);
        }
        *expected += 1;
    }
    true
}

fn handle_control_frame(
    frame: &InboundFrame,
    ready_sent: &mut bool,
    cfg: &ReceiverConfig,
    ctrl_io: &ControlEmitter<'_>,
    eot_index: &mut Option<u64>,
) -> bool {
    let Some((_, control)) = rlm::decode_control(&frame.bytes) else {
        return false;
    };
    match control {
        RlmControl::Manifest { .. } => {
            if !*ready_sent {
                ctrl_io.send(&RlmControl::Ready {
                    node_id: cfg.common.local_node_id as u64,
                });
                *ready_sent = true;
            }
            true
        }
        RlmControl::Eot { last_index, .. } => {
            *eot_index = Some(last_index);
            true
        }
        _ => true,
    }
}

fn emit_sack(ctrl_io: &ControlEmitter<'_>, snapshot: SackSnapshot) {
    ctrl_io.send(&RlmControl::Sack {
        base: snapshot.base,
        runs: snapshot.runs,
    });
}

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
    fn set_max(&mut self, other: Self) {
        if other > *self {
            *self = other;
        }
    }
}
