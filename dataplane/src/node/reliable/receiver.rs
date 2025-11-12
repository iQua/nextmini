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
use super::trace::manifest_from_bytes;

/// Maximum safe chunk size to avoid MTU issues.
/// Calculation: typical MTU (1500) - IP header (20) - TCP header (20) - RLM header (20) - RLM DATA header (12) - safety margin (100) = 1328
const MAX_SAFE_CHUNK_SIZE: usize = 1328;

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
        tracing::debug!(
            session_id = self.session_id,
            ?control,
            src = %self.src_ip,
            src_port = self.src_port,
            dst = %self.dst_ip,
            dst_port = self.dst_port,
            "RLM receiver: emitting control frame"
        );
        let buf = rlm::encode_control(self.session_id, control);
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            &buf,
        );
        let flow_id = packet.flow_id;
        let packet_size = packet.packet_size;
        tracing::debug!(
            session_id = self.session_id,
            control = ?control,
            flow = %flow_id,
            src = %self.src_ip,
            src_port = self.src_port,
            dst = %self.dst_ip,
            dst_port = self.dst_port,
            packet_size = packet_size,
            "RLM receiver: CONTROL packet built, sending to processor"
        );
        self.processors.process_packet(packet);
        tracing::debug!(
            session_id = self.session_id,
            control = ?control,
            flow = %flow_id,
            "RLM receiver: CONTROL packet sent to processor"
        );
    }
}

pub async fn run(
    mut cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let sid = cfg.common.session_id;

    // Validate and adjust chunk_size to avoid MTU issues
    if cfg.common.chunk_size > MAX_SAFE_CHUNK_SIZE {
        tracing::warn!(
            session_id = sid,
            original_chunk_size = cfg.common.chunk_size,
            max_safe_chunk_size = MAX_SAFE_CHUNK_SIZE,
            "RLM receiver: chunk_size exceeds safe MTU limit, automatically reducing to avoid packet fragmentation issues"
        );
        cfg.common.chunk_size = MAX_SAFE_CHUNK_SIZE;
    }

    tracing::debug!(
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
    tracing::debug!(
        session_id = sid,
        node_id = cfg.common.local_node_id,
        src = %src_ip,
        src_port = ctrl_src_port,
        dst = %dst_ip,
        dst_port = ctrl_dst_port,
        group = %cfg.common.group_ip,
        "RLM receiver: sending eager READY before MANIFEST"
    );
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
                    &cfg,
                    &control_io,
                    dst_ip,
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

    tracing::debug!(
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
    cfg: &ReceiverConfig,
    ctrl_io: &ControlEmitter<'_>,
    ctrl_dst_ip: std::net::Ipv4Addr,
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

            if let Some(meta) = manifest_from_bytes(&frame.bytes) {
                let ready_src_ip = (cfg.common.local_node_id as NodeId)
                    .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                tracing::debug!(
                    session_id = meta.session_id,
                    node_id = cfg.common.local_node_id,
                    peer = ?frame.peer_id,
                    source_node = ?frame.source_node_id,
                    ready_src = %ready_src_ip,
                    ready_dst = %ctrl_dst_ip,
                    dst_group = %cfg.common.group_ip,
                    "RLM receiver: MANIFEST received; READY re-sent toward source"
                );
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
