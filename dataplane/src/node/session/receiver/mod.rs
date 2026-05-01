//! Receiver task for block-first lossless sessions.
//!
//! The receiver accepts a manifest, records completed plain blocks locally, and
//! optionally accumulates FEC symbols until a block can be decoded. After
//! `SourceDone(round_id)`, plain mode emits end-of-round `Need` feedback while
//! FEC mode emits one aggregate `Need` describing either completion or the
//! remaining per-block deficits for the next retransmit round.

mod fec;
mod plain;

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, FecScheme, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode, MissingBlockRange, NeedReport,
};

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::SessionId;
use crate::node::session::api::{CompletedReceiverReplay, InboundFrame, LosslessRuntimeMessage};
use crate::node::session::control;
use crate::node::session::plan::BlockPlan;
use crate::node::session::runtime::{ReceiverConfig, TransportRoute};
use crate::node::session::timing;

use self::fec::FecReceiver;
use self::plain::PlainReceiver;

/// Run one receiver session until the transfer is complete or the channel closes.
#[allow(dead_code)]
pub async fn run(
    cfg: ReceiverConfig,
    rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    run_with_runtime(cfg, rx, processors, None).await;
}

pub(super) async fn run_with_runtime(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
    runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
) {
    let mut receiver = SessionReceiver::new(cfg, processors);
    receiver.run(&mut rx, runtime_sender).await;
}

/// Stateful receiver loop shared by plain and FEC transfer modes.
struct SessionReceiver {
    shared: ReceiverShared,
    mode: Option<ReceiverMode>,
    lifecycle: ReceiverLifecycle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiverLifecycle {
    Active,
    PassiveComplete,
    SessionFinished,
}

/// Receiver state that is truly common across plain and FEC modes.
pub(super) struct ReceiverShared {
    pub(super) session_id: SessionId,
    pub(super) route: TransportRoute,
    pub(super) local_node_id: usize,
    pub(super) cfg: ReceiverConfig,
    pub(super) processors: ProcessorHandle,
    pub(super) manifest: Option<LosslessSessionManifest>,
    pub(super) plan: Option<BlockPlan>,
    pub(super) complete_blocks: BTreeSet<u64>,
    payload_frames_received: u64,
    payload_bytes_received: u64,
}

/// Concrete receiver mode selected after the manifest is installed.
enum ReceiverMode {
    Plain(PlainReceiver),
    Fec(FecReceiver),
}

impl SessionReceiver {
    /// Build receiver state for one lossless session.
    fn new(cfg: ReceiverConfig, processors: ProcessorHandle) -> Self {
        Self {
            shared: ReceiverShared {
                session_id: cfg.session_id,
                route: cfg.route,
                local_node_id: cfg.local_node_id,
                cfg,
                processors,
                manifest: None,
                plan: None,
                complete_blocks: BTreeSet::new(),
                payload_frames_received: 0,
                payload_bytes_received: 0,
            },
            mode: None,
            lifecycle: ReceiverLifecycle::Active,
        }
    }

    /// Execute the receiver loop until the object is complete.
    async fn run(
        &mut self,
        rx: &mut mpsc::Receiver<InboundFrame>,
        runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
    ) {
        info!(
            session_id = self.shared.session_id,
            "Lossless receiver started"
        );

        loop {
            let frame = if self.is_passive_complete() {
                let passive_timeout = timing::session_finish_timeout_for(
                    tokio::time::Duration::from_millis(self.shared.cfg.peer_report_timeout_ms),
                );
                tokio::select! {
                    maybe_frame = rx.recv() => maybe_frame,
                    _ = tokio::time::sleep(passive_timeout) => {
                        self.finish_session("session_finish_timeout");
                        break;
                    }
                }
            } else {
                rx.recv().await
            };

            let Some(frame) = frame else {
                self.finish_session("receiver_channel_closed");
                break;
            };

            if lossless_session::decode_control(&frame.bytes).is_some() {
                self.handle_control_frame(frame).await;
            } else if lossless_session::decode_block_data(&frame.bytes).is_some() {
                self.handle_block_data_frame(frame).await;
            } else if lossless_session::decode_block_symbol(&frame.bytes).is_some() {
                self.handle_block_symbol_frame(frame).await;
            }

            if self.reported_complete() {
                self.enter_passive_complete();
            }
        }

        if self.reported_complete() {
            self.shared.log_payload_phase_throughput();
            self.register_completed_replay(runtime_sender).await;
        }

        debug!(
            session_id = self.shared.session_id,
            complete = self.reported_complete(),
            lifecycle = ?self.lifecycle,
            "Lossless receiver finished"
        );
        self.shared.log_payload_summary(self.lifecycle);
    }

    fn reported_complete(&self) -> bool {
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(mode)) => mode.is_complete(),
            Some(ReceiverMode::Fec(mode)) => mode.is_complete(),
            None => false,
        }
    }

    #[cfg(test)]
    fn is_complete(&self) -> bool {
        self.reported_complete()
    }

    fn object_complete(&self) -> bool {
        self.shared.has_all_blocks()
    }

    fn is_passive_complete(&self) -> bool {
        self.lifecycle == ReceiverLifecycle::PassiveComplete
    }

    fn finish_session(&mut self, reason: &'static str) {
        self.lifecycle = ReceiverLifecycle::SessionFinished;
        debug!(
            session_id = self.shared.session_id,
            reason,
            object_complete = self.object_complete(),
            "Lossless receiver entered session-finished state"
        );
    }

    fn enter_passive_complete(&mut self) {
        if self.lifecycle == ReceiverLifecycle::PassiveComplete {
            return;
        }
        self.shared.mark_object_complete();
        self.lifecycle = ReceiverLifecycle::PassiveComplete;
        debug!(
            session_id = self.shared.session_id,
            object_complete = self.object_complete(),
            "Lossless receiver entered passive-complete state"
        );
    }

    /// Handle one inbound control frame.
    async fn handle_control_frame(&mut self, frame: InboundFrame) {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return;
        };

        match control {
            LosslessSessionControl::Manifest { manifest } => {
                info!(
                    session_id = self.shared.session_id,
                    local_node_id = self.shared.local_node_id,
                    total_bytes = manifest.total_bytes,
                    total_blocks = manifest.total_blocks,
                    fec = manifest.mode.is_fec(),
                    "Lossless receiver received manifest control frame"
                );
                self.install_manifest(manifest).await;
            }
            LosslessSessionControl::Ready
            | LosslessSessionControl::Need { .. }
            | LosslessSessionControl::TreeBackpressure { .. } => {}
            LosslessSessionControl::SourceDone { round_id } => {
                if let Some(ReceiverMode::Plain(mode)) = self.mode.as_mut() {
                    mode.handle_source_done(&self.shared, round_id).await;
                }
                if let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() {
                    mode.handle_source_done(&self.shared, round_id).await;
                }
            }
        }
    }

    /// Dispatch one plain data frame when the installed manifest is plain.
    async fn handle_block_data_frame(&mut self, frame: InboundFrame) {
        let Some(ReceiverMode::Plain(mode)) = self.mode.as_mut() else {
            return;
        };
        mode.handle_block_data_frame(&mut self.shared, frame).await;
    }

    /// Dispatch one FEC symbol frame when the installed manifest is FEC.
    async fn handle_block_symbol_frame(&mut self, frame: InboundFrame) {
        let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() else {
            return;
        };
        mode.handle_block_symbol_frame(&mut self.shared, frame)
            .await;
    }

    /// Install the first valid manifest and send READY.
    async fn install_manifest(&mut self, manifest: LosslessSessionManifest) {
        if let Some(existing) = &self.shared.manifest {
            if existing == &manifest {
                self.shared.send_ready().await;
            } else {
                warn!(
                    session_id = self.shared.session_id,
                    installed_total_bytes = existing.total_bytes,
                    installed_block_size = existing.block_size,
                    received_total_bytes = manifest.total_bytes,
                    received_block_size = manifest.block_size,
                    "Lossless receiver ignored conflicting manifest after install"
                );
            }
            return;
        }

        if manifest.mode.is_fec() && !self.shared.cfg.fec_enabled {
            warn!(
                session_id = self.shared.session_id,
                "Lossless receiver rejected FEC manifest because local runtime disabled FEC"
            );
            return;
        }
        if let LosslessSessionMode::Fec(fec) = &manifest.mode
            && !receiver_supports_fec_scheme(fec)
        {
            return;
        }

        let Ok(block_size) = usize::try_from(manifest.block_size) else {
            return;
        };
        let Ok(plan) = BlockPlan::new(manifest.total_bytes, block_size) else {
            return;
        };
        let mode = match &manifest.mode {
            LosslessSessionMode::Plain => ReceiverMode::Plain(PlainReceiver::default()),
            LosslessSessionMode::Fec(fec) => {
                let Some(geometry) = plan.symbol_geometry(fec.symbols_per_block).ok() else {
                    return;
                };
                ReceiverMode::Fec(FecReceiver::new(geometry))
            }
        };

        let Some(object_len) = plan.total_bytes_usize() else {
            warn!(
                session_id = self.shared.session_id,
                manifest_total_bytes = manifest.total_bytes,
                "Lossless receiver rejected manifest that did not fit local address space"
            );
            return;
        };
        if !self.shared.ensure_sink_buffer_len(object_len).await {
            return;
        }
        self.shared.plan = Some(plan);
        self.shared.manifest = Some(manifest);
        self.mode = Some(mode);
        info!(
            session_id = self.shared.session_id,
            local_node_id = self.shared.local_node_id,
            total_bytes = self
                .shared
                .manifest
                .as_ref()
                .map(|manifest| manifest.total_bytes)
                .unwrap_or_default(),
            total_blocks = self
                .shared
                .manifest
                .as_ref()
                .map(|manifest| manifest.total_blocks)
                .unwrap_or_default(),
            fec = self
                .shared
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest.mode.is_fec()),
            "Lossless receiver installed manifest"
        );
        self.shared.send_ready().await;
    }

    async fn register_completed_replay(
        &self,
        runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
    ) {
        let Some(runtime_sender) = runtime_sender else {
            return;
        };
        let Some(replay) = self.completed_replay() else {
            return;
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        if runtime_sender
            .send(LosslessRuntimeMessage::ReceiverCompleted {
                session_id: self.shared.session_id,
                replay,
                ack: ack_tx,
            })
            .await
            .is_ok()
        {
            let _ = ack_rx.await;
        }
    }

    fn completed_replay(&self) -> Option<CompletedReceiverReplay> {
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(mode)) if self.reported_complete() => {
                let round_id = mode.last_source_done_round_id()?;
                Some(CompletedReceiverReplay::Plain {
                    round_id,
                    route: self.shared.route,
                    report: NeedReport::Complete,
                })
            }
            Some(ReceiverMode::Fec(mode)) if self.reported_complete() => {
                let round_id = mode.last_source_done_round_id()?;
                Some(CompletedReceiverReplay::Fec {
                    round_id,
                    route: self.shared.route,
                    report: NeedReport::Complete,
                })
            }
            _ => None,
        }
    }
}

fn receiver_supports_fec_scheme(fec: &LosslessSessionFecMode) -> bool {
    match fec.scheme_kind() {
        Some(FecScheme::RaptorQ) => true,
        Some(FecScheme::Mettle) => true,
        None => false,
    }
}

impl ReceiverShared {
    pub(super) fn observe_payload_frame(
        &mut self,
        kind: &'static str,
        block_id: u64,
        payload_len: usize,
    ) {
        self.payload_frames_received = self.payload_frames_received.saturating_add(1);
        self.payload_bytes_received = self
            .payload_bytes_received
            .saturating_add(payload_len as u64);

        if self.payload_frames_received == 1 {
            info!(
                session_id = self.session_id,
                local_node_id = self.local_node_id,
                kind,
                block_id,
                payload_len,
                "Lossless receiver observed first payload frame"
            );
        } else if self.payload_frames_received.is_multiple_of(256) {
            info!(
                session_id = self.session_id,
                local_node_id = self.local_node_id,
                kind,
                block_id,
                payload_frames_received = self.payload_frames_received,
                payload_bytes_received = self.payload_bytes_received,
                complete_blocks = self.complete_blocks.len(),
                "Lossless receiver payload progress"
            );
        }
    }

    /// Return whether the receiver has completed every planned block.
    fn has_all_blocks(&self) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        self.complete_blocks.len() as u64 == plan.total_blocks()
    }

    /// Record when the first payload unit arrives for this receiver session.
    pub(super) fn mark_first_payload_unit(&self) {
        if let Some(progress) = &self.cfg.progress {
            progress.mark_first_payload_unit();
        }
    }

    /// Record when this receiver first completed the local object.
    fn mark_object_complete(&self) {
        if let Some(progress) = &self.cfg.progress {
            progress.mark_object_complete();
        }
    }

    /// Log payload-phase receiver throughput when first-payload timing is available.
    fn log_payload_phase_throughput(&self) {
        let Some(progress) = &self.cfg.progress else {
            return;
        };
        let Some(first_payload_at) = progress.first_payload_unit_at() else {
            return;
        };
        let Some(object_complete_at) = progress.object_complete_at() else {
            return;
        };
        let Some(manifest) = &self.manifest else {
            return;
        };
        let payload_phase = object_complete_at.saturating_duration_since(first_payload_at);
        if payload_phase.is_zero() {
            return;
        }
        let receiver_mbps =
            manifest.total_bytes as f64 * 8.0 / payload_phase.as_secs_f64() / 1_000_000.0;
        info!(
            session_id = self.session_id,
            local_node_id = self.local_node_id,
            total_bytes = manifest.total_bytes,
            payload_phase_ms = payload_phase.as_millis() as u64,
            receiver_mbps,
            "Lossless receiver payload-phase throughput"
        );
    }

    fn log_payload_summary(&self, lifecycle: ReceiverLifecycle) {
        info!(
            session_id = self.session_id,
            local_node_id = self.local_node_id,
            payload_frames_received = self.payload_frames_received,
            payload_bytes_received = self.payload_bytes_received,
            complete_blocks = self.complete_blocks.len(),
            manifest_installed = self.manifest.is_some(),
            plan_installed = self.plan.is_some(),
            lifecycle = ?lifecycle,
            "Lossless receiver payload summary"
        );
    }

    /// Copy one completed block payload into the optional sink buffer.
    pub(super) async fn write_block(&self, block_id: u64, payload: &[u8]) {
        let Some(plan) = self.plan else {
            return;
        };
        let Some(span) = plan.block_span(block_id) else {
            return;
        };
        let Some(sink) = &self.cfg.sink_buffer else {
            return;
        };

        let mut guard = sink.lock().await;
        let Some(object_len) = plan.total_bytes_usize() else {
            return;
        };
        if guard.len() < object_len {
            guard.resize(object_len, 0);
        }

        let start = usize::try_from(span.offset()).unwrap_or(0);
        let end = start + payload.len().min(span.len());
        if end <= guard.len() {
            guard[start..end].copy_from_slice(&payload[..end - start]);
        }
    }

    /// Ensure the optional sink buffer is large enough for the full object.
    async fn ensure_sink_buffer_len(&self, object_len: usize) -> bool {
        let Some(sink) = &self.cfg.sink_buffer else {
            return true;
        };
        let mut guard = sink.lock().await;
        if guard.len() < object_len {
            let additional = object_len - guard.len();
            if guard.try_reserve_exact(additional).is_err() {
                warn!(
                    session_id = self.session_id,
                    object_len, "Lossless receiver failed to reserve sink buffer for manifest"
                );
                return false;
            }
            guard.resize(object_len, 0);
        }
        true
    }

    /// Send a READY control frame back to the sender.
    async fn send_ready(&self) {
        info!(
            session_id = self.session_id,
            local_node_id = self.local_node_id,
            "Lossless receiver sent Ready"
        );
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Ready,
        )
        .await;
    }

    async fn send_fec_need(&self, round_id: u32, report: &NeedReport) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Need {
                round_id,
                report: report.clone(),
            },
        )
        .await;
    }

    fn plain_need(&self) -> Option<NeedReport> {
        let total_blocks = self.plan?.total_blocks();
        if total_blocks == 0 {
            return Some(NeedReport::Complete);
        }
        if self.complete_blocks.len() as u64 == total_blocks {
            return Some(NeedReport::Complete);
        }

        let mut ranges = Vec::new();
        let mut next_missing = 0u64;
        while next_missing < total_blocks {
            if self.complete_blocks.contains(&next_missing) {
                next_missing += 1;
                continue;
            }

            let start_block_id = next_missing;
            let end_block_id = self
                .complete_blocks
                .range((Excluded(start_block_id), Unbounded))
                .next()
                .copied()
                .unwrap_or(total_blocks);
            ranges.push(MissingBlockRange {
                start_block_id,
                end_block_id,
            });
            next_missing = end_block_id;
        }

        Some(NeedReport::Plain { ranges })
    }

    async fn send_plain_need(&self, round_id: u32, report: &NeedReport) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Need {
                round_id,
                report: report.clone(),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use nextmini_messages::lossless_session::NeedBlock;
    use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

    use super::*;
    use crate::node::NodeIdExt;
    use crate::node::config::LocalConfig;
    use crate::node::packet::Packet;
    use crate::node::processor::ProcessorHandle;
    use crate::node::session::receiver::fec::{FecBlockState, FecReceiver};

    const SOURCE_NODE_ID: usize = 51;
    const RECEIVER_NODE_ID: usize = 52;

    #[tokio::test]
    async fn fec_receiver_replays_cached_need_after_late_symbols_for_same_round() {
        let (mut receiver, mut packet_rx) = fec_test_receiver(
            8,
            BTreeSet::new(),
            BTreeMap::from([(0, BTreeMap::from([(0, vec![1, 2])]))]),
        )
        .await;

        let source_done = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let expected = NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 3,
            }],
        };

        receiver.handle_control_frame(source_done.clone()).await;
        assert_eq!(recv_fec_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());

        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    receiver.shared.session_id,
                    0,
                    1,
                    0,
                    &[3, 4],
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        receiver.handle_control_frame(source_done).await;
        assert_eq!(recv_fec_need(&mut packet_rx).await, expected);
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn block_deficit_requests_missing_source_symbols_first() {
        let shared = ReceiverShared {
            session_id: 7,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 7,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 16,
                total_blocks: 2,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![0, 1],
                    ),
                ),
            }),
            plan: BlockPlan::new(16, 8).ok(),
            complete_blocks: BTreeSet::new(),
        };
        let mut receiver = FecReceiver::new(
            BlockPlan::new(16, 8)
                .ok()
                .and_then(|plan| plan.symbol_geometry(4).ok())
                .expect("valid geometry"),
        );
        receiver.blocks = BTreeMap::from([(
            0,
            FecBlockState {
                symbols: BTreeMap::from([(0, vec![1, 2])]),
            },
        )]);
        receiver.last_source_done_round_id = Some(0);
        receiver.last_round_need = Some(NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 3,
            }],
        });

        assert_eq!(receiver.block_deficit(&shared, 0), 3);
    }

    #[tokio::test]
    async fn fec_status_reports_all_missing_blocks_for_large_transfers() {
        let plan = BlockPlan::new(300, 1).expect("plan");
        let geometry = plan.symbol_geometry(4).expect("geometry");
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 1,
                total_bytes: 300,
                total_blocks: 300,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![0, 1],
                    ),
                ),
            }),
            plan: Some(plan),
            complete_blocks: BTreeSet::new(),
        };
        let receiver = FecReceiver::new(geometry);

        let NeedReport::Fec { blocks } = receiver.need_report(&shared).expect("status") else {
            panic!("expected missing-block status");
        };

        assert_eq!(blocks.len(), 300);
        assert_eq!(blocks.first().map(|b| b.block_id), Some(0));
        assert_eq!(blocks.last().map(|b| b.block_id), Some(299));
    }

    #[tokio::test]
    async fn plain_receiver_only_completes_after_reporting_complete_on_source_done() {
        let receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id: 8,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                cfg: ReceiverConfig {
                    session_id: 8,
                    route: crate::node::session::runtime::TransportRoute {
                        src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                        dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                        src_port: 1,
                        dst_port: 2,
                    },
                    local_node_id: 1,
                    sink_buffer: None,
                    progress: None,
                    peer_report_timeout_ms: 200,
                    fec_enabled: false,
                },
                processors: crate::node::processor::ProcessorHandle::new(Default::default()),
                manifest: Some(LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 16,
                    total_blocks: 2,
                    mode: LosslessSessionMode::Plain,
                }),
                plan: BlockPlan::new(16, 8).ok(),
                complete_blocks: BTreeSet::from([0, 1]),
            },
            mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
            lifecycle: ReceiverLifecycle::Active,
        };

        assert!(!receiver.is_complete());
    }

    #[test]
    fn receiver_supports_mettle_for_small_experimental_geometry() {
        const PAPER_SCALE_METTLE_K: u16 = 2400;

        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(
                PAPER_SCALE_METTLE_K,
                vec![0],
            )
        ));
        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(16, vec![0])
        ));
        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(4, vec![0])
        ));
    }

    #[tokio::test]
    async fn fec_receiver_rejects_malformed_symbol_payload_length() {
        let (mut receiver, _packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;

        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    receiver.shared.session_id,
                    0,
                    0,
                    0,
                    &[1],
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert!(
            receiver
                .mode
                .as_ref()
                .and_then(|mode| match mode {
                    ReceiverMode::Fec(fec) => fec.blocks.get(&0),
                    ReceiverMode::Plain(_) => None,
                })
                .is_none(),
            "malformed FEC symbol payloads must be dropped before insertion"
        );
    }

    #[tokio::test]
    async fn plain_receiver_reports_complete_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0, 1])).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
        assert_eq!(receiver.shared.plain_need(), Some(NeedReport::Complete));
    }

    #[tokio::test]
    async fn plain_receiver_reports_sparse_missing_ranges() {
        let (receiver, _packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;

        assert_eq!(
            receiver.shared.plain_need(),
            Some(NeedReport::Plain {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    },
                    MissingBlockRange {
                        start_block_id: 3,
                        end_block_id: 4,
                    },
                ],
            })
        );
    }

    #[tokio::test]
    async fn plain_receiver_emits_sparse_missing_ranges_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;
        let expected = NeedReport::Plain {
            ranges: vec![
                MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                },
                MissingBlockRange {
                    start_block_id: 3,
                    end_block_id: 4,
                },
            ],
        };

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(recv_plain_need(&mut packet_rx).await, expected);
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_keeps_missing_status_stable_across_repeated_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;

        let source_done = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let expected = NeedReport::Plain {
            ranges: vec![MissingBlockRange {
                start_block_id: 1,
                end_block_id: 2,
            }],
        };

        receiver.handle_control_frame(source_done.clone()).await;
        assert_eq!(recv_plain_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
        assert_eq!(receiver.shared.plain_need(), Some(expected.clone()));

        receiver
            .handle_block_data_frame(InboundFrame {
                bytes: lossless_session::encode_block_data(
                    receiver.shared.session_id,
                    1,
                    b"ijklmnop",
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        receiver.handle_control_frame(source_done).await;
        assert_eq!(recv_plain_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_drops_stale_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;
        assert_eq!(
            recv_plain_need(&mut packet_rx).await,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            }
        );

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale SourceDone must be dropped"
        );
    }

    #[tokio::test]
    async fn fec_receiver_drops_stale_source_done() {
        let (mut receiver, mut packet_rx) = fec_test_receiver(
            8,
            BTreeSet::new(),
            BTreeMap::from([(0, BTreeMap::from([(0, vec![1, 2])]))]),
        )
        .await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;
        assert_eq!(
            recv_fec_need(&mut packet_rx).await,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 3,
                }],
            }
        );

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale SourceDone must be dropped"
        );
    }

    #[tokio::test]
    async fn plain_receiver_ignores_duplicate_data_before_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;
        let frame = InboundFrame {
            bytes: lossless_session::encode_block_data(receiver.shared.session_id, 0, b"abcdefgh"),
            peer_id: Some(SOURCE_NODE_ID),
        };

        receiver.handle_block_data_frame(frame.clone()).await;
        receiver.handle_block_data_frame(frame).await;

        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "plain receiver should not emit per-block feedback before SourceDone"
        );
        assert!(!receiver.is_complete());
        assert_eq!(
            receiver.shared.plain_need(),
            Some(NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            })
        );
    }

    #[tokio::test]
    async fn plain_receiver_reports_complete_for_zero_byte_object_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(0, BTreeSet::new()).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn fec_receiver_reports_complete_for_zero_byte_object_on_source_done() {
        let (mut receiver, mut packet_rx) =
            fec_test_receiver(0, BTreeSet::new(), BTreeMap::new()).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(recv_fec_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn mark_first_payload_unit_records_progress() {
        let progress = Arc::new(crate::node::session::runtime::ReceiverProgress::default());
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                progress: Some(progress.clone()),
                peer_report_timeout_ms: 200,
                fec_enabled: false,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 8,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
        };

        shared.mark_first_payload_unit();

        assert!(progress.first_payload_unit_at().is_some());
    }

    #[tokio::test]
    async fn mark_object_complete_records_progress() {
        let progress = Arc::new(crate::node::session::runtime::ReceiverProgress::default());
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                progress: Some(progress.clone()),
                peer_report_timeout_ms: 200,
                fec_enabled: false,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 8,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
        };

        shared.mark_object_complete();

        assert!(progress.object_complete_at().is_some());
    }

    #[tokio::test]
    async fn completed_fec_receiver_registers_complete_replay_before_teardown() {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4751,
            dst_port: 5751,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let geometry = BlockPlan::new(8, 8)
            .ok()
            .and_then(|plan| plan.symbol_geometry(4).ok())
            .expect("valid geometry");
        let mut receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id: 9,
                route,
                local_node_id: RECEIVER_NODE_ID,
                cfg: ReceiverConfig {
                    session_id: 9,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    sink_buffer: None,
                    progress: None,
                    peer_report_timeout_ms: 200,
                    fec_enabled: true,
                },
                processors,
                manifest: Some(LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 8,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Fec(
                        nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                            4,
                            vec![0, 1],
                        ),
                    ),
                }),
                plan: BlockPlan::new(8, 8).ok(),
                complete_blocks: BTreeSet::from([0]),
            },
            mode: Some(ReceiverMode::Fec(FecReceiver::new(geometry))),
            lifecycle: ReceiverLifecycle::Active,
        };

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;
        assert_eq!(
            recv_fec_need(&mut packet_rx).await,
            NeedReport::Complete,
            "completed FEC receivers should report complete at the round boundary"
        );
        assert!(receiver.is_complete());

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let register_task = tokio::spawn(async move {
            receiver.register_completed_replay(Some(runtime_tx)).await;
        });

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay registration")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 9);
        assert_eq!(
            replay,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
            }
        );
        ack.send(())
            .expect("replay registration should still await ack");

        register_task
            .await
            .expect("replay registration task should exit cleanly");
    }

    #[tokio::test]
    async fn passive_complete_receiver_defers_replay_registration_until_session_finish() {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4753,
            dst_port: 5753,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let receiver_task = tokio::spawn(run_with_runtime(
            ReceiverConfig {
                session_id: 11,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
            },
            rx,
            processors.clone(),
            Some(runtime_tx),
        ));

        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                11,
                &LosslessSessionControl::Manifest {
                    manifest: LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: 8,
                        total_blocks: 1,
                        mode: LosslessSessionMode::Plain,
                    },
                },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("manifest should reach receiver");

        let _ready = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for Ready")
            .expect("packet capture closed unexpectedly");

        tx.send(InboundFrame {
            bytes: lossless_session::encode_block_data(11, 0, b"abcdefgh"),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("block data should reach receiver");
        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                11,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("first SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(
            timeout(Duration::from_millis(20), runtime_rx.recv())
                .await
                .is_err(),
            "runtime handoff must not start while the passive-complete receiver can still answer later rounds"
        );

        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                11,
                &LosslessSessionControl::SourceDone { round_id: 1 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("second SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(
            timeout(Duration::from_millis(20), runtime_rx.recv())
                .await
                .is_err(),
            "runtime handoff must still wait while the live passive-complete receiver owns future-round replies"
        );

        drop(tx);

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay handoff")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 11);
        assert_eq!(
            replay,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route,
                report: NeedReport::Complete,
            }
        );
        ack.send(()).expect("replay handoff should still await ack");

        receiver_task
            .await
            .expect("receiver task should exit cleanly after handoff");
    }

    #[tokio::test]
    async fn passive_complete_receiver_survives_past_peer_report_timeout_to_answer_later_round() {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4754,
            dst_port: 5754,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let receiver_task = tokio::spawn(run_with_runtime(
            ReceiverConfig {
                session_id: 12,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
            },
            rx,
            processors.clone(),
            Some(runtime_tx),
        ));

        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                12,
                &LosslessSessionControl::Manifest {
                    manifest: LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: 8,
                        total_blocks: 1,
                        mode: LosslessSessionMode::Plain,
                    },
                },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("manifest should reach receiver");

        let _ready = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for Ready")
            .expect("packet capture closed unexpectedly");

        tx.send(InboundFrame {
            bytes: lossless_session::encode_block_data(12, 0, b"abcdefgh"),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("block data should reach receiver");
        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                12,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("first SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        tokio::time::sleep(
            crate::node::session::timing::peer_report_timeout() + Duration::from_millis(25),
        )
        .await;
        assert!(
            matches!(
                runtime_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "receiver should remain live past the sender peer-report timeout budget"
        );

        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                12,
                &LosslessSessionControl::SourceDone { round_id: 1 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("second SourceDone should reach receiver");
        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);

        drop(tx);

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay handoff")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 12);
        assert_eq!(
            replay,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route,
                report: NeedReport::Complete,
            }
        );
        ack.send(()).expect("replay handoff should still await ack");

        receiver_task
            .await
            .expect("receiver task should exit cleanly after handoff");
    }

    async fn plain_test_receiver(
        total_blocks: u64,
        complete_blocks: BTreeSet<u64>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4750,
            dst_port: 5750,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        (
            SessionReceiver {
                shared: ReceiverShared {
                    session_id: 8,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    cfg: ReceiverConfig {
                        session_id: 8,
                        route,
                        local_node_id: RECEIVER_NODE_ID,
                        sink_buffer: None,
                        progress: None,
                        peer_report_timeout_ms: 200,
                        fec_enabled: false,
                    },
                    processors,
                    manifest: Some(LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: total_blocks * 8,
                        total_blocks,
                        mode: LosslessSessionMode::Plain,
                    }),
                    plan: BlockPlan::new(total_blocks * 8, 8).ok(),
                    complete_blocks,
                },
                mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
                lifecycle: ReceiverLifecycle::Active,
            },
            packet_rx,
        )
    }

    async fn fec_test_receiver(
        total_bytes: u64,
        complete_blocks: BTreeSet<u64>,
        blocks: BTreeMap<u64, BTreeMap<u32, Vec<u8>>>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4752,
            dst_port: 5752,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let plan = BlockPlan::new(total_bytes, 8).expect("valid plan");
        let geometry = plan.symbol_geometry(4).expect("valid geometry");

        let mut fec = FecReceiver::new(geometry);
        fec.blocks = blocks
            .into_iter()
            .map(|(block_id, symbols)| (block_id, FecBlockState { symbols }))
            .collect();

        (
            SessionReceiver {
                shared: ReceiverShared {
                    session_id: 10,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    cfg: ReceiverConfig {
                        session_id: 10,
                        route,
                        local_node_id: RECEIVER_NODE_ID,
                        sink_buffer: None,
                        progress: None,
                        peer_report_timeout_ms: 200,
                        fec_enabled: true,
                    },
                    processors,
                    manifest: Some(LosslessSessionManifest {
                        block_size: 8,
                        total_bytes,
                        total_blocks: plan.total_blocks(),
                        mode: LosslessSessionMode::Fec(
                            nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                                4,
                                vec![0, 1],
                            ),
                        ),
                    }),
                    plan: Some(plan),
                    complete_blocks,
                },
                mode: Some(ReceiverMode::Fec(fec)),
                lifecycle: ReceiverLifecycle::Active,
            },
            packet_rx,
        )
    }

    async fn recv_plain_need(packet_rx: &mut mpsc::Receiver<Packet>) -> NeedReport {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for plain need")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("plain need packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("plain need should decode");
        let LosslessSessionControl::Need { report, .. } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        report
    }

    async fn recv_fec_need(packet_rx: &mut mpsc::Receiver<Packet>) -> NeedReport {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for fec need")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("fec need packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("fec need should decode");
        let LosslessSessionControl::Need { report, .. } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        report
    }
}
