//! Sender task for block-first lossless sessions.
//!
//! Plain mode sends complete blocks on the default tree and converges with
//! end-of-round status feedback. FEC mode sends source symbols first, emits
//! `SourceDone { round_id }` after the source sweep, and only then responds to
//! aggregate round status feedback with extra fountain symbols.

mod block_symbol_frame;
mod fec;
mod plain;
mod state;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode, NeedReport,
};

use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::session::api::{InboundFrame, SessionOutcome};
use crate::node::session::control;
use crate::node::session::plan::{BlockPlan, BlockSpan, SymbolGeometry};
use crate::node::session::runtime::{SenderConfig, SessionConfig, TransportRoute};
use crate::node::session::timing;

use self::fec::FecSender;
use self::plain::PlainSender;
use self::state::{ActiveSessionQuorum, QuorumLiveness};

const MANIFEST_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const IDLE_WAIT: Duration = Duration::from_millis(10);

/// Run one sender session until completion or channel shutdown.
pub async fn run(
    cfg: SenderConfig,
    mut ctrl_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) -> SessionOutcome {
    let mut sender = match SessionSender::new(cfg, processors) {
        Ok(sender) => sender,
        Err(reason) => {
            warn!(reason, "Lossless sender aborted before start");
            return SessionOutcome::Aborted;
        }
    };

    sender.run(&mut ctrl_rx).await
}

/// Mode-specific sender hooks invoked by the shared control path.
pub(super) trait ModeHooks {
    /// Observe end-of-round Need feedback from a receiver.
    fn on_need(
        &mut self,
        _shared: &mut SenderShared,
        _peer_id: usize,
        _round_id: u32,
        _report: NeedReport,
    ) {
    }

    /// Return the frozen-quorum peers that still owe feedback for the current round.
    fn pending_feedback_peers(&self, shared: &SenderShared) -> Vec<usize> {
        shared
            .active_quorum
            .active_members()
            .iter()
            .copied()
            .collect()
    }
}

/// Shared sender shell that owns session-level transport state.
struct SessionSender {
    shared: SenderShared,
    mode: SenderMode,
}

/// Sender state that is truly common across plain and FEC modes.
pub(super) struct SenderShared {
    pub(super) session: SessionConfig,
    pub(super) route: TransportRoute,
    pub(super) processors: ProcessorHandle,
    pub(super) manifest: LosslessSessionManifest,
    pub(super) receiver_ids: Vec<usize>,
    pub(in crate::node::session::sender) active_quorum: ActiveSessionQuorum,
    pub(in crate::node::session::sender) quorum_liveness: QuorumLiveness,
    pub(super) plan: BlockPlan,
    pub(super) source: BlockSource,
    pub(super) ready_grace: Duration,
    pub(super) topology_ready: Option<watch::Receiver<bool>>,
    pub(super) pacer: Option<TokenBucket>,
    pub(super) payload_emitted: bool,
}

/// Concrete sender mode selected from the manifest.
enum SenderMode {
    Plain(PlainSender),
    Fec(FecSender),
}

/// Source object wrapper used to derive block payloads and source symbols.
#[derive(Clone)]
pub(super) struct BlockSource {
    bytes: Bytes,
}

impl BlockSource {
    /// Wrap the transfer bytes used by the sender.
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Materialize one logical block payload for the requested span.
    fn block_payload(&self, span: BlockSpan) -> Vec<u8> {
        let len = span.len();
        let Some(offset) = usize::try_from(span.offset()).ok() else {
            return vec![0u8; len];
        };
        if offset + len <= self.bytes.len() {
            return self.bytes.slice(offset..offset + len).to_vec();
        }

        let mut out = vec![0u8; len];
        if offset < self.bytes.len() {
            let available = len.min(self.bytes.len() - offset);
            out[..available].copy_from_slice(&self.bytes[offset..offset + available]);
        }
        out
    }

    /// Materialize one padded block image sized for fixed-width source symbols.
    fn padded_symbol_bytes(&self, span: BlockSpan, geometry: SymbolGeometry) -> Bytes {
        let block = self.block_payload(span);
        let total_symbol_bytes = geometry.source_symbols() * geometry.symbol_size();
        let mut padded = vec![0u8; total_symbol_bytes];
        let copy_len = block.len().min(total_symbol_bytes);
        padded[..copy_len].copy_from_slice(&block[..copy_len]);
        Bytes::from(padded)
    }

    /// Partition one logical block into reusable fixed-size source symbols.
    fn source_symbols(&self, span: BlockSpan, geometry: SymbolGeometry) -> Vec<Bytes> {
        let padded = self.padded_symbol_bytes(span, geometry);
        let symbol_size = geometry.symbol_size();
        (0..geometry.source_symbols())
            .map(|idx| {
                let start = idx * symbol_size;
                padded.slice(start..start + symbol_size)
            })
            .collect()
    }
}

impl SessionSender {
    /// Build sender state from the validated runtime configuration.
    fn new(cfg: SenderConfig, processors: ProcessorHandle) -> Result<Self, &'static str> {
        let manifest = cfg.manifest;
        let block_size =
            usize::try_from(manifest.block_size).map_err(|_| "invalid block size in manifest")?;
        let plan = BlockPlan::new(manifest.total_bytes, block_size)
            .map_err(|_| "invalid block plan for sender")?;
        let pacer = cfg.pacing.map(TokenBucket::new);
        let ready_grace = Duration::from_millis(cfg.ready_grace_ms);
        let source = BlockSource::new(cfg.source_buffer);
        let active_quorum = ActiveSessionQuorum::new(cfg.receiver_ids.iter().copied());
        let quorum_liveness = QuorumLiveness::new(
            timing::quorum_solicitation_interval(),
            Duration::from_millis(cfg.peer_report_timeout_ms),
        );
        let mode = match &manifest.mode {
            LosslessSessionMode::Plain => SenderMode::Plain(PlainSender::default()),
            LosslessSessionMode::Fec(_) => SenderMode::Fec(FecSender::new(&manifest, plan)?),
        };

        Ok(Self {
            shared: SenderShared {
                session: cfg.session,
                route: cfg.route,
                processors,
                manifest,
                receiver_ids: cfg.receiver_ids,
                active_quorum,
                quorum_liveness,
                plan,
                source,
                ready_grace,
                topology_ready: cfg.topology_ready,
                pacer,
                payload_emitted: false,
            },
            mode,
        })
    }

    /// Execute the sender state machine for the negotiated transfer mode.
    async fn run(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) -> SessionOutcome {
        info!(
            session_id = self.shared.session.session_id,
            total_bytes = self.shared.manifest.total_bytes,
            total_blocks = self.shared.manifest.total_blocks,
            receivers = self.shared.receiver_ids.len(),
            fec = self.shared.manifest.mode.is_fec(),
            "Lossless sender started"
        );

        self.shared.wait_topology_ready().await;
        let ready = match &mut self.mode {
            SenderMode::Plain(mode) => self.shared.negotiate_ready(ctrl_rx, mode).await,
            SenderMode::Fec(mode) => self.shared.negotiate_ready(ctrl_rx, mode).await,
        };
        if !ready {
            return SessionOutcome::Aborted;
        }

        let outcome = match &mut self.mode {
            SenderMode::Plain(mode) => mode.run(&mut self.shared, ctrl_rx).await,
            SenderMode::Fec(mode) => mode.run(&mut self.shared, ctrl_rx).await,
        };
        info!(
            session_id = self.shared.session.session_id,
            complete = outcome == SessionOutcome::Completed,
            "Lossless sender finished"
        );
        outcome
    }
}

impl SenderShared {
    /// Wait for the runtime-level topology gate before beginning the handshake.
    async fn wait_topology_ready(&mut self) {
        let Some(rx) = self.topology_ready.as_mut() else {
            return;
        };
        if *rx.borrow() {
            return;
        }

        debug!(
            session_id = self.session.session_id,
            "Lossless sender waiting for topology readiness"
        );

        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                debug!(
                    session_id = self.session.session_id,
                    "Lossless sender observed topology readiness"
                );
                break;
            }
        }
    }

    /// Repeatedly advertise the manifest until the READY gate opens.
    async fn negotiate_ready<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) -> bool {
        if self.active_quorum.configured_len() == 0 {
            self.freeze_active_quorum();
            return true;
        }

        let deadline = Instant::now() + self.ready_grace;
        let mut next_manifest_at = Instant::now();

        while self.active_quorum.active_members().len() < self.active_quorum.configured_len() {
            let now = Instant::now();
            if now >= next_manifest_at {
                self.send_manifest().await;
                next_manifest_at = now + MANIFEST_RETRY_INTERVAL;
            }
            if now >= deadline {
                break;
            }

            let wake_at = next_manifest_at.min(deadline);
            tokio::select! {
                maybe_frame = ctrl_rx.recv() => {
                    let Some(frame) = maybe_frame else {
                        return false;
                    };
                    self.handle_control(frame, mode);
                }
                _ = tokio::time::sleep_until(wake_at) => {}
            }
        }

        if self.active_quorum.active_members().len() < self.active_quorum.configured_len() {
            let missing = self
                .active_quorum
                .configured_members()
                .difference(self.active_quorum.active_members())
                .copied()
                .collect::<Vec<_>>();
            warn!(
                session_id = self.session.session_id,
                ?missing,
                "Lossless sender opening data gate before all receivers sent Ready"
            );
        }

        self.freeze_active_quorum();
        true
    }

    /// Wait for quorum feedback while periodically soliciting and timing out
    /// silent frozen peers.
    pub(super) async fn wait_for_quorum_feedback<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
        round_id: u32,
    ) -> QuorumWaitOutcome {
        let Some(timeout_at) = self.quorum_liveness.timeout_at() else {
            warn!(
                session_id = self.session.session_id,
                round_id,
                reason = "quorum_feedback_wait_not_started",
                "Lossless sender aborted because quorum feedback wait had no active timeout budget"
            );
            return QuorumWaitOutcome::Closed;
        };

        let wake_at = self
            .quorum_liveness
            .next_solicitation_at()
            .unwrap_or(timeout_at)
            .min(timeout_at);

        tokio::select! {
            maybe_frame = ctrl_rx.recv() => {
                let Some(frame) = maybe_frame else {
                    warn!(
                        session_id = self.session.session_id,
                        round_id,
                        reason = "control_channel_closed",
                        "Lossless sender aborted while waiting for quorum feedback because the control channel closed"
                    );
                    return QuorumWaitOutcome::Closed;
                };
                self.handle_control(frame, mode);
                QuorumWaitOutcome::Control
            }
            _ = tokio::time::sleep_until(wake_at) => {
                let now = Instant::now();
                if self.quorum_liveness.timed_out(now) {
                    warn!(
                        session_id = self.session.session_id,
                        reason = "peer_report_timeout",
                        missing = ?mode.pending_feedback_peers(self),
                        solicitation_count = self.quorum_liveness.solicitation_count(),
                        "Lossless sender timed out waiting for frozen quorum feedback"
                    );
                    return QuorumWaitOutcome::TimedOut;
                }

                if self.quorum_liveness.should_solicit(now) {
                    debug!(
                        session_id = self.session.session_id,
                        round_id,
                        solicitation_count = self.quorum_liveness.solicitation_count() + 1,
                        "Lossless sender is retransmitting SourceDone while waiting for quorum feedback"
                    );
                    self.send_source_done(round_id).await;
                    self.quorum_liveness.note_solicitation(now);
                    return QuorumWaitOutcome::Solicited;
                }

                QuorumWaitOutcome::Control
            }
        }
    }

    /// Drain any queued control frames without blocking the send loop.
    pub(super) fn drain_controls<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) {
        while let Ok(frame) = ctrl_rx.try_recv() {
            self.handle_control(frame, mode);
        }
    }

    /// Wait for either new control input or a short idle retry interval.
    pub(super) async fn wait_for_signal<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) -> bool {
        tokio::select! {
            maybe_frame = ctrl_rx.recv() => {
                let Some(frame) = maybe_frame else {
                    warn!(
                        session_id = self.session.session_id,
                        reason = "control_channel_closed",
                        "Lossless sender aborted while waiting for more control because the control channel closed"
                    );
                    return false;
                };
                self.handle_control(frame, mode);
                true
            }
            _ = tokio::time::sleep(IDLE_WAIT) => true,
        }
    }

    /// Freeze the active quorum when the data gate opens.
    pub(super) fn freeze_active_quorum(&mut self) {
        if self.active_quorum.is_frozen() {
            return;
        }

        self.active_quorum.freeze();
        info!(
            session_id = self.session.session_id,
            quorum = ?self.active_quorum.active_members(),
            "Lossless sender froze the active session quorum"
        );
    }

    /// Mark that a payload frame has actually left the sender.
    pub(super) fn mark_payload_emitted(&mut self) {
        self.payload_emitted = true;
    }

    /// Begin the fixed-interval solicitation window for the frozen quorum.
    pub(super) fn start_quorum_feedback_wait(&mut self) {
        self.quorum_liveness.start(Instant::now());
    }

    /// Clear solicitation and timeout state after the current round closes.
    pub(in crate::node::session::sender) fn clear_quorum_feedback_wait(&mut self) {
        self.quorum_liveness.clear();
    }

    /// Return whether the frozen quorum has no active members.
    pub(super) fn active_quorum_is_empty(&self) -> bool {
        self.active_quorum.active_members().is_empty()
    }

    /// Apply one inbound control frame to the sender state machine.
    fn handle_control<M: ModeHooks>(&mut self, frame: InboundFrame, mode: &mut M) {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return;
        };

        match control {
            LosslessSessionControl::Manifest { .. } | LosslessSessionControl::SourceDone { .. } => {
            }
            LosslessSessionControl::Ready => {
                let Some(peer_id) = frame.peer_id else {
                    warn!(
                        session_id = self.session.session_id,
                        "Lossless sender dropped Ready without transport peer_id"
                    );
                    return;
                };
                if self.active_quorum.is_frozen() {
                    warn!(
                        session_id = self.session.session_id,
                        peer_id, "Lossless sender ignored late Ready after quorum freeze"
                    );
                    return;
                }
                self.active_quorum.record_ready(peer_id);
            }
            LosslessSessionControl::Need { round_id, report } => {
                let Some(peer_id) = frame.peer_id else {
                    warn!(
                        session_id = self.session.session_id,
                        "Lossless sender dropped Need without transport peer_id"
                    );
                    return;
                };
                if !self.active_quorum.active_members().contains(&peer_id) {
                    warn!(
                        session_id = self.session.session_id,
                        peer_id, "Lossless sender ignored Need from non-quorum peer"
                    );
                    return;
                }
                if let Err(err) = self
                    .manifest
                    .validate_control(&LosslessSessionControl::Need {
                        round_id,
                        report: report.clone(),
                    })
                {
                    warn!(
                        session_id = self.session.session_id,
                        ?err,
                        peer_id,
                        round_id,
                        "Lossless sender dropped invalid Need for the current manifest"
                    );
                    return;
                }
                mode.on_need(self, peer_id, round_id, report);
            }
        }
    }

    /// Send the negotiated manifest to every receiver.
    async fn send_manifest(&mut self) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Manifest {
                manifest: self.manifest.clone(),
            },
        )
        .await;
    }

    /// Emit the burst-boundary marker for the current sender round.
    pub(super) async fn send_source_done(&mut self, round_id: u32) {
        debug!(
            session_id = self.session.session_id,
            round_id, "Lossless sender emitted SourceDone"
        );
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::SourceDone { round_id },
        )
        .await;
    }

    /// Apply optional pacing before sending `bytes` bytes of payload.
    pub(super) async fn pace(&mut self, bytes: usize) {
        if let Some(bucket) = self.pacer.as_mut() {
            bucket.wait_for_bytes(bytes).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QuorumWaitOutcome {
    Control,
    Solicited,
    TimedOut,
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::time::Duration;

    use crate::node::config::LocalConfig;

    #[test]
    #[ignore = "T1 red test scaffold; enable when round state machine lands"]
    fn red_local_exhaustion_alone_does_not_close_the_round() {
        panic!("pending rewrite invariant: local exhaustion alone cannot close a round");
    }

    #[tokio::test]
    async fn ready_with_transport_peer_identity_joins_active_quorum() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            },
            &mut NoopMode,
        );

        assert_eq!(
            shared
                .active_quorum
                .active_members()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![22]
        );
    }

    #[tokio::test]
    async fn ready_without_transport_peer_identity_is_ignored() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: None,
            },
            &mut NoopMode,
        );

        assert!(shared.active_quorum.active_members().is_empty());
    }

    #[tokio::test]
    async fn late_ready_after_quorum_freeze_does_not_join_completion_quorum() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            },
            &mut NoopMode,
        );
        shared.freeze_active_quorum();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(23),
            },
            &mut NoopMode,
        );

        assert_eq!(
            shared
                .active_quorum
                .active_members()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![22]
        );
    }

    #[tokio::test]
    async fn ready_grace_freezes_quorum_before_pre_payload_control_drain() {
        let mut shared = test_sender_shared();
        let (ctrl_tx, mut ctrl_rx) = mpsc::channel(8);

        ctrl_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            })
            .await
            .expect("ready should enqueue");

        assert!(shared.negotiate_ready(&mut ctrl_rx, &mut NoopMode).await);
        assert!(shared.active_quorum.is_frozen());

        ctrl_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(23),
            })
            .await
            .expect("late ready should enqueue");
        shared.drain_controls(&mut ctrl_rx, &mut NoopMode);

        assert_eq!(
            shared
                .active_quorum
                .active_members()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![22]
        );
    }

    #[tokio::test]
    async fn non_quorum_need_is_ignored_after_freeze() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            },
            &mut NoopMode,
        );
        shared.freeze_active_quorum();

        let mut mode = RecordingMode::default();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(
                    7,
                    &LosslessSessionControl::Need {
                        round_id: 0,
                        report: NeedReport::Complete,
                    },
                ),
                peer_id: Some(23),
            },
            &mut mode,
        );

        assert!(mode.needs.is_empty());
    }

    #[tokio::test]
    async fn need_without_transport_peer_identity_is_ignored() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            },
            &mut NoopMode,
        );
        shared.freeze_active_quorum();

        let mut mode = RecordingMode::default();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(
                    7,
                    &LosslessSessionControl::Need {
                        round_id: 0,
                        report: NeedReport::Complete,
                    },
                ),
                peer_id: None,
            },
            &mut mode,
        );

        assert!(mode.needs.is_empty());
    }

    #[tokio::test]
    async fn quorum_feedback_times_out_for_silent_frozen_peer() {
        let mut shared = test_sender_shared();
        shared.handle_control(
            InboundFrame {
                bytes: lossless_session::encode_control(7, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            },
            &mut NoopMode,
        );
        shared.freeze_active_quorum();
        shared.quorum_liveness =
            QuorumLiveness::new(Duration::from_millis(50), Duration::from_millis(1));
        shared.start_quorum_feedback_wait();

        let (_ctrl_tx, mut ctrl_rx) = mpsc::channel(1);
        let outcome = shared
            .wait_for_quorum_feedback(&mut ctrl_rx, &mut NoopMode, 0)
            .await;

        assert_eq!(outcome, QuorumWaitOutcome::TimedOut);
    }

    #[tokio::test]
    async fn ready_grace_completion_only_waits_for_active_quorum() {
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        let cfg = SenderConfig {
            session: SessionConfig {
                session_id: 9,
                block_size: 4,
            },
            route: TransportRoute {
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1111,
                dst_port: 2222,
            },
            pacing: None,
            receiver_ids: vec![22, 23],
            source_buffer: Bytes::new(),
            manifest: LosslessSessionManifest {
                block_size: 4,
                total_bytes: 0,
                total_blocks: 0,
                mode: LosslessSessionMode::Plain,
            },
            ready_grace_ms: 1,
            peer_report_timeout_ms: 1500,
            topology_ready: None,
        };
        let mut sender = SessionSender::new(cfg, processors).expect("sender should build");
        sender.shared.quorum_liveness =
            QuorumLiveness::new(Duration::from_millis(50), Duration::from_millis(20));

        let (ctrl_tx, mut ctrl_rx) = mpsc::channel(8);
        let sender_task = tokio::spawn(async move { sender.run(&mut ctrl_rx).await });

        ctrl_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(9, &LosslessSessionControl::Ready),
                peer_id: Some(22),
            })
            .await
            .expect("ready control should enqueue");
        tokio::time::sleep(Duration::from_millis(10)).await;
        ctrl_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    9,
                    &LosslessSessionControl::Need {
                        round_id: 0,
                        report: NeedReport::Complete,
                    },
                ),
                peer_id: Some(22),
            })
            .await
            .expect("need should enqueue");

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), sender_task)
                .await
                .expect("sender task should finish")
                .expect("sender task should not panic"),
            SessionOutcome::Completed
        );
    }

    #[tokio::test]
    async fn zero_byte_sender_completes_immediately_when_ready_grace_freezes_empty_quorum() {
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        let cfg = SenderConfig {
            session: SessionConfig {
                session_id: 10,
                block_size: 4,
            },
            route: TransportRoute {
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1111,
                dst_port: 2222,
            },
            pacing: None,
            receiver_ids: vec![22],
            source_buffer: Bytes::new(),
            manifest: LosslessSessionManifest {
                block_size: 4,
                total_bytes: 0,
                total_blocks: 0,
                mode: LosslessSessionMode::Plain,
            },
            ready_grace_ms: 1,
            peer_report_timeout_ms: 1500,
            topology_ready: None,
        };
        let sender = SessionSender::new(cfg, processors).expect("sender should build");
        let (_ctrl_tx, mut ctrl_rx) = mpsc::channel(8);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), async move {
                let mut sender = sender;
                sender.run(&mut ctrl_rx).await
            })
            .await
            .expect("sender task should finish"),
            SessionOutcome::Completed
        );
    }

    #[test]
    fn block_source_zero_fills_when_buffer_is_short() {
        let source = BlockSource::new(Bytes::from_static(b"ab"));
        let plan = BlockPlan::new(8, 4).expect("valid plan");
        let payload = source.block_payload(plan.block_span(1).expect("second block"));
        assert_eq!(payload, b"\0\0\0\0");
    }

    #[test]
    fn block_source_builds_fixed_size_source_symbols() {
        let source = BlockSource::new(Bytes::from_static(b"abcdef"));
        let plan = BlockPlan::new(6, 6).expect("valid plan");
        let geometry = plan.symbol_geometry(4).expect("valid geometry");
        let symbols = source.source_symbols(plan.block_span(0).expect("first block"), geometry);

        assert_eq!(symbols.len(), 4);
        assert_eq!(symbols[0].as_ref(), b"ab");
        assert_eq!(symbols[1].as_ref(), b"cd");
        assert_eq!(symbols[2].as_ref(), b"ef");
        assert_eq!(symbols[3].as_ref(), b"\0\0");
    }

    fn test_sender_shared() -> SenderShared {
        let manifest = LosslessSessionManifest {
            block_size: 4,
            total_bytes: 4,
            total_blocks: 1,
            mode: LosslessSessionMode::Plain,
        };
        let plan = BlockPlan::new(4, 4).expect("valid plan");
        SenderShared {
            session: SessionConfig {
                session_id: 7,
                block_size: 4,
            },
            route: TransportRoute {
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1111,
                dst_port: 2222,
            },
            processors: ProcessorHandle::new(LocalConfig {
                node_id: 0,
                n_nodes: 1,
                num_packet_processors: 1,
                channel_capacity: 8,
                user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: Ipv4Addr::new(255, 255, 255, 0),
                ..Default::default()
            }),
            manifest,
            receiver_ids: vec![22, 23],
            active_quorum: ActiveSessionQuorum::new([22, 23]),
            quorum_liveness: QuorumLiveness::new(
                Duration::from_millis(10),
                Duration::from_millis(30),
            ),
            plan,
            source: BlockSource::new(Bytes::from_static(b"abcd")),
            ready_grace: Duration::from_millis(1),
            topology_ready: None,
            pacer: None,
            payload_emitted: false,
        }
    }

    struct NoopMode;

    impl ModeHooks for NoopMode {}

    #[derive(Default)]
    struct RecordingMode {
        needs: Vec<(usize, u32, NeedReport)>,
    }

    impl ModeHooks for RecordingMode {
        fn on_need(
            &mut self,
            _shared: &mut SenderShared,
            peer_id: usize,
            round_id: u32,
            report: NeedReport,
        ) {
            self.needs.push((peer_id, round_id, report));
        }
    }
}
