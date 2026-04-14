//! Background runtime that owns lossless sender and receiver session tasks.

use std::net::Ipv4Addr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{debug, warn};

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{self, LosslessSessionControl, LosslessSessionManifest};

use crate::node::config::LosslessConfig;
use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressContract, ProcessorHandle};
use crate::node::session::api::{
    CompletedReceiverReplay, InboundFrame, LosslessRuntimeMessage, LosslessSessionHandle,
    SessionId, SessionOutcome, SessionState, StartError,
};
pub use crate::node::session::fec_policy::PreflightError;
use crate::node::session::plan::BlockPlan;
use crate::node::session::{control, fec_policy, receiver, sender};

/// Settings shared by sender and receiver session tasks.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Canonical logical block size for this transfer.
    pub block_size: usize,
}

/// Precomputed transport envelope used for outbound lossless session frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportRoute {
    /// Local source IP address for the synthetic TCP wrapper.
    pub src_ip: Ipv4Addr,
    /// Remote destination IP address for the synthetic TCP wrapper.
    pub dst_ip: Ipv4Addr,
    /// Local TCP source port used for outbound frames.
    pub src_port: u16,
    /// Remote TCP destination port used for outbound frames.
    pub dst_port: u16,
}

/// User-facing request used to start a sender session.
#[derive(Clone, Debug)]
pub struct SenderRequest {
    /// Shared per-session settings.
    pub session: SessionConfig,
    /// Precomputed transport envelope for sender traffic.
    pub route: TransportRoute,
    /// Optional pacing configuration applied to outbound data.
    pub pacing: Option<TokenBucketSpec>,
    /// Receiver node IDs expected to provide lossless feedback.
    pub receiver_ids: Vec<usize>,
    /// Total logical object length in bytes.
    pub total_bytes: u64,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Maximum time to wait for READY frames during session start.
    pub ready_grace_ms: u64,
    /// Maximum time to wait for frozen-quorum feedback after `SourceDone`.
    pub peer_report_timeout_ms: u64,
}

/// User-facing request used to start a receiver session.
#[derive(Clone, Debug)]
pub struct ReceiverRequest {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Precomputed transport envelope for receiver control traffic.
    pub route: TransportRoute,
    /// Local node identifier advertised in READY.
    pub local_node_id: usize,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Optional progress tracker updated when the first payload unit arrives.
    pub progress: Option<Arc<ReceiverProgress>>,
}

/// Shared receiver-side progress markers exported to integration harnesses.
#[derive(Debug, Default)]
pub struct ReceiverProgress {
    first_payload_unit_at: OnceLock<Instant>,
    object_complete_at: OnceLock<Instant>,
}

impl ReceiverProgress {
    /// Record when the first payload unit arrived at the receiver.
    pub fn mark_first_payload_unit(&self) {
        let _ = self.first_payload_unit_at.set(Instant::now());
    }

    /// Return the timestamp of the first payload unit, if any.
    #[allow(dead_code)]
    pub fn first_payload_unit_at(&self) -> Option<Instant> {
        self.first_payload_unit_at.get().copied()
    }

    /// Record when the receiver first reached local object completion.
    pub fn mark_object_complete(&self) {
        let _ = self.object_complete_at.set(Instant::now());
    }

    /// Return the timestamp of local object completion, if any.
    #[allow(dead_code)]
    pub fn object_complete_at(&self) -> Option<Instant> {
        self.object_complete_at.get().copied()
    }
}

/// Fully derived sender configuration passed to the sender task.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Shared per-session settings.
    pub session: SessionConfig,
    /// Precomputed transport envelope for sender traffic.
    pub route: TransportRoute,
    /// Optional pacing configuration applied to outbound data.
    pub pacing: Option<TokenBucketSpec>,
    /// Receiver node IDs expected to provide lossless feedback.
    pub receiver_ids: Vec<usize>,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Validated manifest emitted during the READY handshake.
    pub manifest: LosslessSessionManifest,
    /// Maximum time to wait for READY frames during session start.
    pub ready_grace_ms: u64,
    /// Maximum time to wait for frozen-quorum feedback after `SourceDone`.
    pub peer_report_timeout_ms: u64,
    /// Optional topology-ready gate shared by newly spawned senders.
    pub topology_ready: Option<watch::Receiver<bool>>,
}

/// Fully derived receiver configuration passed to the receiver task.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Precomputed transport envelope for receiver control traffic.
    pub route: TransportRoute,
    /// Local node identifier advertised in READY.
    pub local_node_id: usize,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Optional progress tracker updated when the first payload unit arrives.
    pub progress: Option<Arc<ReceiverProgress>>,
    /// Maximum time to keep a passive-complete receiver alive while waiting for later rounds.
    pub peer_report_timeout_ms: u64,
    /// Whether FEC manifests are accepted by this runtime.
    pub fec_enabled: bool,
}

/// Handle for interacting with the background lossless runtime actor.
#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    message_sender: mpsc::UnboundedSender<LosslessRuntimeMessage>,
}

struct SessionEntry {
    inbox: mpsc::Sender<InboundFrame>,
    state_sender: watch::Sender<SessionState>,
    abort_handle: tokio::task::AbortHandle,
}

impl LosslessRuntimeHandle {
    /// Spawn a new runtime actor bound to the provided processor handle.
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        let runtime =
            LosslessRuntime::new(processors, config, message_sender.clone(), message_receiver);

        tokio::spawn(async move {
            let mut runtime = runtime;
            runtime.run().await;
        });

        Self { message_sender }
    }

    /// Start a sender task after deriving and validating its manifest.
    pub async fn start_sender(
        &self,
        cfg: SenderRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .message_sender
            .send(LosslessRuntimeMessage::StartSender {
                cfg,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(StartError::RuntimeChannelClosed);
        }

        reply_rx
            .await
            .unwrap_or(Err(StartError::RuntimeChannelClosed))
    }

    /// Start a receiver task for a precomputed session identifier.
    pub async fn start_receiver(
        &self,
        cfg: ReceiverRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .message_sender
            .send(LosslessRuntimeMessage::StartReceiver {
                cfg,
                reply: reply_tx,
            });
        reply_rx
            .await
            .unwrap_or(Err(StartError::RuntimeChannelClosed))
    }

    /// Deliver one already-decoded frame to a running session task.
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self
            .message_sender
            .send(LosslessRuntimeMessage::Deliver { session, frame });
    }

    /// Update the topology-ready gate shared by newly spawned senders.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self
            .message_sender
            .send(LosslessRuntimeMessage::SetTopologyReady { ready });
    }
}

/// Background actor that owns live sender and receiver tasks.
struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    sessions: AHashMap<SessionId, SessionEntry>,
    completed_receivers: AHashMap<SessionId, CompletedReceiverReplay>,
    topology_ready_sender: watch::Sender<bool>,
    topology_ready: bool,
    message_sender: mpsc::UnboundedSender<LosslessRuntimeMessage>,
    message_receiver: mpsc::UnboundedReceiver<LosslessRuntimeMessage>,
}

impl LosslessRuntime {
    /// Build a new runtime actor with an initially closed topology gate.
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        message_sender: mpsc::UnboundedSender<LosslessRuntimeMessage>,
        message_receiver: mpsc::UnboundedReceiver<LosslessRuntimeMessage>,
    ) -> Self {
        let (topology_ready_sender, _) = watch::channel(false);

        Self {
            processors,
            config,
            sessions: AHashMap::default(),
            completed_receivers: AHashMap::default(),
            topology_ready_sender,
            topology_ready: false,
            message_sender,
            message_receiver,
        }
    }

    /// Main command loop for the runtime actor.
    async fn run(&mut self) {
        while let Some(message) = self.message_receiver.recv().await {
            match message {
                LosslessRuntimeMessage::StartSender { cfg, reply } => {
                    let _ = reply.send(self.start_sender_session(cfg));
                }
                LosslessRuntimeMessage::StartReceiver { cfg, reply } => {
                    let _ = reply.send(self.start_receiver_session(cfg));
                }
                LosslessRuntimeMessage::Abort { session_id } => {
                    self.abort_session(session_id);
                }
                LosslessRuntimeMessage::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                LosslessRuntimeMessage::ReceiverCompleted {
                    session_id,
                    replay,
                    ack,
                } => {
                    self.completed_receivers.insert(session_id, replay);
                    let _ = ack.send(());
                }
                LosslessRuntimeMessage::SessionExited {
                    session_id,
                    outcome,
                } => {
                    self.finish_session(session_id, outcome);
                }
                LosslessRuntimeMessage::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Forward one inbound frame to the matching session task.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(header) = lossless_session::peek_header(&frame.bytes)
            && header.version != lossless_session::LOSSLESS_SESSION_VERSION
        {
            warn!(
                session_id = session,
                wire_session_id = header.session_id,
                observed_version = header.version,
                expected_version = lossless_session::LOSSLESS_SESSION_VERSION,
                kind = header.kind,
                ctrl_kind = header.ctrl_kind,
                "Lossless runtime: dropping frame with unsupported session version."
            );
            return;
        }

        match self.deliver_live_receiver(session, frame.clone()).await {
            LiveDeliveryOutcome::Delivered => return,
            LiveDeliveryOutcome::Closed => {
                if self.replay_completed_receiver(session, frame.clone()).await {
                    return;
                }
                warn!(
                    session_id = session,
                    "Lossless runtime: session dropped inbound frame."
                );
                return;
            }
            LiveDeliveryOutcome::Missing => {}
        }

        if self.replay_completed_receiver(session, frame.clone()).await {
            return;
        }

        warn!(
            session_id = session,
            "Lossless runtime: no session for inbound frame."
        );
    }

    fn abort_session(&mut self, session_id: SessionId) {
        if let Some(entry) = self.sessions.remove(&session_id) {
            entry.abort_handle.abort();
            let _ = entry
                .state_sender
                .send(SessionState::Finished(SessionOutcome::Aborted));
        }
        self.completed_receivers.remove(&session_id);
    }

    fn finish_session(&mut self, session_id: SessionId, outcome: SessionOutcome) {
        let keep_completed_replay = outcome == SessionOutcome::Completed;
        if let Some(entry) = self.sessions.remove(&session_id) {
            let _ = entry.state_sender.send(SessionState::Finished(outcome));
        }
        if !keep_completed_replay {
            self.completed_receivers.remove(&session_id);
        }
    }

    /// Derive sender state, allocate an ingress channel, and spawn the sender task.
    fn start_sender_session(
        &mut self,
        req: SenderRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let sid = req.session.session_id;
        if self.sessions.contains_key(&sid) {
            return Err(StartError::SessionAlreadyActive { session_id: sid });
        }
        self.completed_receivers.remove(&sid);

        let block_size = fec_policy::validate_block_size(req.session.block_size)?;
        let plan = BlockPlan::new(req.total_bytes, req.session.block_size).map_err(|_| {
            PreflightError::InvalidBlockSize {
                value: req.session.block_size,
            }
        })?;
        let policy = fec_policy::derive_sender_policy(&self.config)?;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: req.total_bytes,
            total_blocks: plan.total_blocks(),
            mode: policy.mode,
        };
        self.validate_sender_ingress_contract(&req.route, &req.session, &manifest)?;

        let mut cfg = SenderConfig {
            session: req.session,
            route: req.route,
            pacing: req.pacing,
            receiver_ids: req.receiver_ids,
            source_buffer: req.source_buffer,
            manifest,
            ready_grace_ms: req.ready_grace_ms,
            peer_report_timeout_ms: req.peer_report_timeout_ms,
            topology_ready: None,
        };
        if !self.topology_ready {
            cfg.topology_ready = Some(self.topology_ready_sender.subscribe());
        }

        let processors = self.processors.clone();
        let (inbox, inbox_receiver) = mpsc::channel(1024);
        let (state_sender, state_receiver) = watch::channel(SessionState::Running);

        let task = tokio::spawn(sender::run(cfg, inbox_receiver, processors));
        let abort_handle = task.abort_handle();
        let message_sender = self.message_sender.clone();
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(outcome) => outcome,
                Err(_) => SessionOutcome::Aborted,
            };
            let _ = message_sender.send(LosslessRuntimeMessage::SessionExited {
                session_id: sid,
                outcome,
            });
        });

        self.sessions.insert(
            sid,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle,
            },
        );

        Ok(LosslessSessionHandle::new(
            sid,
            state_receiver,
            self.message_sender.clone(),
        ))
    }

    /// Reject multi-tree FEC sessions unless processor ingress exposes
    /// tree-specific non-blocking backpressure for this exact path.
    fn validate_sender_ingress_contract(
        &self,
        route: &TransportRoute,
        session: &SessionConfig,
        manifest: &LosslessSessionManifest,
    ) -> Result<(), PreflightError> {
        let nextmini_messages::lossless_session::LosslessSessionMode::Fec(fec) = &manifest.mode
        else {
            return Ok(());
        };
        if fec.tree_ids.len() <= 1 {
            return Ok(());
        }

        let probe = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            route.src_ip,
            route.src_port,
            route.dst_ip,
            route.dst_port,
            Some(LosslessTransportMeta {
                session_id: session.session_id,
                tree_id: Some(fec.tree_ids[0]),
            }),
            b"x",
        );
        if self.processors.lossless_ingress_contract(&probe)
            != LosslessIngressContract::TreeVisibleNonBlocking
        {
            return Err(PreflightError::MultiTreeRequiresTreeVisibleIngress);
        }

        Ok(())
    }

    /// Allocate an ingress channel and spawn the receiver task.
    fn start_receiver_session(
        &mut self,
        req: ReceiverRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let sid = req.session_id;
        if self.sessions.contains_key(&sid) {
            return Err(StartError::SessionAlreadyActive { session_id: sid });
        }
        self.completed_receivers.remove(&sid);

        let cfg = ReceiverConfig {
            session_id: req.session_id,
            route: req.route,
            local_node_id: req.local_node_id,
            sink_buffer: req.sink_buffer,
            progress: req.progress,
            peer_report_timeout_ms: self.config.peer_report_timeout_ms,
            fec_enabled: self.config.fec_enabled,
        };
        let processors = self.processors.clone();

        let (inbox, inbox_receiver) = mpsc::channel(1024);
        let (state_sender, state_receiver) = watch::channel(SessionState::Running);

        let message_sender = self.message_sender.clone();
        let task = tokio::spawn(receiver::run_with_runtime(
            cfg,
            inbox_receiver,
            processors,
            Some(message_sender.clone()),
        ));
        let abort_handle = task.abort_handle();
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(()) => SessionOutcome::Completed,
                Err(_) => SessionOutcome::Aborted,
            };
            let _ = message_sender.send(LosslessRuntimeMessage::SessionExited {
                session_id: sid,
                outcome,
            });
        });

        self.sessions.insert(
            sid,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle,
            },
        );

        Ok(LosslessSessionHandle::new(
            sid,
            state_receiver,
            self.message_sender.clone(),
        ))
    }

    /// Publish the current topology-ready state to newly waiting senders.
    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        let _ = self.topology_ready_sender.send(ready);
    }

    async fn replay_completed_receiver(&self, session: SessionId, frame: InboundFrame) -> bool {
        let Some(replay) = self.completed_receivers.get(&session) else {
            return false;
        };
        let Some((_, LosslessSessionControl::SourceDone { round_id })) =
            lossless_session::decode_control(&frame.bytes)
        else {
            return false;
        };
        match replay {
            CompletedReceiverReplay::Plain {
                round_id: replay_round_id,
                route,
                report,
            } => {
                if round_id < *replay_round_id {
                    debug!(
                        session_id = session,
                        round_id,
                        replay_round_id,
                        "Lossless runtime dropped stale replay attempt for a completed plain receiver"
                    );
                    return false;
                }
                control::send_control(
                    &self.processors,
                    control::FrameRoute {
                        session_id: session,
                        tree_id: None,
                        src_ip: route.src_ip,
                        src_port: route.src_port,
                        dst_ip: route.dst_ip,
                        dst_port: route.dst_port,
                    },
                    &LosslessSessionControl::Need {
                        round_id,
                        report: report.clone(),
                    },
                )
                .await;
                true
            }
            CompletedReceiverReplay::Fec {
                round_id: replay_round_id,
                route,
                report,
            } => {
                if round_id < *replay_round_id {
                    debug!(
                        session_id = session,
                        round_id,
                        replay_round_id,
                        "Lossless runtime dropped stale replay attempt for a completed FEC receiver"
                    );
                    return false;
                }
                control::send_control(
                    &self.processors,
                    control::FrameRoute {
                        session_id: session,
                        tree_id: None,
                        src_ip: route.src_ip,
                        src_port: route.src_port,
                        dst_ip: route.dst_ip,
                        dst_port: route.dst_port,
                    },
                    &LosslessSessionControl::Need {
                        round_id,
                        report: report.clone(),
                    },
                )
                .await;
                true
            }
        }
    }

    async fn deliver_live_receiver(
        &self,
        session: SessionId,
        frame: InboundFrame,
    ) -> LiveDeliveryOutcome {
        let Some(inbox) = self.sessions.get(&session).map(|entry| entry.inbox.clone()) else {
            return LiveDeliveryOutcome::Missing;
        };

        if inbox.send(frame).await.is_ok() {
            return LiveDeliveryOutcome::Delivered;
        }

        LiveDeliveryOutcome::Closed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveDeliveryOutcome {
    Delivered,
    Closed,
    Missing,
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio::time::timeout;

    use nextmini_messages::lossless_session::{self, NeedReport};
    use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

    use super::*;
    use crate::node::NodeIdExt;
    use crate::node::config::LocalConfig;

    const SOURCE_NODE_ID: usize = 61;
    const RECEIVER_NODE_ID: usize = 62;

    #[tokio::test]
    async fn deliver_frame_falls_back_to_completed_plain_receiver_on_source_done() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E401;
        let (inbox, inbox_rx) = mpsc::channel(1);
        drop(inbox_rx);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 0,
                route,
                report: NeedReport::Complete,
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert_plain_complete(&mut packet_rx).await;
    }

    #[tokio::test]
    async fn deliver_frame_replays_fec_complete_for_source_done() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E402;

        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_fec_complete(&mut packet_rx).await;

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_fec_complete(&mut packet_rx).await;
    }

    #[tokio::test]
    async fn deliver_frame_replays_completed_receiver_for_same_or_future_rounds_only() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E40A;

        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route,
                report: NeedReport::Complete,
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale round replay must be dropped"
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 2 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_plain_complete_for_round(&mut packet_rx, 2).await;
    }

    #[tokio::test]
    async fn deliver_frame_prefers_live_plain_inbox_over_completed_replay_during_handoff() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E403;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 0,
                route,
                report: NeedReport::Complete,
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "runtime-owned replay must not preempt a live receiver inbox during handoff"
        );
        let delivered = timeout(Duration::from_secs(2), inbox_rx.recv())
            .await
            .expect("timed out waiting for live plain handoff delivery")
            .expect("live plain inbox should receive the duplicate frame");
        assert!(matches!(
            lossless_session::decode_control(&delivered.bytes),
            Some((_, LosslessSessionControl::SourceDone { round_id: 0 }))
        ));
    }

    #[tokio::test]
    async fn deliver_frame_prefers_live_fec_inbox_over_completed_replay_during_handoff() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E404;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "runtime-owned replay must not preempt a live receiver inbox during handoff"
        );
        let delivered = timeout(Duration::from_secs(2), inbox_rx.recv())
            .await
            .expect("timed out waiting for live FEC handoff delivery")
            .expect("live FEC inbox should receive the duplicate frame");
        assert!(matches!(
            lossless_session::decode_control(&delivered.bytes),
            Some((_, LosslessSessionControl::SourceDone { round_id: 0 }))
        ));
    }

    #[tokio::test]
    async fn deliver_frame_drops_unsupported_version_before_dispatch() {
        let (mut runtime, _packet_rx, _route) = test_runtime().await;
        let session_id = 0xA11C_E405;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                inbox,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );

        let mut bytes =
            lossless_session::encode_control(session_id, &LosslessSessionControl::Ready);
        bytes[4] = lossless_session::LOSSLESS_SESSION_VERSION - 1;

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes,
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), inbox_rx.recv())
                .await
                .is_err(),
            "unsupported-version frames should be dropped before session delivery"
        );
    }

    async fn test_runtime() -> (LosslessRuntime, mpsc::Receiver<Packet>, TransportRoute) {
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

        let route = TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4760,
            dst_port: 5760,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        (
            LosslessRuntime::new(
                processors,
                cfg.lossless_runtime_config.clone(),
                message_sender,
                message_receiver,
            ),
            packet_rx,
            route,
        )
    }

    async fn assert_plain_complete(packet_rx: &mut mpsc::Receiver<Packet>) {
        assert_plain_complete_for_round(packet_rx, 0).await;
    }

    async fn assert_plain_complete_for_round(packet_rx: &mut mpsc::Receiver<Packet>, round_id: u32) {
        let packet = tokio::time::timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for replayed plain status")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("plain status packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("plain status should decode");
        assert_eq!(
            control,
            LosslessSessionControl::Need {
                round_id,
                report: NeedReport::Complete,
            }
        );
    }

    async fn assert_fec_complete(packet_rx: &mut mpsc::Receiver<Packet>) {
        let packet = tokio::time::timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for replayed fec status")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("fec status packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("fec status should decode");
        assert_eq!(
            control,
            LosslessSessionControl::Need {
                round_id: 0,
                report: NeedReport::Complete,
            }
        );
    }
}
