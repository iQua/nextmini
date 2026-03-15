//! Background runtime that owns lossless sender and receiver session tasks.

use std::net::Ipv4Addr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::warn;

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::LosslessSessionManifest;

use crate::node::config::LosslessConfig;
use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressContract, ProcessorHandle};
use crate::node::session::api::{
    InboundFrame, LosslessRuntimeMessage, LosslessSessionHandle, SessionId, SessionOutcome,
    SessionState, StartError,
};
pub use crate::node::session::fec_policy::PreflightError;
use crate::node::session::plan::BlockPlan;
use crate::node::session::{fec_policy, receiver, sender};

/// Settings shared by sender and receiver session tasks.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Canonical logical block size for this transfer.
    pub block_size: usize,
}

/// Precomputed transport envelope used for outbound lossless session frames.
#[derive(Clone, Copy, Debug)]
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
    /// Receiver node IDs expected to acknowledge each block.
    pub receiver_ids: Vec<usize>,
    /// Total logical object length in bytes.
    pub total_bytes: u64,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Maximum time to wait for READY frames before opening the data gate.
    pub ready_grace_ms: u64,
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
    /// Optional progress tracker updated when the first logical block completes.
    pub progress: Option<Arc<ReceiverProgress>>,
}

/// Shared receiver-side progress markers exported to integration harnesses.
#[derive(Debug, Default)]
pub struct ReceiverProgress {
    first_completed_block_at: OnceLock<Instant>,
}

impl ReceiverProgress {
    /// Record when the first logical block completed at the receiver.
    pub fn mark_first_completed_block(&self) {
        let _ = self.first_completed_block_at.set(Instant::now());
    }

    /// Return the timestamp of the first completed block, if any.
    pub fn first_completed_block_at(&self) -> Option<Instant> {
        self.first_completed_block_at.get().copied()
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
    /// Receiver node IDs expected to acknowledge each block.
    pub receiver_ids: Vec<usize>,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Validated manifest emitted during the READY handshake.
    pub manifest: LosslessSessionManifest,
    /// Maximum time to wait for READY frames before opening the data gate.
    pub ready_grace_ms: u64,
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
    /// Optional progress tracker updated when the first logical block completes.
    pub progress: Option<Arc<ReceiverProgress>>,
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
        let inbox = self.sessions.get(&session).map(|entry| entry.inbox.clone());

        if let Some(inbox) = inbox {
            if inbox.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Lossless runtime: session dropped inbound frame."
                );
            }
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: no session for inbound frame."
            );
        }
    }

    fn abort_session(&mut self, session_id: SessionId) {
        if let Some(entry) = self.sessions.remove(&session_id) {
            entry.abort_handle.abort();
            let _ = entry
                .state_sender
                .send(SessionState::Finished(SessionOutcome::Aborted));
        }
    }

    fn finish_session(&mut self, session_id: SessionId, outcome: SessionOutcome) {
        if let Some(entry) = self.sessions.remove(&session_id) {
            let _ = entry.state_sender.send(SessionState::Finished(outcome));
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

        let cfg = ReceiverConfig {
            session_id: req.session_id,
            route: req.route,
            local_node_id: req.local_node_id,
            sink_buffer: req.sink_buffer,
            progress: req.progress,
            fec_enabled: self.config.fec_enabled,
        };
        let processors = self.processors.clone();

        let (inbox, inbox_receiver) = mpsc::channel(1024);
        let (state_sender, state_receiver) = watch::channel(SessionState::Running);

        let task = tokio::spawn(receiver::run(cfg, inbox_receiver, processors));
        let abort_handle = task.abort_handle();
        let message_sender = self.message_sender.clone();
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
}
