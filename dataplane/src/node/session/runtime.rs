//! Background runtime that owns lossless sender and receiver session tasks.

use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::LosslessSessionManifest;

use crate::node::config::LosslessConfig;
use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressContract, ProcessorHandle};
use crate::node::session::api::{Command, InboundFrame, SessionId};
use crate::node::session::plan::BlockPlan;
use crate::node::session::{fec_policy, receiver, sender};
use crate::node::{NodeId, NodeIdExt};

pub use crate::node::session::fec_policy::PreflightError;

/// Settings shared by sender and receiver session tasks.
#[derive(Clone, Debug)]
pub struct CommonConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Peer IP address for the synthetic TCP wrapper.
    pub dest_ip: Ipv4Addr,
    /// Canonical logical block size for this transfer.
    pub block_size: usize,
    /// Local TCP source port used for outbound frames.
    pub src_port: u16,
    /// Remote TCP destination port used for outbound frames.
    pub dst_port: u16,
    /// Optional pacing configuration applied to outbound data.
    pub data_bucket: Option<TokenBucketSpec>,
    /// Local node identifier used to derive the source address.
    pub local_node_id: usize,
    /// Base address for user-space node addressing.
    pub user_space_base_addr: Ipv4Addr,
    /// Netmask paired with `user_space_base_addr`.
    pub local_netmask: Ipv4Addr,
}

/// User-facing request used to start a sender session.
#[derive(Clone, Debug)]
pub struct SenderRequest {
    /// Shared per-session transport settings.
    pub common: CommonConfig,
    /// Receiver node IDs expected to acknowledge each block.
    pub receiver_ids: Vec<usize>,
    /// Total logical object length in bytes.
    pub total_bytes: u64,
    /// Source bytes or repeating template used to build payload blocks.
    pub source_buffer: Bytes,
    /// Maximum time to wait for READY frames before opening the data gate.
    pub ready_grace_ms: u64,
}

/// User-facing request used to start a receiver session.
#[derive(Clone, Debug)]
pub struct ReceiverRequest {
    /// Shared per-session transport settings.
    pub common: CommonConfig,
    /// Sender node ID expected to originate the transfer.
    pub source_node_id: usize,
    /// Expected number of payload bytes for the completed object.
    pub expected_bytes: u64,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
}

/// Fully derived sender configuration passed to the sender task.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Shared per-session transport settings.
    pub common: CommonConfig,
    /// Receiver node IDs expected to acknowledge each block.
    pub receiver_ids: Vec<usize>,
    /// Total logical object length in bytes.
    pub total_bytes: u64,
    /// Source bytes or repeating template used to build payload blocks.
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
    /// Shared per-session transport settings.
    pub common: CommonConfig,
    /// Sender node ID expected to originate the transfer.
    pub source_node_id: usize,
    /// Expected number of payload bytes for the completed object.
    pub expected_bytes: u64,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Whether FEC manifests are accepted by this runtime.
    pub fec_enabled: bool,
}

/// Handle for interacting with the background lossless runtime actor.
#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl LosslessRuntimeHandle {
    /// Spawn a new runtime actor bound to the provided processor handle.
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let runtime = LosslessRuntime::new(processors, config, command_rx);

        tokio::spawn(async move {
            let mut runtime = runtime;
            runtime.run().await;
        });

        Self { command_tx }
    }

    /// Start a sender task after deriving and validating its manifest.
    pub async fn start_sender(&self, cfg: SenderRequest) -> Result<SessionId, PreflightError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .command_tx
            .send(Command::StartSender {
                cfg,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(PreflightError::RuntimeChannelClosed);
        }

        reply_rx
            .await
            .unwrap_or(Err(PreflightError::RuntimeChannelClosed))
    }

    /// Start a receiver task for a precomputed session identifier.
    pub async fn start_receiver(&self, cfg: ReceiverRequest) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::StartReceiver {
            cfg,
            reply: reply_tx,
        });
        reply_rx.await.expect("The session ID.")
    }

    /// Abort a running session task and drop its ingress channel.
    pub fn stop(&self, session: SessionId) {
        let _ = self.command_tx.send(Command::Stop { session });
    }

    /// Deliver one already-decoded frame to a running session task.
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self.command_tx.send(Command::Deliver { session, frame });
    }

    /// Wait for a session task to finish.
    ///
    /// Returns `false` if the task was not running when the request was issued.
    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::Wait {
            session,
            reply: reply_tx,
        });
        reply_rx.await.unwrap_or(false)
    }

    /// Update the topology-ready gate shared by newly spawned senders.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.command_tx.send(Command::SetTopologyReady { ready });
    }
}

/// Background actor that owns live sender and receiver tasks.
struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    topology_ready_tx: watch::Sender<bool>,
    topology_ready: bool,
    command_rx: mpsc::UnboundedReceiver<Command>,
}

impl LosslessRuntime {
    /// Build a new runtime actor with an initially closed topology gate.
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);

        Self {
            processors,
            config,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            topology_ready_tx,
            topology_ready: false,
            command_rx,
        }
    }

    /// Main command loop for the runtime actor.
    async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                Command::StartSender { cfg, reply } => {
                    let sid = self.spawn_sender(cfg);
                    let _ = reply.send(sid);
                }
                Command::StartReceiver { cfg, reply } => {
                    let sid = self.spawn_receiver(cfg);
                    let _ = reply.send(sid);
                }
                Command::Stop { session } => {
                    self.stop(session).await;
                }
                Command::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                Command::Wait { session, reply } => {
                    self.handle_wait(session, reply);
                }
                Command::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Forward one inbound frame to the matching session task.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(tx) = self.input_sender(session) {
            if tx.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Lossless runtime: receiver dropped inbound frame."
                );
            }
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: no receiver for inbound frame."
            );
        }
    }

    /// Detach the task handle so completion can be awaited without blocking the actor.
    fn handle_wait(&mut self, session: SessionId, reply: oneshot::Sender<bool>) {
        let handle = self.take_task(session);

        tokio::spawn(async move {
            if let Some(handle) = handle {
                let _ = handle.await;
                let _ = reply.send(true);
            } else {
                let _ = reply.send(false);
            }
        });
    }

    /// Remove and return the task handle for a live session, if any.
    fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    /// Derive sender state, allocate an ingress channel, and spawn the sender task.
    fn spawn_sender(&mut self, req: SenderRequest) -> Result<SessionId, PreflightError> {
        let sid = req.common.session_id;
        let block_size = fec_policy::validate_block_size(req.common.block_size)?;
        let plan = BlockPlan::new(req.total_bytes, req.common.block_size).map_err(|_| {
            PreflightError::InvalidBlockSize {
                value: req.common.block_size,
            }
        })?;
        let policy = fec_policy::derive_sender_policy(&self.config)?;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: req.total_bytes,
            total_blocks: plan.total_blocks(),
            mode: policy.mode,
        };
        manifest
            .validate()
            .expect("runtime-derived manifest must validate");
        self.validate_sender_ingress_contract(&req.common, &manifest)?;

        let mut cfg = SenderConfig {
            common: req.common,
            receiver_ids: req.receiver_ids,
            total_bytes: req.total_bytes,
            source_buffer: req.source_buffer,
            manifest,
            ready_grace_ms: req.ready_grace_ms,
            topology_ready: None,
        };
        if !self.topology_ready {
            cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        }

        let processors = self.processors.clone();
        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let sender_handle = tokio::spawn(sender::run(cfg, rx, processors));
        self.tasks.insert(sid, sender_handle);

        Ok(sid)
    }

    /// Reject multi-tree FEC sessions unless processor ingress exposes
    /// tree-specific non-blocking backpressure for this exact path.
    fn validate_sender_ingress_contract(
        &self,
        common: &CommonConfig,
        manifest: &LosslessSessionManifest,
    ) -> Result<(), PreflightError> {
        let nextmini_messages::lossless_session::LosslessSessionMode::Fec(fec) = &manifest.mode
        else {
            return Ok(());
        };
        if fec.tree_ids.len() <= 1 {
            return Ok(());
        }

        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let probe = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            src_ip,
            common.src_port,
            common.dest_ip,
            common.dst_port,
            Some(LosslessTransportMeta {
                session_id: common.session_id,
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
    fn spawn_receiver(&mut self, req: ReceiverRequest) -> SessionId {
        let sid = req.common.session_id;
        let cfg = ReceiverConfig {
            common: req.common,
            source_node_id: req.source_node_id,
            expected_bytes: req.expected_bytes,
            sink_buffer: req.sink_buffer,
            fec_enabled: self.config.fec_enabled,
        };
        let processors = self.processors.clone();

        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let receiver_handle = tokio::spawn(receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, receiver_handle);

        sid
    }

    /// Abort and remove a running session task.
    async fn stop(&mut self, sid: SessionId) {
        if let Some(handle) = self.tasks.remove(&sid) {
            handle.abort();
        }
        self.inputs.remove(&sid);
    }

    /// Return a clone of the ingress sender for `sid`, if the task is live.
    fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    /// Publish the current topology-ready state to newly waiting senders.
    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        let _ = self.topology_ready_tx.send(ready);
    }
}
