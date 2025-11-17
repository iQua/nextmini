use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use nextmini_messages::TokenBucketSpec;

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{Command, InboundFrame, SessionId};
use crate::node::session::{receiver, sender};

/// Socket addressing and runtime knobs shared by senders and receivers.
#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub dest_ip: Ipv4Addr,
    pub chunk_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub data_bucket: Option<TokenBucketSpec>,
    pub local_node_id: usize,
    pub user_space_base_addr: Ipv4Addr,
    pub local_netmask: Ipv4Addr,
}

/// Sender-only configuration (fan-out, source path, ready grace, etc.).
#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_buffer: Bytes,
    pub ready_grace_ms: u64,
    pub topology_ready: Option<watch::Receiver<bool>>,
}

/// Receiver-only configuration (source node, expected bytes, sink path, etc.).
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
}

/// Handle for communicating with the reliable runtime actor.
/// This handle can be cloned and used to manage reliable sessions.
#[derive(Clone, Debug)]
pub struct ReliableRuntimeHandle {
    tx: mpsc::UnboundedSender<Command>,
}

impl ReliableRuntimeHandle {
    pub fn new(processors: ProcessorHandle) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let runtime = ReliableRuntime::new(processors, rx);

        // spawns the reliable runtime actor task
        tokio::spawn(async move {
            let mut runtime = runtime;
            runtime.run().await;
        });

        Self { tx }
    }

    /// Request that the runtime spin up a sender session with the supplied
    /// configuration and return its session ID.
    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.tx.send(Command::StartSender {
            cfg,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Request that the runtime spin up a receiver immediately.
    #[allow(dead_code)]
    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.tx.send(Command::StartReceiver {
            cfg,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Request that the runtime stage a receiver that will be paired once the
    /// control-plane assigns a session ID.
    pub async fn start_receiver_pending(
        &self,
        cfg: ReceiverConfig,
        key: PendingReceiverKey,
    ) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.tx.send(Command::StartReceiverPending {
            cfg,
            key,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Cancel a session regardless of whether it is a sender or receiver.
    pub fn stop(&self, session: SessionId) {
        let _ = self.tx.send(Command::Stop { session });
    }

    /// Deliver an inbound reliable frame to the owning session's queue.
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self.tx.send(Command::Deliver { session, frame });
    }

    /// Wait until the runtime observes completion (EOT/ACKs) for a session.
    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.tx.send(Command::Wait {
            session,
            reply: reply_tx,
        });
        reply_rx.await.unwrap_or(false)
    }

    /// Reserve the next session identifier from the runtime's allocator.
    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.tx.send(Command::AllocateSession { reply: reply_tx });

        reply_rx.await.expect("The session ID.")
    }

    /// Notify the runtime that the control plane finished setting up the topology.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.tx.send(Command::SetTopologyReady { ready });
    }
}

/// Tracks running reliable sessions along with their inboxes and join handles.
/// This is the actor that processes commands and manages session lifecycle.
struct ReliableRuntime {
    processors: ProcessorHandle,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    pending: AHashMap<PendingReceiverKey, VecDeque<PendingReceiver>>,
    topology_ready_tx: watch::Sender<bool>,
    topology_ready: bool,
    command_rx: mpsc::UnboundedReceiver<Command>,
}

/// Key that allows a receiver to be created speculatively and paired once the
/// control plane decides which session ID to use for a (destination, source) tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingReceiverKey {
    pub dest_ip: Ipv4Addr,
    pub source_node_id: usize,
}

/// Wrapper that stores the receiver config until a session ID is assigned.
struct PendingReceiver {
    cfg: ReceiverConfig,
    reply: oneshot::Sender<SessionId>,
}

impl ReliableRuntime {
    /// Construct a runtime that can spawn sender/receiver tasks and track their lifetimes.
    fn new(processors: ProcessorHandle, command_rx: mpsc::UnboundedReceiver<Command>) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);

        Self {
            processors,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            pending: AHashMap::default(),
            topology_ready_tx,
            topology_ready: false,
            command_rx,
        }
    }

    /// Main event loop for the reliable runtime actor that processes inbound commands.
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
                Command::StartReceiverPending { cfg, key, reply } => {
                    self.enqueue_pending_receiver(key, cfg, reply);
                }
                Command::Stop { session } => {
                    self.stop(session).await;
                }
                Command::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                Command::Wait { session, reply } => {
                    // handles a wait command asynchronously without blocking the main loop
                    self.handle_wait(session, reply);
                }
                Command::AllocateSession { reply } => {
                    let sid = self.allocate_session_id();

                    let _ = reply.send(sid);
                }
                Command::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Deliver inbound frames to sessions.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        let dest_ip = frame.dest_ip;
        let source_node_id = frame.source_node_id;

        let (sender, pending_reply) = if let Some(tx) = self.input_sender(session) {
            (Some(tx), None)
        } else if let (Some(dest_ip), Some(source_node_id)) = (dest_ip, source_node_id) {
            if let Some((cfg, reply)) = self.adopt_pending_receiver(
                PendingReceiverKey {
                    dest_ip,
                    source_node_id,
                },
                session,
            ) {
                let _ = self.spawn_receiver(cfg);
                (self.input_sender(session), Some(reply))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        if let Some(tx) = sender {
            if tx.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Reliable runtime: receiver dropped inbound frame."
                );
            }
            if let Some(reply) = pending_reply {
                let _ = reply.send(session);
            }
        } else {
            warn!(
                session_id = session,
                "Reliable runtime: no receiver for inbound frame."
            );
        }
    }

    /// Handles wait command by spawning a separate task.
    fn handle_wait(&mut self, session: SessionId, reply: oneshot::Sender<bool>) {
        // Take ownership of the task handle. Note: we don't remove inputs here
        // because for pending receivers, the task hasn't been spawned yet and
        // frames may still arrive. Cleanup happens in stop() after wait completes.
        let handle = self.take_task(session);

        tokio::spawn(async move {
            if let Some(handle) = handle {
                let _ = handle.await; // ignore join errors; treat as completion
                let _ = reply.send(true);
            } else {
                let _ = reply.send(false);
            }
        });
    }

    /// Removes and returns the join handle for a session task, if present.
    fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    /// Spawn a sender task, wiring up control-plane readiness watchers and
    /// returning its assigned session ID.
    fn spawn_sender(&mut self, mut cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();

        if self.topology_ready {
            cfg.topology_ready = None;
        } else {
            cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        }

        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let handle = tokio::spawn(sender::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);

        sid
    }

    /// Spawn a receiver task and hand it a bounded inbox for inbound frames.
    fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();

        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);

        let handle = tokio::spawn(receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);

        sid
    }

    /// Abort a running session and drop its inbox, if still active.
    async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) {
            h.abort();
        }
        self.remove_inputs(sid);
    }

    /// Remove the inbound channel for a session ID.
    fn remove_inputs(&mut self, sid: SessionId) {
        self.inputs.remove(&sid);
    }

    /// Returns a clone of the inbound channel for a session, if present.
    fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    /// Reserve a unique session identifier for future tasks.
    fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        sid
    }

    /// Broadcast topology readiness so all senders may advance their state gates.
    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        let _ = self.topology_ready_tx.send(ready);
    }

    fn enqueue_pending_receiver(
        &mut self,
        key: PendingReceiverKey,
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    ) {
        // Multiple listeners may race to attach; keep them queued until the
        // control plane picks a session ID.
        self.pending
            .entry(key)
            .or_default()
            .push_back(PendingReceiver { cfg, reply });
    }

    /// Pair the next pending receiver for a (destination, source) tuple with the
    /// concrete session ID chosen by the control plane.
    fn adopt_pending_receiver(
        &mut self,
        key: PendingReceiverKey,
        session_id: SessionId,
    ) -> Option<(ReceiverConfig, oneshot::Sender<SessionId>)> {
        let queue = self.pending.get_mut(&key)?;
        let mut pending = queue.pop_front()?;
        pending.cfg.common.session_id = session_id;
        if queue.is_empty() {
            self.pending.remove(&key);
        }

        Some((pending.cfg, pending.reply))
    }
}
