use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use nextmini_messages::TokenBucketSpec;

use crate::node::processor::ProcessorHandle;

use super::api::{InboundFrame, SessionId};

/// Socket addressing and runtime knobs shared by senders and receivers.
#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub dest_ip: Ipv4Addr,
    pub chunk_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub control_weight: usize,
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
    pub routes_ready: Option<watch::Receiver<bool>>,
}

/// Receiver-only configuration (source node, expected bytes, sink path, etc.).
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
}

/// Tracks running reliable sessions along with their inboxes and join handles.
pub struct SessionManager {
    processors: ProcessorHandle,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    pending: AHashMap<PendingReceiverKey, VecDeque<PendingReceiver>>,
    topology_ready_tx: watch::Sender<bool>,
    route_ready: AHashMap<(Ipv4Addr, usize), watch::Sender<bool>>,
}

/// Key that allows a receiver to be created speculatively and paired once the
/// control plane decides which session ID to use for a (destination, source) tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingReceiverKey {
    pub dest_ip: Ipv4Addr,
    pub source_node_id: usize,
}

/// Wrapper that stores receiver config until a session ID is assigned.
struct PendingReceiver {
    cfg: ReceiverConfig,
    reply: oneshot::Sender<SessionId>,
}

impl SessionManager {
    /// Removes and returns the join handle for a session task, if present.
    pub fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    /// Construct a manager that can spawn sender/receiver tasks and track their lifetimes.
    pub fn new(processors: ProcessorHandle) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);
        Self {
            processors,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            pending: AHashMap::default(),
            topology_ready_tx,
            route_ready: AHashMap::default(),
        }
    }

    /// Spawn a sender task, wiring up control-plane readiness watchers and
    /// returning its assigned session ID.
    pub fn spawn_sender(&mut self, mut cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();
        cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        let route_key = (cfg.common.dest_ip, cfg.common.local_node_id);
        let routes_ready_sender = self.route_ready.entry(route_key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(false);
            tx
        });
        cfg.routes_ready = Some(routes_ready_sender.subscribe());
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);
        let handle = tokio::spawn(super::sender::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        sid
    }

    /// Spawn a receiver task and hand it a bounded inbox for inbound frames.
    pub fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);
        let processors = self.processors.clone();
        let handle = tokio::spawn(super::receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        sid
    }

    /// Abort a running session and drop its inbox, if still active.
    pub async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) {
            h.abort();
        }
        self.remove_inputs(sid);
    }

    /// Remove the inbound channel for a session ID.
    pub fn remove_inputs(&mut self, sid: SessionId) {
        self.inputs.remove(&sid);
    }

    /// Returns a clone of the inbound channel for a session, if present.
    pub fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    /// Reserve a unique session identifier for future tasks.
    pub fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        sid
    }

    /// Broadcast topology readiness so all senders may advance their state gates.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.topology_ready_tx.send(ready);
    }

    /// Settle or create a destination-route watch channel and mark it ready.
    pub fn set_dest_routes_ready(&mut self, dest_ip: Ipv4Addr, src_node_id: usize) {
        let key = (dest_ip, src_node_id);
        let entry = self.route_ready.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(false);
            tx
        });
        let _ = entry.send(true);
    }

    pub fn enqueue_pending_receiver(
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
    pub fn adopt_pending_receiver(
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
