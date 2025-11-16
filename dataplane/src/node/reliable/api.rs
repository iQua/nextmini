use std::net::Ipv4Addr;

use tokio::sync::{mpsc, oneshot};

use super::session::{PendingReceiverKey, ReceiverConfig, SenderConfig};

pub type SessionId = u64;

/// Metadata and payload extracted from inbound reliable frames. The control
/// loop fills out peer/destination context without re-parsing outer headers.
#[derive(Clone, Debug)]
pub struct InboundFrame {
    /// Raw payload extracted from the transport pipeline.
    pub bytes: Vec<u8>,
    /// Optional identifier for the peer that sourced the frame.
    pub peer_id: Option<usize>,
    /// Destination IP (unicast or multicast) for the frame, if any.
    pub dest_ip: Option<Ipv4Addr>,
    /// Controller-assigned ID of the originating node.
    pub source_node_id: Option<usize>,
}

/// Thin handle that lets callers enqueue commands for the reliable runtime
/// task (sender/receiver lifecycle, frame delivery, etc.).
#[derive(Clone, Debug)]
pub struct ReliableHandle {
    tx: mpsc::UnboundedSender<Command>,
}

/// Commands processed by the reliable runtime event loop. Most commands are
/// async (reply over oneshot) so the caller can await session IDs or
/// completion state.
pub enum Command {
    StartSender {
        cfg: SenderConfig,
        reply: oneshot::Sender<SessionId>,
    },
    StartReceiver {
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    },
    #[allow(dead_code)]
    StartReceiverPending {
        cfg: ReceiverConfig,
        key: PendingReceiverKey,
        reply: oneshot::Sender<SessionId>,
    },
    Stop {
        session: SessionId,
    },
    /// Deliver an inbound reliable frame (bytes) to a receiver session.
    Deliver {
        session: SessionId,
        frame: InboundFrame,
    },
    Wait {
        session: SessionId,
        reply: oneshot::Sender<bool>,
    },
    #[allow(dead_code)]
    AllocateSession {
        reply: oneshot::Sender<SessionId>,
    },
    SetTopologyReady {
        ready: bool,
    },
    SetDestRoutesReady {
        dest_ip: Ipv4Addr,
        src_node_id: usize,
    },
}

impl ReliableHandle {
    /// Creates a handle plus receiver pair that can be owned by the runtime task.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Command>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Request that the runtime spin up a sender session with the supplied
    /// configuration and return its session ID.
    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartSender { cfg, reply: tx });
        rx.await.expect("start_sender reply")
    }

    /// Request that the runtime spin up a receiver immediately.
    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartReceiver { cfg, reply: tx });
        rx.await.expect("start_receiver reply")
    }

    /// Request that the runtime stage a receiver that will be paired once the
    /// control-plane assigns a session ID (pending receivers cover this race).
    #[allow(dead_code)]
    pub async fn start_receiver_pending(
        &self,
        cfg: ReceiverConfig,
        key: PendingReceiverKey,
    ) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartReceiverPending {
            cfg,
            key,
            reply: tx,
        });
        rx.await.expect("start_receiver_pending reply")
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
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Wait { session, reply: tx });
        rx.await.unwrap_or(false)
    }

    /// Reserve the next session identifier from the runtime's allocator.
    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::AllocateSession { reply: tx });
        rx.await.expect("allocate_session_id reply")
    }

    /// Notify the runtime that the control plane finished installing topology.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.tx.send(Command::SetTopologyReady { ready });
    }

    /// Notify the runtime that destination routes for (dest, src) are in place.
    pub fn set_dest_routes_ready(&self, dest_ip: Ipv4Addr, src_node_id: usize) {
        let _ = self.tx.send(Command::SetDestRoutesReady {
            dest_ip,
            src_node_id,
        });
    }
}
