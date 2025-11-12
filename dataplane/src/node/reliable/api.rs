use std::net::Ipv4Addr;

use tokio::sync::{mpsc, oneshot};

use super::session::{PendingReceiverKey, ReceiverConfig, SenderConfig};

pub type SessionId = u64;

#[derive(Clone, Debug)]
pub struct InboundFrame {
    pub bytes: Vec<u8>,
    pub peer_id: Option<usize>,
    pub group_ip: Option<Ipv4Addr>,
    pub source_node_id: Option<usize>,
}

impl InboundFrame {
    #[allow(dead_code)]
    pub fn new(
        bytes: Vec<u8>,
        peer_id: Option<usize>,
        group_ip: Option<Ipv4Addr>,
        source_node_id: Option<usize>,
    ) -> Self {
        Self {
            bytes,
            peer_id,
            group_ip,
            source_node_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReliableHandle {
    #[allow(dead_code)]
    tx: mpsc::UnboundedSender<Command>,
}

#[allow(dead_code)]
pub enum Command {
    StartSender {
        cfg: SenderConfig,
        reply: oneshot::Sender<SessionId>,
    },
    StartReceiver {
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    },
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
    AllocateSession {
        reply: oneshot::Sender<SessionId>,
    },
}

impl ReliableHandle {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Command>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    #[allow(dead_code)]
    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartSender { cfg, reply: tx });
        rx.await.expect("start_sender reply")
    }

    #[allow(dead_code)]
    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartReceiver { cfg, reply: tx });
        rx.await.expect("start_receiver reply")
    }

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

    #[allow(dead_code)]
    pub fn stop(&self, session: SessionId) {
        let _ = self.tx.send(Command::Stop { session });
    }

    #[allow(dead_code)]
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        tracing::info!(
            session_id = session,
            bytes_len = frame.bytes.len(),
            ?frame.peer_id,
            ?frame.group_ip,
            ?frame.source_node_id,
            "ReliableHandle::deliver: sending Command::Deliver to channel"
        );
        match self.tx.send(Command::Deliver { session, frame }) {
            Ok(_) => {
                tracing::info!(
                    session_id = session,
                    "ReliableHandle::deliver: Command::Deliver sent successfully"
                );
            }
            Err(e) => {
                tracing::error!(
                    session_id = session,
                    error = ?e,
                    "ReliableHandle::deliver: failed to send Command::Deliver - channel closed?"
                );
            }
        }
    }

    #[allow(dead_code)]
    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Wait { session, reply: tx });
        rx.await.unwrap_or(false)
    }

    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::AllocateSession { reply: tx });
        rx.await.expect("allocate_session_id reply")
    }
}
