use tokio::sync::{mpsc, oneshot};

use super::session::{ReceiverConfig, SenderConfig};

pub type SessionId = u64;

#[derive(Clone)]
pub struct ReliableHandle {
    tx: mpsc::UnboundedSender<Command>,
}

pub enum Command {
    StartSender {
        cfg: SenderConfig,
        reply: oneshot::Sender<SessionId>,
    },
    StartReceiver {
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    },
    Stop {
        session: SessionId,
    },
    Wait {
        session: SessionId,
        reply: oneshot::Sender<bool>,
    },
}

impl ReliableHandle {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Command>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartSender { cfg, reply: tx });
        rx.await.expect("start_sender reply")
    }

    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartReceiver { cfg, reply: tx });
        rx.await.expect("start_receiver reply")
    }

    pub fn stop(&self, session: SessionId) {
        let _ = self.tx.send(Command::Stop { session });
    }

    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Wait { session, reply: tx });
        rx.await.unwrap_or(false)
    }
}
