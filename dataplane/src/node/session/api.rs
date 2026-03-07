use tokio::sync::oneshot;

use crate::node::session::runtime::{PreflightError, ReceiverRequest, SenderRequest};

pub type SessionId = u64;

pub use crate::node::session::runtime::LosslessRuntimeHandle;

/// Metadata and payload extracted from inbound lossless frames.
#[derive(Clone, Debug)]
pub struct InboundFrame {
    pub bytes: Vec<u8>,
    pub peer_id: Option<usize>,
}

pub(super) enum Command {
    StartSender {
        cfg: SenderRequest,
        reply: oneshot::Sender<Result<SessionId, PreflightError>>,
    },
    StartReceiver {
        cfg: ReceiverRequest,
        reply: oneshot::Sender<SessionId>,
    },
    Stop {
        session: SessionId,
    },
    Deliver {
        session: SessionId,
        frame: InboundFrame,
    },
    Wait {
        session: SessionId,
        reply: oneshot::Sender<bool>,
    },
    SetTopologyReady {
        ready: bool,
    },
}
