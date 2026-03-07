//! Lightweight runtime-facing types shared across the lossless session actor.

use tokio::sync::oneshot;

use crate::node::session::runtime::{PreflightError, ReceiverRequest, SenderRequest};

/// Opaque identifier used to route lossless session control and data frames.
pub type SessionId = u64;

pub use crate::node::session::runtime::LosslessRuntimeHandle;

/// Metadata and payload extracted from inbound lossless frames.
#[derive(Clone, Debug)]
pub struct InboundFrame {
    /// Raw session frame bytes.
    pub bytes: Vec<u8>,
    /// Optional peer identity derived from the lower transport path.
    pub peer_id: Option<usize>,
}

/// Commands sent to the background lossless runtime task.
pub(super) enum Command {
    /// Start a sender task for the provided session request.
    StartSender {
        cfg: SenderRequest,
        reply: oneshot::Sender<Result<SessionId, PreflightError>>,
    },
    /// Start a receiver task for the provided session request.
    StartReceiver {
        cfg: ReceiverRequest,
        reply: oneshot::Sender<SessionId>,
    },
    /// Abort and remove a running session task.
    Stop { session: SessionId },
    /// Deliver one decoded session frame to the matching task.
    Deliver {
        session: SessionId,
        frame: InboundFrame,
    },
    /// Wait for a task to finish and report whether it existed.
    Wait {
        session: SessionId,
        reply: oneshot::Sender<bool>,
    },
    /// Update the sender-side topology-ready gate shared by new sessions.
    SetTopologyReady { ready: bool },
}
