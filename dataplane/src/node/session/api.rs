//! Lightweight runtime-facing types shared across the lossless session actor.

use std::fmt::{Display, Formatter};

use tokio::sync::oneshot;
use tokio::sync::watch;

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

/// Final outcome reported by a completed lossless session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionOutcome {
    Completed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SessionState {
    Running,
    Finished(SessionOutcome),
}

/// Public handle for one started lossless session.
#[derive(Debug)]
pub struct LosslessSessionHandle {
    pub(crate) session_id: SessionId,
    #[cfg_attr(not(any(test, feature = "python-extension")), allow(dead_code))]
    pub(crate) runtime: crate::node::session::runtime::LosslessRuntimeHandle,
    pub(crate) state_receiver: watch::Receiver<SessionState>,
}

/// Errors returned when starting a sender or receiver session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartError {
    RuntimeChannelClosed,
    SessionAlreadyActive { session_id: SessionId },
    Preflight(PreflightError),
}

impl From<PreflightError> for StartError {
    fn from(value: PreflightError) -> Self {
        Self::Preflight(value)
    }
}

impl Display for StartError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeChannelClosed => {
                write!(f, "lossless runtime channel closed before session start completed")
            }
            Self::SessionAlreadyActive { session_id } => {
                write!(f, "session {session_id} is already active")
            }
            Self::Preflight(err) => Display::fmt(err, f),
        }
    }
}

/// Messages sent to the background lossless runtime task.
pub(super) enum LosslessRuntimeMessage {
    /// Start a sender task for the provided session request.
    StartSender {
        cfg: SenderRequest,
        reply: oneshot::Sender<Result<LosslessSessionHandle, StartError>>,
    },
    /// Start a receiver task for the provided session request.
    StartReceiver {
        cfg: ReceiverRequest,
        reply: oneshot::Sender<Result<LosslessSessionHandle, StartError>>,
    },
    /// Abort and remove a running session task.
    #[cfg_attr(not(any(test, feature = "python-extension")), allow(dead_code))]
    Abort { session_id: SessionId },
    /// Deliver one decoded session frame to the matching task.
    Deliver {
        session: SessionId,
        frame: InboundFrame,
    },
    /// Report one child session exit back into the runtime actor.
    SessionExited {
        session_id: SessionId,
        outcome: SessionOutcome,
    },
    /// Update the sender-side topology-ready gate shared by new sessions.
    SetTopologyReady { ready: bool },
}
