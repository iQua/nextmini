//! Lightweight runtime-facing types shared across the lossless session actor.

use std::fmt::{Display, Formatter};

use tokio::sync::{mpsc, oneshot, watch};

use nextmini_messages::lossless_session::NeedReport;

use crate::node::session::runtime::{
    PreflightError, ReceiverRequest, SenderRequest, TransportRoute,
};

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

/// abort() needs LosslessSessionHandle to talk back to the runtime.
/// Here we simply wrap the abort sender.
#[derive(Debug)]
struct SessionAbortHandle {
    sender: mpsc::UnboundedSender<LosslessRuntimeMessage>,
}

impl SessionAbortHandle {
    fn new(sender: mpsc::UnboundedSender<LosslessRuntimeMessage>) -> Self {
        Self { sender }
    }

    fn abort(&self, session_id: SessionId) {
        let _ = self
            .sender
            .send(LosslessRuntimeMessage::Abort { session_id });
    }
}

/// Public handle for one started lossless session.
#[derive(Debug)]
pub struct LosslessSessionHandle {
    session_id: SessionId,
    state_receiver: watch::Receiver<SessionState>,
    // abort remains part of the public session API can be called
    #[allow(dead_code)]
    abort_handle: SessionAbortHandle,
}

impl LosslessSessionHandle {
    pub(super) fn new(
        session_id: SessionId,
        state_receiver: watch::Receiver<SessionState>,
        abort_sender: mpsc::UnboundedSender<LosslessRuntimeMessage>,
    ) -> Self {
        Self {
            session_id,
            state_receiver,
            abort_handle: SessionAbortHandle::new(abort_sender),
        }
    }

    pub fn id(&self) -> SessionId {
        self.session_id
    }

    pub async fn wait(&mut self) -> SessionOutcome {
        loop {
            match &*self.state_receiver.borrow() {
                SessionState::Running => {}
                SessionState::Finished(outcome) => return outcome.clone(),
            }

            if self.state_receiver.changed().await.is_err() {
                return SessionOutcome::Aborted;
            }
        }
    }

    #[allow(dead_code)]
    pub fn abort(&self) {
        self.abort_handle.abort(self.session_id);
    }
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
                write!(
                    f,
                    "lossless runtime channel closed before session start completed"
                )
            }
            Self::SessionAlreadyActive { session_id } => {
                write!(f, "session {session_id} is already active")
            }
            Self::Preflight(err) => Display::fmt(err, f),
        }
    }
}

/// Replay state retained for receivers that completed before late duplicate
/// frames fully drained out of the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompletedReceiverReplay {
    Plain {
        round_id: u32,
        route: TransportRoute,
        report: NeedReport,
    },
    Fec {
        round_id: u32,
        route: TransportRoute,
        report: NeedReport,
    },
    Mettle {
        round_id: u32,
        route: TransportRoute,
        report: NeedReport,
    },
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
    #[allow(dead_code)]
    Abort { session_id: SessionId },
    /// Deliver one decoded session frame to the matching task.
    Deliver {
        session: SessionId,
        frame: InboundFrame,
    },
    /// Register a completed receiver replay before its inbox is torn down.
    ReceiverCompleted {
        session_id: SessionId,
        replay: CompletedReceiverReplay,
        ack: oneshot::Sender<()>,
    },
    /// Report one child session exit back into the runtime actor.
    SessionExited {
        session_id: SessionId,
        outcome: SessionOutcome,
    },
    /// Update the sender-side topology-ready gate shared by new sessions.
    SetTopologyReady { ready: bool },
}
