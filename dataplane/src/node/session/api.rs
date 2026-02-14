use tokio::sync::oneshot;

use crate::node::session::runtime::{PreflightError, ReceiverConfig, SenderConfig};

pub type SessionId = u64;

// Re-export the LosslessRuntimeHandle as the public API
pub use crate::node::session::runtime::LosslessRuntimeHandle;

/// Metadata and payload extracted from inbound lossless frames.
#[derive(Clone, Debug)]
pub struct InboundFrame {
    /// Raw payload extracted from the transport pipeline.
    pub bytes: Vec<u8>,
    /// Optional identifier for the peer that sourced the frame.
    pub peer_id: Option<usize>,
}

/// Commands processed by the lossless runtime event loop. Most commands are
/// async (reply over oneshot) so the caller can await session IDs or
/// completion state.
pub(super) enum Command {
    StartSender {
        cfg: SenderConfig,
        reply: oneshot::Sender<Result<SessionId, PreflightError>>,
    },
    StartReceiver {
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    },
    Stop {
        session: SessionId,
    },
    /// Deliver an inbound lossless frame (bytes) to a receiver session.
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
}
