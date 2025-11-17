use std::net::Ipv4Addr;

use tokio::sync::oneshot;

use crate::node::session::runtime::{PendingReceiverKey, ReceiverConfig, SenderConfig};

pub type SessionId = u64;

// Re-export the ReliableRuntimeHandle as the public API
pub use crate::node::session::runtime::ReliableRuntimeHandle;

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

/// Commands processed by the reliable runtime event loop. Most commands are
/// async (reply over oneshot) so the caller can await session IDs or
/// completion state.
pub(super) enum Command {
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
}
