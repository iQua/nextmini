pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::packet::Packet;
use tokio::sync::mpsc;

const SOCKET_BUFFER_SIZE: usize = 1048576;
pub const INVALID_FLOW_ID: u128 = u128::MAX;

/// Reserved flow ID for link probe packets.
/// Corresponds to src=127.0.0.1, dst=127.0.0.2, ports=0.
pub const PROBE_FLOW_ID: u128 = 0x7F000001_7F000002_0000_0000_0000_0000;

/// A type for sending packets to destinations in user-space TCP flows.
pub type UserSpaceSender = mpsc::Sender<Packet>;
