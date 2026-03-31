pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::packet::Packet;
use tokio::sync::mpsc;

const SOCKET_BUFFER_SIZE: usize = 1048576;
pub const INVALID_FLOW_ID: u128 = u128::MAX;

/// A type for sending packets to destinations in user-space TCP flows.
pub type UserSpaceSender = mpsc::Sender<Packet>;
