pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::packet::Packet;
use tokio::sync::mpsc;

const SOCKET_BUFFER_SIZE: usize = 655350;

/// A type for sending packets to destinations in user-space TCP flows.
pub type UserSpaceSender = mpsc::Sender<Packet>;
