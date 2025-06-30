pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::packet::Packet;
use flume;

const SOCKET_BUFFER_SIZE: usize = 655350;

/// A type for sending packets to destinations in user-space TCP flows.
pub type UserSpaceSender = flume::Sender<Packet>;
