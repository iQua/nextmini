pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::LocalDestination;
use crate::node::packet::Packet;
use tokio::sync::mpsc;

const SOCKET_BUFFER_SIZE: usize = 655350;

/// A trait for sending packets to destinations in user-space TCP flows.
impl LocalDestination for mpsc::Sender<Packet> {
    fn send_packet(&self, packet: Packet) {
        if self.try_send(packet).is_err() {
            tracing::error!("Failed to send a packet to its local destination.");
        }
    }
}
