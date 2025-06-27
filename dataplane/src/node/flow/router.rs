use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use smoltcp::wire::{Ipv4Packet, TcpPacket};

use crate::node::LocalDestination;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use tracing::error;

#[derive(Clone, Debug, Default)]
struct RoutingTable {
    clients: HashMap<u16, flume::Sender<Packet>>, // maps client ports to their packet senders
    server: Option<flume::Sender<Packet>>,        // we only have one server port for now
}

#[derive(Clone, Debug)]
pub struct PacketRouter {
    table: Arc<Mutex<RoutingTable>>, // shared routing table for clients and server
    server_port: u16,                // the port on which the user space server listens
}

impl PacketRouter {
    // creates a new packet router
    pub fn new(config: &LocalConfig) -> Self {
        Self {
            table: Arc::new(Mutex::new(RoutingTable::default())),
            server_port: config.user_space_server_port,
        }
    }

    // registers a new client with the router
    pub fn register_client(&self, port: u16, sender: flume::Sender<Packet>) {
        let mut table = self.table.lock().unwrap();
        table.clients.insert(port, sender);
    }

    // deregisters a client from the router
    pub fn deregister_client(&self, port: u16) {
        let mut table = self.table.lock().unwrap();
        table.clients.remove(&port);
    }

    // registers the server with the router, haven't implemented deregistration yet
    pub fn register_server(&self, sender: flume::Sender<Packet>) {
        let mut table = self.table.lock().unwrap();
        table.server = Some(sender);
    }
}

impl LocalDestination for PacketRouter {
    fn send_packet(&self, packet: Packet) {
        // for an IPv4 packet
        let ipv4_packet = Ipv4Packet::new_unchecked(&packet.buf[0..packet.packet_size]);

        // for a TCP packet
        let tcp_packet = TcpPacket::new_unchecked(ipv4_packet.payload());

        // gets the destination port
        let dst_port = tcp_packet.dst_port();

        // gets the routing table
        let table = self.table.lock().unwrap();

        // for the server
        if dst_port == self.server_port {
            // if the server is registered
            if let Some(sender) = &table.server {
                let _ = sender.try_send(packet);
            }
        // for clients
        // Q: not sure if we should handle this case for clients
        // as client will not be the downstream destination
        } else if let Some(sender) = table.clients.get(&dst_port) {
            let _ = sender.try_send(packet);
        }
        // the error case is if there is no destination for port.
        else {
            error!("No destination found for port {}.", dst_port);
        }
    }
}
