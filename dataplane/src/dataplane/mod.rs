pub mod configs;
pub mod context;
pub mod controller_interface;
pub mod drop;
pub mod local_interface;
pub mod metrics;
pub mod node_interface;
pub mod packet;
pub mod processor;
pub mod protocols_client;
pub mod protocols_io;
pub mod protocols_server;
pub mod routes;
pub mod scheduler;
pub mod utils;

use std::collections::HashMap;

use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::dataplane::packet::Packet;
use crate::dataplane::utils::RateLimiter;

/// The maximum Maximum Transmission Unit (MTU).
const MAX_MTU: usize = 6400;

/// The buffer size for ProtocolReader to receive a packet from the network.
pub const RECEIVE_BUF_SIZE: usize = MAX_MTU + 4;

/// The maximum number of packets allowed in Tokio's bounded mpsc (multi-producer, single-consumer) channels,
/// used throughout the dataplane implementation
const INTERNAL_Q_SIZE: usize = 10000;

/// The flow ID is a 128-bit integer, used to store complete 4-tuple: src_ip(32) + dst_ip(32) + src_port(16)
/// + dst_port(16) + reserved(32)
pub type FlowId = u128;

pub trait FlowIdExt {
    fn src_ip(&self) -> std::net::Ipv4Addr;
    fn dst_ip(&self) -> std::net::Ipv4Addr;
    fn src_port(&self) -> u16;
    fn dst_port(&self) -> u16;
}

impl FlowIdExt for FlowId {
    fn src_ip(&self) -> std::net::Ipv4Addr {
        // Extract source IP
        let src_u32 = (self >> 96) as u32;
        std::net::Ipv4Addr::from(src_u32)
    }

    fn dst_ip(&self) -> std::net::Ipv4Addr {
        // Extract destination IP
        let dst_u32 = ((self >> 64) & 0xFFFFFFFF) as u32;
        std::net::Ipv4Addr::from(dst_u32)
    }

    fn src_port(&self) -> u16 {
        // Extract source port
        ((self >> 48) & 0xFFFF) as u16
    }

    fn dst_port(&self) -> u16 {
        // Extract destination port
        ((self >> 32) & 0xFFFF) as u16
    }
}

/// The node ID.
pub type NodeId = usize;

/// The packet buffer, used for receiving a packet from the network.
type PacketBuf = [u8; RECEIVE_BUF_SIZE];

/// Each processor channel includes a sender and a receiver for an mpsc channel.
type ProcessorChannel = (mpsc::Sender<Packet>, Option<mpsc::Receiver<Packet>>);

/// A hashmap from node IDs to rate limiters (optional)
type RateLimiterMap = HashMap<NodeId, Arc<RwLock<Option<RateLimiter>>>>;
