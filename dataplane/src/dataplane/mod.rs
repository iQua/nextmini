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
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::dataplane::packet::Packet;
use crate::dataplane::utils::RateLimiter;

/// The maximum Maximum Transmission Unit (MTU).
const MAX_MTU: usize = 6400;

/// The buffer size for ProtocolReader to receive a packet from the network.
const RECEIVE_BUF_SIZE: usize = MAX_MTU + 4;

/// The maximum number of packets allowed in Tokio's bounded mpsc (multi-producer, single-consumer) channels,
/// used throughout the dataplane implementation
const INTERNAL_Q_SIZE: usize = 10000;

/// The third element in the destination address is ignored since it determines the interface only.
const FLOW_ID_PATH_MASK: u64 = 0xFFFFFFFF_FFFF00FF;

/// The flow ID.
pub type FlowId = u64;

/// Defines the extension trait for extracting the source and destination IP addresses from FlowId
pub trait FlowIdExt {
    fn src_addr(&self) -> Ipv4Addr;
    fn dest_addr(&self) -> Ipv4Addr;
}

/// Implements the trait for FlowId, which is of u64 type
impl FlowIdExt for u64 {
    fn src_addr(&self) -> Ipv4Addr {
        // Extract upper 32 bits (source IP) by shifting right 32 bits
        let src_u32 = (self >> 32) as u32;
        Ipv4Addr::from(src_u32)
    }

    fn dest_addr(&self) -> Ipv4Addr {
        // Extract lower 32 bits (destination IP) by masking with 0xFFFFFFFF
        let dest_u32 = (self & 0xFFFFFFFF) as u32;
        Ipv4Addr::from(dest_u32)
    }
}

/// The node ID.
pub type NodeId = usize;

/// The socket ID.
// pub type SocketId = (u16, u16);

/// The packet buffer, used for receiving a packet from the network.
type PacketBuf = [u8; RECEIVE_BUF_SIZE];

/// Each processor channel includes a sender and a receiver for an mpsc channel.
type ProcessorChannel = (mpsc::Sender<Packet>, Option<mpsc::Receiver<Packet>>);

/// A hashmap from node IDs to rate limiters (optional)
type RateLimiterMap = HashMap<NodeId, Arc<RwLock<Option<RateLimiter>>>>;
