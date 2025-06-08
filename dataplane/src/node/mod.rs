pub mod conductor;
pub mod config;
pub mod controller_interface;
pub mod drop;
pub mod local_interface;
pub mod metrics;
pub mod network_interface;
pub mod packet;
pub mod processor;
pub mod quic;
pub mod route;
pub mod scheduler;
pub mod tcp;
pub mod udp;

use std::hash::Hasher;

use rapidhash::RapidInlineHasher;

/// The node ID.
pub type NodeId = usize;

/// The maximum Maximum Transmission Unit (MTU).
const MAX_MTU: usize = 6400;

/// The buffer size for the network interface reader to receive a packet from the network.
const RECEIVE_BUF_SIZE: usize = MAX_MTU + 4;

/// The packet buffer, used for receiving a packet from the network.
type PacketBuf = [u8; RECEIVE_BUF_SIZE];

/// The flow ID is a 128-bit integer, used to store complete 4-tuple: src_ip(32) + dst_ip(32) + src_port(16)
/// + dst_port(16) + reserved(32)
pub type FlowId = u128;

pub trait FlowIdExt {
    fn src_ip(&self) -> std::net::Ipv4Addr;
    fn dst_ip(&self) -> std::net::Ipv4Addr;
    fn src_port(&self) -> u16;
    fn dst_port(&self) -> u16;
    fn hash(&self) -> usize;
}

impl FlowIdExt for FlowId {
    /// Extracts the source IP
    fn src_ip(&self) -> std::net::Ipv4Addr {
        let src_u32 = (self >> 96) as u32;
        std::net::Ipv4Addr::from(src_u32)
    }

    /// Extracts the destination IP
    fn dst_ip(&self) -> std::net::Ipv4Addr {
        let dst_u32 = ((self >> 64) & 0xFFFFFFFF) as u32;
        std::net::Ipv4Addr::from(dst_u32)
    }

    /// Extracts the source port
    fn src_port(&self) -> u16 {
        ((self >> 48) & 0xFFFF) as u16
    }

    /// Extracts the destination port
    fn dst_port(&self) -> u16 {
        ((self >> 32) & 0xFFFF) as u16
    }

    /// Computes the hash value using RapidHash, a fast and deterministic hash function
    fn hash(&self) -> usize {
        let mut hasher = RapidInlineHasher::default();
        hasher.write(&self.to_be_bytes());
        let hash = hasher.finish();

        hash as usize
    }
}
