pub mod conductor;
pub mod config;
pub mod controller_interface;
pub mod drop;
pub mod local_interface;
pub mod network_interface;
pub mod packet;
pub mod processor;
pub mod quic;
pub mod reporter;
pub mod route;
pub mod scheduler;
pub mod tcp;
pub mod token_bucket;

#[cfg(target_os = "linux")]
pub mod local_reader_tso;
#[cfg(target_os = "linux")]
pub mod local_writer_tso;

#[cfg(not(target_os = "linux"))]
pub mod local_reader;
#[cfg(not(target_os = "linux"))]
pub mod local_writer;

use jumphash::JumpHasher;

/// The node ID.
pub type NodeId = usize;

/// The maximum Maximum Transmission Unit (MTU).
const MAX_MTU: usize = 6400;

/// The buffer size for the network interface reader to receive a packet from the network.
const RECEIVE_BUF_SIZE: usize = MAX_MTU + 4;

/// The packet buffer, used for receiving a packet from the network.
type PacketBuf = Vec<u8>;

/// The flow ID is a 128-bit integer, used to store complete 4-tuple: src_ip(32) + dst_ip(32) + src_port(16)
/// + dst_port(16) + reserved(32)
pub type FlowId = u128;

pub trait FlowIdExt {
    fn src_ip(&self) -> std::net::Ipv4Addr;
    fn dst_ip(&self) -> std::net::Ipv4Addr;
    fn src_port(&self) -> u16;
    fn dst_port(&self) -> u16;
    fn hash(&self, capacity: usize) -> usize;
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

    /// Computes the hash value using Jump Hash, a consistent hash function
    fn hash(&self, capacity: usize) -> usize {
        let hasher = JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321);
        let hash = hasher.slot(&self, capacity as u32);

        hash as usize
    }
}

pub trait FlowCvt {
    fn from_ip_port_to_flow_id(
        &self,
        src_ip: u32,
        dst_ip: u32,
        src_port: u16,
        dst_port: u16,
    ) -> FlowId;
}

impl FlowCvt for FlowId {
    fn from_ip_port_to_flow_id(
        &self,
        src_ip: u32,
        dst_ip: u32,
        src_port: u16,
        dst_port: u16,
    ) -> FlowId {
        let mut flow_id = 0;
        flow_id |= (src_ip as u128) << 96;
        flow_id |= (dst_ip as u128) << 64;
        flow_id |= (src_port as u128) << 48;
        flow_id |= (dst_port as u128) << 32;
        flow_id
    }
}
