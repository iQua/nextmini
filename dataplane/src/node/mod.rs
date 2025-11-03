pub mod conductor;
pub mod config;
pub mod connector;
pub mod controller;
pub mod flow;
pub mod local;
#[cfg(target_os = "linux")]
pub mod namespace;
pub mod network;
pub mod packet;
pub mod processor;
pub mod route;
pub mod scheduler;

use std::net::Ipv4Addr;

use jumphash::JumpHasher;

/// The node ID.
pub type NodeId = usize;

/// Converts a NodeId to a virtual IP address.
pub trait NodeIdExt {
    /// Computes a new virtual IP address by adding the node ID to the base address.
    fn ip_addr(&self, base_addr: Ipv4Addr, net_mask: Ipv4Addr) -> Ipv4Addr;
}

impl NodeIdExt for NodeId {
    fn ip_addr(&self, base_addr: Ipv4Addr, net_mask: Ipv4Addr) -> Ipv4Addr {
        // makes a copy of the base address
        let base_ip = u32::from(base_addr);

        // adds the node ID as an offset to the base address
        let new_ip = base_ip.wrapping_add(*self as u32);

        // applies the netmask to protect against overflow
        let net_mask = u32::from(net_mask);
        let network = base_ip & net_mask;
        let new_network = new_ip & net_mask;

        // checks if the new virtual address is outside the subnet
        if network != new_network {
            panic!(
                "Could not generate IP for node {}, the node ID might be too large for the network mask.",
                self
            );
        } else {
            Ipv4Addr::from(new_ip)
        }
    }
}

/// The maximum Maximum Transmission Unit (MTU).
const MAX_MTU: usize = 6400;

/// The buffer size for the network interface reader to receive a packet from the network.
const RECEIVE_BUF_SIZE: usize = MAX_MTU + 4;

/// The flow ID is a 128-bit integer, used to store complete 4-tuple: src_ip(32) + dst_ip(32) + src_port(16)
/// + dst_port(16) + reserved(32)
pub type FlowId = u128;

pub trait FlowIdExt {
    fn src_ip(&self) -> Ipv4Addr;
    fn dst_ip(&self) -> Ipv4Addr;
    fn src_port(&self) -> u16;
    fn dst_port(&self) -> u16;
    fn reverse(&self) -> FlowId;
    fn hash(&self, capacity: usize) -> usize;
}

impl FlowIdExt for FlowId {
    /// Extracts the source IP
    fn src_ip(&self) -> Ipv4Addr {
        let src_u32 = (self >> 96) as u32;
        Ipv4Addr::from(src_u32)
    }

    /// Extracts the destination IP
    fn dst_ip(&self) -> Ipv4Addr {
        let dst_u32 = ((self >> 64) & 0xFFFFFFFF) as u32;
        Ipv4Addr::from(dst_u32)
    }

    /// Extracts the source port
    fn src_port(&self) -> u16 {
        ((self >> 48) & 0xFFFF) as u16
    }

    /// Extracts the destination port
    fn dst_port(&self) -> u16 {
        ((self >> 32) & 0xFFFF) as u16
    }

    fn reverse(&self) -> FlowId {
        let src_ip = self.src_ip();
        let dst_ip = self.dst_ip();
        let src_port = self.src_port();
        let dst_port = self.dst_port();

        let new_src_ip = u32::from(dst_ip) as u128;
        let new_dst_ip = u32::from(src_ip) as u128;
        let new_src_port = dst_port as u128;
        let new_dst_port = src_port as u128;

        (new_src_ip << 96) | (new_dst_ip << 64) | (new_src_port << 48) | (new_dst_port << 32)
    }

    /// Computes the hash value using Jump Hash, a consistent hash function
    fn hash(&self, capacity: usize) -> usize {
        let hasher = JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321);
        let hash = hasher.slot(&self, capacity as u32);

        hash as usize
    }
}
