use crate::node::NodeId;
use crate::node::packet::Packet;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransportScope {
    Default,
    Tree(u16),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ScopedNode {
    pub remote_node_id: NodeId,
    pub scope: TransportScope,
}

impl ScopedNode {
    pub const fn new(remote_node_id: NodeId, scope: TransportScope) -> Self {
        Self {
            remote_node_id,
            scope,
        }
    }
}

impl TransportScope {
    pub const ENCODED_LEN: usize = std::mem::size_of::<u64>() + 1 + std::mem::size_of::<u16>();

    pub fn from_packet(packet: &Packet) -> Self {
        match packet.lossless_fec_tree_id() {
            Some(tree_id) => Self::Tree(tree_id),
            None => Self::Default,
        }
    }

    pub fn encode_handshake(self, local_node_id: NodeId) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0u8; Self::ENCODED_LEN];
        buf[..8].copy_from_slice(&(local_node_id as u64).to_be_bytes());
        match self {
            Self::Default => {
                buf[8] = 0;
            }
            Self::Tree(tree_id) => {
                buf[8] = 1;
                buf[9..11].copy_from_slice(&tree_id.to_be_bytes());
            }
        }
        buf
    }

    pub fn decode_handshake(buf: &[u8; Self::ENCODED_LEN]) -> Option<(NodeId, Self)> {
        let remote_node_id = u64::from_be_bytes(buf[..8].try_into().ok()?) as NodeId;
        let scope = match buf[8] {
            0 => Self::Default,
            1 => Self::Tree(u16::from_be_bytes(buf[9..11].try_into().ok()?)),
            _ => return None,
        };
        Some((remote_node_id, scope))
    }
}
