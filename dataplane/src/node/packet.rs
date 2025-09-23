use byteorder::{BigEndian, ByteOrder};

use crate::node::flow;
use crate::node::{FlowId, PacketBuf};

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub packet_size: usize,
    pub buf: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, buf: PacketBuf) -> Self {
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf),
            packet_size,
            buf,
        }
    }

    pub fn seq_num(&self) -> u32 {
        let ihl = (self.buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        let tcp_offset = ip_header_len;

        // extracts the sequence number from the TCP header
        BigEndian::read_u32(&self.buf[tcp_offset + 4..tcp_offset + 8])
    }

    pub fn is_tcp_data(&self) -> bool {
        let ihl = (self.buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        let tcp_offset = ip_header_len;

        // checks if the packet is TCP by examining the protocol field in the IP header (offset 9)
        let is_tcp = self.buf[9] == 6; // 6 is the protocol number for TCP

        // extracts TCP flags from the TCP header (offset +13 contains the flags byte)
        let tcp_flags = self.buf[tcp_offset + 13];

        // checks specific TCP flags
        let is_syn = is_tcp && (tcp_flags & 0x02) != 0; // SYN flag is bit 1
        let is_fin = is_tcp && (tcp_flags & 0x01) != 0; // FIN flag is bit 0
        let is_rst = is_tcp && (tcp_flags & 0x04) != 0; // RST flag is bit 2
        let is_ack = is_tcp && (tcp_flags & 0x10) != 0; // ACK flag is bit 4

        is_tcp && !is_syn && !is_fin && !is_rst && !is_ack
    }

    fn get_flow_id_from_buf(buf: &PacketBuf) -> FlowId {
        if buf[0] >> 4 == 4 {
            let src_dst_ip = BigEndian::read_u64(&buf[12..20]);
            let src_dst_port = BigEndian::read_u32(&buf[20..24]);
            (src_dst_ip as u128) << 64 | (src_dst_port as u128) << 32
        } else {
            flow::INVALID_FLOW_ID
        }
    }

    /// for TSO support: avoids copying the buffer
    #[cfg(target_os = "linux")]
    pub fn from_slice(packet_size: usize, slice: &[u8]) -> Self {
        let buf = slice[..packet_size].to_vec();
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf),
            packet_size,
            buf,
        }
    }
}
