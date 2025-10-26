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

    /// Returns `true` if this is a TCP packet **with non-zero payload** and no SYN/FIN/RST.
    /// Previously we excluded any packet with the ACK flag set, which misclassified
    /// normal ACK+data segments as "non-data".
    pub fn is_tcp_data(&self) -> bool {
        // checks if it is tcp
        if self.buf[9] != 6 {
            return false;
        }

        let ihl = (self.buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        let tcp_offset = ip_header_len;

        // checks TCP flags - exclude SYN/FIN/RST
        let tcp_flags = self.buf[tcp_offset + 13];
        let is_syn = (tcp_flags & 0x02) != 0;
        let is_fin = (tcp_flags & 0x01) != 0;
        let is_rst = (tcp_flags & 0x04) != 0;

        if is_syn || is_fin || is_rst {
            return false;
        }

        // extracts total length from IP header (bytes 2-3)
        let total_length = BigEndian::read_u16(&self.buf[2..4]) as usize;

        // extracts TCP header length from data offset field (upper 4 bits of byte 12 in TCP header)
        let tcp_data_offset = ((self.buf[tcp_offset + 12] >> 4) & 0x0F) as usize;
        let tcp_header_len = tcp_data_offset * 4;

        // calculates payload size
        let payload_size = total_length.saturating_sub(ip_header_len + tcp_header_len);

        payload_size > 0
    }

    pub fn is_tcp_fin_or_rst(&self) -> bool {
        // checks if it is tcp
        if self.buf[9] != 6 {
            return false;
        }

        let ihl = (self.buf[0] & 0x0F) as usize;
        let tcp_offset = ihl * 4;
        let tcp_flags = self.buf[tcp_offset + 13];

        // checks if FIN or RST flag is set
        (tcp_flags & 0x01) != 0 || (tcp_flags & 0x04) != 0
    }

    fn get_flow_id_from_buf(buf: &PacketBuf) -> FlowId {
        if buf.len() < 20 || buf[0] >> 4 != 4 {
            return flow::INVALID_FLOW_ID;
        }

        let src_dst_ip = BigEndian::read_u64(&buf[12..20]);

        let ihl = (buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if ip_header_len < 20 || buf.len() < ip_header_len + 4 {
            return flow::INVALID_FLOW_ID;
        }

        let src_dst_port = BigEndian::read_u32(&buf[ip_header_len..ip_header_len + 4]);

        (src_dst_ip as u128) << 64 | (src_dst_port as u128) << 32
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
