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
        if self.buf.len() < 20 || (self.buf[0] >> 4) != 4 {
            return 0;
        }
        let ihl = (self.buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if self.buf.len() < ip_header_len + 8 {
            return 0;
        }
        // extracts the sequence number from the TCP header
        BigEndian::read_u32(&self.buf[ip_header_len + 4..ip_header_len + 8])
    }

    // Is this packet a TCP data packet with non-zero TCP payload?
    pub fn is_tcp_data(&self) -> bool {
        if self.buf.len() < 20 || (self.buf[0] >> 4) != 4 {
            return false;
        }
        if self.buf[9] != 6 {
            return false; // not TCP
        }

        self.has_tcp_payload()
    }

    // Is this packet a TCP FIN or RST?
    pub fn is_tcp_fin_or_rst(&self) -> bool {
        // Must be TCP and long enough for flags byte.
        if self.buf.len() < 20 || self.buf[9] != 6 {
            return false;
        }

        let ihl = (self.buf[0] & 0x0F) as usize;
        let tcp_offset = ihl * 4;
        if self.buf.len() <= tcp_offset + 13 {
            return false;
        }
        let tcp_flags = self.buf[tcp_offset + 13];

        // checks if FIN or RST flag is set
        (tcp_flags & 0x01) != 0 || (tcp_flags & 0x04) != 0
    }

    /// Does this TCP packet have payload (non-zero data length)?
    pub fn has_tcp_payload(&self) -> bool {
        // must be IPv4 TCP
        if self.buf.len() < 20 || self.buf[9] != 6 {
            return false;
        }

        let ihl = (self.buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if self.buf.len() < ip_header_len + 14 {
            // not enough for a minimal TCP header with flags/offset
            return false;
        }

        // extracts total length from IP header (bytes 2-3)
        let total_length = BigEndian::read_u16(&self.buf[2..4]) as usize;

        // extracts TCP header length from data offset field (upper 4 bits of byte 12 in TCP header)
        let tcp_offset = ip_header_len;
        let tcp_data_offset = ((self.buf[tcp_offset + 12] >> 4) & 0x0F) as usize;
        let tcp_header_len = tcp_data_offset * 4;

        // guards against malformed headers that claim a tiny offset
        if tcp_header_len < 20 || ip_header_len + tcp_header_len > self.buf.len() {
            return false;
        }

        // calculates payload size
        let payload_size = total_length.saturating_sub(ip_header_len + tcp_header_len);

        payload_size > 0
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn make_tcp_packet(flags: u8, payload_len: usize) -> Packet {
        let ip_hlen = 20usize;
        let tcp_hlen = 20usize;
        let total_len = ip_hlen + tcp_hlen + payload_len;

        let mut buf = vec![0u8; total_len];

        // IPv4 header (IHL=5)
        buf[0] = 0x45;
        BigEndian::write_u16(&mut buf[2..4], total_len as u16);
        buf[8] = 64; // TTL
        buf[9] = 6; // TCP
        // src/dst ip
        buf[12..16].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 1).octets());
        buf[16..20].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 2).octets());

        // TCP header
        let tcp_off = ip_hlen;
        BigEndian::write_u16(&mut buf[tcp_off..tcp_off + 2], 4000); // src port
        BigEndian::write_u16(&mut buf[tcp_off + 2..tcp_off + 4], 5000); // dst port
        BigEndian::write_u32(&mut buf[tcp_off + 4..tcp_off + 8], 1); // seq
        // data offset = 5 (20 bytes), NS/CWR/ECE=0
        buf[tcp_off + 12] = 0x50;
        buf[tcp_off + 13] = flags;

        // payload already zero-initialized
        Packet::new(total_len, buf)
    }

    #[test]
    fn is_tcp_data_true_for_ack_with_payload() {
        // ACK with payload -> should be considered data
        let p = make_tcp_packet(0x10 /* ACK */, 64);
        assert!(p.is_tcp_data(), "ACK + payload must be treated as data.");
        assert!(p.has_tcp_payload(), "Sanity: payload should be detected.");
    }

    #[test]
    fn is_tcp_data_false_for_pure_ack() {
        // Pure ACK (no payload) -> not data
        let p = make_tcp_packet(0x10 /* ACK */, 0);
        assert!(!p.is_tcp_data(), "Pure ACK must not be treated as data.");
        assert!(!p.has_tcp_payload(), "Sanity: no payload.");
    }

    #[test]
    fn is_tcp_data_true_for_fin_with_payload() {
        // FIN can be piggybacked with data; treat as data if payload present
        let p = make_tcp_packet(0x11 /* FIN|ACK */, 32);
        assert!(
            p.is_tcp_data(),
            "FIN with payload should be treated as data."
        );
        assert!(p.has_tcp_payload(), "Sanity: payload should be detected.");
    }
}
