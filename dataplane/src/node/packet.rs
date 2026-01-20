use std::net::Ipv4Addr;
use std::ops::Deref;
use std::sync::Mutex;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;

use crate::node::flow;
use crate::node::{FlowId, RECEIVE_BUF_SIZE};

static PACKET_BUFFER_POOL: Lazy<Mutex<Vec<BytesMut>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// A reusable packet buffer backed by a global pool.
#[derive(Debug)]
pub struct PacketBuf {
    buf: Option<BytesMut>,
    pooled: bool,
}

impl Default for PacketBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketBuf {
    /// Acquires a buffer from the global pool, allocating on demand.
    pub fn new() -> Self {
        let mut pool = PACKET_BUFFER_POOL.lock().unwrap();
        let mut buf = pool
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(RECEIVE_BUF_SIZE));
        buf.truncate(0);
        PacketBuf {
            buf: Some(buf),
            pooled: true,
        }
    }

    /// Wraps an existing vector without copying the contents.
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let bytes = Bytes::from(vec);
        PacketBuf {
            buf: Some(BytesMut::from(bytes)),
            pooled: false,
        }
    }

    /// Wraps an existing BytesMut (used internally for zero-copy splits).
    pub fn from_bytes_mut(bytes: BytesMut) -> Self {
        PacketBuf {
            buf: Some(bytes),
            pooled: false,
        }
    }

    /// Ensures the buffer has space for `len` bytes and exposes them as a slice.
    /// The returned slice may contain uninitialised data; callers must fully
    /// overwrite it before using.
    pub fn prepare_uninit(&mut self, len: usize) -> &mut [u8] {
        let buf = self.buf.as_mut().expect("packet buffer already released");
        if buf.capacity() < len {
            buf.reserve(len - buf.capacity());
        }
        let current_len = buf.len();
        if len > current_len {
            unsafe {
                buf.set_len(len);
            }
        } else {
            buf.truncate(len);
        }
        buf.as_mut()
    }

    /// Shrinks the logical length to `len`.
    pub fn truncate(&mut self, len: usize) {
        if let Some(buf) = self.buf.as_mut() {
            buf.truncate(len);
        }
    }

    /// Returns the current length of valid data.
    pub fn len(&self) -> usize {
        self.buf.as_ref().map(|b| b.len()).unwrap_or(0)
    }

    /// Indicates whether the buffer currently holds any bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Provides read-only access to the stored bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.deref()
    }

    /// Provides mutable access to the currently initialised bytes.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf
            .as_mut()
            .expect("packet buffer already released")
            .as_mut()
    }
}

impl Deref for PacketBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().map(|b| b.as_ref()).unwrap_or(&[])
    }
}

#[cfg(feature = "python-extension")]
impl AsRef<[u8]> for PacketBuf {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Drop for PacketBuf {
    fn drop(&mut self) {
        if self.pooled
            && let Some(mut buf) = self.buf.take()
        {
            buf.truncate(0);
            if buf.capacity() > RECEIVE_BUF_SIZE * 4 {
                buf = BytesMut::with_capacity(RECEIVE_BUF_SIZE);
            }
            PACKET_BUFFER_POOL.lock().unwrap().push(buf);
        }
    }
}

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub packet_size: usize,
    buffer: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, mut buffer: PacketBuf) -> Self {
        if buffer.len() > packet_size {
            buffer.truncate(packet_size);
        }
        let actual_size = buffer.len().min(packet_size);
        let flow_id = if buffer.is_empty() {
            flow::INVALID_FLOW_ID
        } else {
            Self::get_flow_id_from_buf(&buffer[..actual_size])
        };
        Self {
            flow_id,
            packet_size: actual_size,
            buffer,
        }
    }

    /// Convenience constructor for tests.
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let len = vec.len();
        Packet::new(len, PacketBuf::from_vec(vec))
    }

    /// Returns a read-only view over the packet payload.
    pub fn bytes(&self) -> &[u8] {
        &self.buffer[..self.packet_size]
    }

    #[cfg(feature = "python-extension")]
    pub fn tcp_payload_range(&self) -> Option<std::ops::Range<usize>> {
        self.tcp_payload_bounds().map(|(start, end)| start..end)
    }

    pub fn seq_num(&self) -> u32 {
        let buf = self.bytes();
        if self.packet_size < 20 || (buf[0] >> 4) != 4 {
            return 0;
        }
        let ihl = (buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if self.packet_size < ip_header_len + 8 {
            return 0;
        }
        BigEndian::read_u32(&buf[ip_header_len + 4..ip_header_len + 8])
    }

    pub fn is_tcp_data(&self) -> bool {
        let buf = self.bytes();
        if self.packet_size < 20 || (buf[0] >> 4) != 4 {
            return false;
        }
        if buf[9] != 6 {
            return false;
        }

        self.has_tcp_payload()
    }

    pub fn is_tcp_syn(&self) -> bool {
        let buf = self.bytes();
        if self.packet_size < 20 || buf[9] != 6 {
            return false;
        }

        let ihl = (buf[0] & 0x0F) as usize;
        let tcp_offset = ihl * 4;
        if self.packet_size <= tcp_offset + 13 {
            return false;
        }

        let tcp_flags = buf[tcp_offset + 13];
        (tcp_flags & 0x02) != 0
    }

    pub fn is_tcp_fin_or_rst(&self) -> bool {
        let buf = self.bytes();
        if self.packet_size < 20 || buf[9] != 6 {
            return false;
        }

        let ihl = (buf[0] & 0x0F) as usize;
        let tcp_offset = ihl * 4;
        if self.packet_size <= tcp_offset + 13 {
            return false;
        }
        let tcp_flags = buf[tcp_offset + 13];

        (tcp_flags & 0x01) != 0 || (tcp_flags & 0x04) != 0
    }

    pub fn tcp_payload_len(&self) -> usize {
        self.tcp_payload_bounds()
            .map(|(start, end)| end - start)
            .unwrap_or(0)
    }

    pub fn has_tcp_payload(&self) -> bool {
        self.tcp_payload_len() > 0
    }

    pub fn tcp_payload(&self) -> Option<&[u8]> {
        let (start, end) = self.tcp_payload_bounds()?;
        Some(&self.bytes()[start..end])
    }

    #[cfg(feature = "python-extension")]
    pub fn into_bytes(self) -> Bytes {
        let packet_size = self.packet_size;
        let bytes = Bytes::from_owner(self.buffer);
        if bytes.len() > packet_size {
            bytes.slice(..packet_size)
        } else {
            bytes
        }
    }

    fn tcp_payload_bounds(&self) -> Option<(usize, usize)> {
        let buf = self.bytes();
        if self.packet_size < 20 || (buf[0] >> 4) != 4 {
            return None;
        }
        if buf[9] != 6 {
            return None;
        }

        let ihl = (buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if self.packet_size < ip_header_len + 20 {
            return None;
        }

        let mut total_length = BigEndian::read_u16(&buf[2..4]) as usize;
        if total_length > self.packet_size {
            total_length = self.packet_size;
        }

        let tcp_offset = ip_header_len;
        if self.packet_size <= tcp_offset + 12 {
            return None;
        }
        let tcp_data_offset = ((buf[tcp_offset + 12] >> 4) & 0x0F) as usize;
        let tcp_header_len = tcp_data_offset * 4;

        if tcp_header_len < 20 || ip_header_len + tcp_header_len > total_length {
            return None;
        }

        let start = ip_header_len + tcp_header_len;
        if start > total_length {
            return None;
        }

        Some((start, total_length))
    }

    /// Compute the flow identifier directly from the IPv4+TCP tuple.
    #[allow(dead_code)]
    pub fn flow_id_from_parts(
        src_ip: Ipv4Addr,
        src_port: u16,
        dst_ip: Ipv4Addr,
        dst_port: u16,
    ) -> FlowId {
        let mut ip_bytes = [0u8; 8];
        ip_bytes[..4].copy_from_slice(&src_ip.octets());
        ip_bytes[4..].copy_from_slice(&dst_ip.octets());
        let src_dst_ip = BigEndian::read_u64(&ip_bytes);

        let mut port_bytes = [0u8; 4];
        BigEndian::write_u16(&mut port_bytes[..2], src_port);
        BigEndian::write_u16(&mut port_bytes[2..], dst_port);
        let src_dst_port = BigEndian::read_u32(&port_bytes);

        ((src_dst_ip as u128) << 64) | ((src_dst_port as u128) << 32)
    }

    /// Construct a minimal IPv4/TCP packet that wraps the provided payload.
    /// Checksums are omitted—the overlay stack guarantees integrity.
    pub fn build_ipv4_tcp_packet(
        src_ip: Ipv4Addr,
        src_port: u16,
        dst_ip: Ipv4Addr,
        dst_port: u16,
        payload: &[u8],
    ) -> Self {
        const IP_HLEN: usize = 20;
        const TCP_HLEN: usize = 20;

        let total_len = IP_HLEN + TCP_HLEN + payload.len();
        let mut buf = vec![0u8; total_len];

        // IPv4 header
        buf[0] = 0x45; // version=4, header length=5 words
        BigEndian::write_u16(&mut buf[2..4], total_len as u16);
        buf[8] = 64; // TTL
        buf[9] = 6; // protocol = TCP
        buf[12..16].copy_from_slice(&src_ip.octets());
        buf[16..20].copy_from_slice(&dst_ip.octets());

        // TCP header (no options)
        let tcp_off = IP_HLEN;
        BigEndian::write_u16(&mut buf[tcp_off..tcp_off + 2], src_port);
        BigEndian::write_u16(&mut buf[tcp_off + 2..tcp_off + 4], dst_port);
        buf[tcp_off + 12] = 0x50; // data offset = 5 (20 bytes), reserved bits = 0
        buf[tcp_off + 13] = 0x18; // PSH + ACK to mark data frame

        // Payload
        buf[IP_HLEN + TCP_HLEN..].copy_from_slice(payload);

        Packet::from_vec(buf)
    }

    fn get_flow_id_from_buf(buf: &[u8]) -> FlowId {
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

    #[cfg(target_os = "linux")]
    pub fn from_slice(packet_size: usize, slice: &[u8]) -> Self {
        Self::from_vec(slice[..packet_size].to_vec())
    }
}

impl Clone for Packet {
    fn clone(&self) -> Self {
        // Duplicate the underlying bytes; flow_id will be recomputed consistently.
        Packet::from_vec(self.bytes().to_vec())
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

        buf[0] = 0x45;
        BigEndian::write_u16(&mut buf[2..4], total_len as u16);
        buf[8] = 64;
        buf[9] = 6;
        buf[12..16].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 1).octets());
        buf[16..20].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 2).octets());

        let tcp_off = ip_hlen;
        BigEndian::write_u16(&mut buf[tcp_off..tcp_off + 2], 4000);
        BigEndian::write_u16(&mut buf[tcp_off + 2..tcp_off + 4], 5000);
        BigEndian::write_u32(&mut buf[tcp_off + 4..tcp_off + 8], 1);
        buf[tcp_off + 12] = 0x50;
        buf[tcp_off + 13] = flags;

        Packet::from_vec(buf)
    }

    #[test]
    fn is_tcp_data_true_for_ack_with_payload() {
        let p = make_tcp_packet(0x10, 64);
        assert!(p.is_tcp_data());
        assert!(p.has_tcp_payload());
    }

    #[test]
    fn is_tcp_data_false_for_pure_ack() {
        let p = make_tcp_packet(0x10, 0);
        assert!(!p.is_tcp_data());
        assert!(!p.has_tcp_payload());
    }

    #[test]
    fn is_tcp_data_true_for_fin_with_payload() {
        let p = make_tcp_packet(0x11, 32);
        assert!(p.is_tcp_data());
        assert!(p.has_tcp_payload());
    }

    #[test]
    fn flow_id_from_parts_matches_packet_flow_id() {
        let src_ip = Ipv4Addr::new(10, 0, 0, 1);
        let dst_ip = Ipv4Addr::new(10, 0, 0, 2);
        let src_port = 4000;
        let dst_port = 5000;

        let packet = Packet::build_ipv4_tcp_packet(src_ip, src_port, dst_ip, dst_port, &[1, 2, 3]);
        let flow_from_parts = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);

        assert_eq!(flow_from_parts, packet.flow_id);
    }

    #[test]
    fn build_ipv4_tcp_packet_copies_payload() {
        let payload = vec![1u8, 2, 3, 4, 5];
        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &payload,
        );

        assert!(packet.is_tcp_data());
        let header_len = 20 + 20;
        assert_eq!(
            &packet.bytes()[header_len..header_len + payload.len()],
            payload.as_slice()
        );
    }
}
