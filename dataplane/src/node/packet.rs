use std::ops::Deref;
use std::sync::Mutex;

use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
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
        let mut bytes = BytesMut::with_capacity(vec.len());
        bytes.extend_from_slice(&vec);
        PacketBuf {
            buf: Some(bytes),
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

impl Drop for PacketBuf {
    fn drop(&mut self) {
        if self.pooled
            && let Some(mut buf) = self.buf.take() {
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
        let buf = self.bytes();
        if self.packet_size < 20 || (buf[0] >> 4) != 4 {
            return 0;
        }
        if buf[9] != 6 {
            return 0;
        }

        let ihl = (buf[0] & 0x0F) as usize;
        let ip_header_len = ihl * 4;
        if self.packet_size < ip_header_len + 14 {
            return 0;
        }

        let mut total_length = BigEndian::read_u16(&buf[2..4]) as usize;
        if total_length > self.packet_size {
            total_length = self.packet_size;
        }

        let tcp_offset = ip_header_len;
        let tcp_data_offset = ((buf[tcp_offset + 12] >> 4) & 0x0F) as usize;
        let tcp_header_len = tcp_data_offset * 4;

        if tcp_header_len < 20 || ip_header_len + tcp_header_len > self.packet_size {
            return 0;
        }

        total_length.saturating_sub(ip_header_len + tcp_header_len)
    }

    pub fn has_tcp_payload(&self) -> bool {
        self.tcp_payload_len() > 0
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
}
