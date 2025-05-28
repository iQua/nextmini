use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

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

    fn get_flow_id_from_buf(buf: &PacketBuf) -> FlowId {
        if buf[0] >> 4 == 4 {
            // IPv4 packet detected, now extracts source and destination IP addresses
            let mut cursor = Cursor::new(buf.get(12..20).unwrap());
            let src_dst_ip = cursor.read_u64::<BigEndian>().unwrap();

            let mut cursor = Cursor::new(buf.get(20..24).unwrap());
            let src_dst_port = cursor.read_u32::<BigEndian>().unwrap();

            // packs complete 4-tuple into 128-bit flow_id without compression:
            // src_ip(32) + dst_ip(32) + src_port(16) + dst_port(16) + reserved(32)
            (src_dst_ip as u128) << 64 | (src_dst_port as u128) << 32
        } else {
            0
        }
    }
}
