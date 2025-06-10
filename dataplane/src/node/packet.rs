use byteorder::{BigEndian, ByteOrder};

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
            let src_dst_ip = BigEndian::read_u64(&buf[12..20]);
            let src_dst_port = BigEndian::read_u32(&buf[20..24]);
            (src_dst_ip as u128) << 64 | (src_dst_port as u128) << 32
        } else {
            0
        }
    }
}
