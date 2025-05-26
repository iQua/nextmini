use crate::dataplane::{FlowId, PacketBuf};

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub packet_size: usize,
    pub buf: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, buf: PacketBuf) -> Self {
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf, packet_size),
            packet_size,
            buf,
        }
    }

    fn get_flow_id_from_buf(buf: &PacketBuf, _packet_size: usize) -> FlowId {
        // Check if it's an IPv4 packet
        if (buf[0] >> 4) != 4 {
            return 0;
        }

        // IPv4 only
        let src_ip = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let dst_ip = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

        let ports_u32 = if buf[9] == 6 || buf[9] == 17 {
            // TCP (6) or UDP (17)
            u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]])
        } else {
            // Other protocols: no ports
            0
        };

        // Pack into 128-bit: src_ip(32) + dst_ip(32) + ports(32) + reserved(32)
        ((src_ip as u128) << 96) | ((dst_ip as u128) << 64) | ((ports_u32 as u128) << 32)
    }
}
