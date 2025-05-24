use std::io::Cursor;

use crate::dataplane::{FlowId, PacketBuf};
use byteorder::{BigEndian, ReadBytesExt};

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

    fn get_flow_id_from_buf(buf: &PacketBuf, packet_size: usize) -> FlowId {
        // Check if it's an IPv4 packet
        if packet_size < 20 || buf[0] >> 4 != 4 {
            // Non-IPv4 or insufficient data, using fallback calculation
            // Fallback to old behavior for non-IPv4 packets - convert to 128-bit
            let mut cursor = Cursor::new(buf.get(12..20).unwrap_or(&[0; 8]));
            let old_flow_id = cursor.read_u64::<BigEndian>().unwrap_or(0);
            return old_flow_id as u128;
        }

        // Extract source and destination IP addresses
        let src_ip = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let dst_ip = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

        // Extract IHL to determine start of transport header
        let ihl = (buf[0] & 0x0F) as usize;
        let transport_header_start = 4 * ihl;

        // Validate IHL
        if ihl < 5 || transport_header_start > packet_size {
            // Invalid IHL or insufficient packet size
            return 0;
        }

        let (src_port, dst_port) = if packet_size >= transport_header_start + 4 {
            match buf[9] {
                6 | 17 => {
                    // TCP (6) or UDP (17) - both have ports at same offset
                    let src_port = u16::from_be_bytes([
                        buf[transport_header_start],
                        buf[transport_header_start + 1],
                    ]);
                    let dst_port = u16::from_be_bytes([
                        buf[transport_header_start + 2],
                        buf[transport_header_start + 3],
                    ]);
                    // Extracted ports - removed debug for performance
                    (src_port, dst_port)
                }
                _ => {
                    // Protocol - no port extraction
                    (0, 0) // Other protocols
                }
            }
        } else {
            // Insufficient data for transport header
            (0, 0) // Not enough data
        };

        // Pack complete 4-tuple into 128-bit flow_id without compression:
        // src_ip(32) + dst_ip(32) + src_port(16) + dst_port(16) + reserved(32)
        let flow_id = ((src_ip as u128) << 96)
            | ((dst_ip as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32);

        flow_id
    }
}
