use std::io::Cursor;

use crate::dataplane::{FlowId, PacketBuf};
use byteorder::{BigEndian, ReadBytesExt};
use tracing::{debug, warn};

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
        // validates packet size first
        if packet_size == 0 {
            warn!("A zero-length packet has been received.");
            return 0;
        }

        if packet_size > buf.len() {
            warn!(
                "Packet size {} exceeds the buffer length {}, using the buffer length.",
                packet_size,
                buf.len()
            );
        }

        // checks if it's an IPv4 packet
        if packet_size < 20 || buf[0] >> 4 != 4 {
            // Non-IPv4 or insufficient data, using fallback calculation
            warn!(
                "Non-IPv4 packet detected: version: {}, size: {}.",
                buf[0] >> 4,
                packet_size
            );

            // uses the source and destination addresses as the flow ID
            let mut cursor = Cursor::new(buf.get(12..20).unwrap_or(&[0; 8]));
            let flow_id = cursor.read_u64::<BigEndian>().unwrap_or(0);
            debug!(
                "Non-IPv4 packet, using fallback flow_id: {:#x}",
                flow_id as u128
            );
            return flow_id as u128;
        }

        // extracts source and destination IP addresses
        let src_ip = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let dst_ip = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

        // extracts IHL to determine start of transport header
        let ihl = (buf[0] & 0x0F) as usize;
        let transport_header_start = 4 * ihl;

        // validates IHL
        if ihl < 5 || transport_header_start > packet_size {
            // invalid IHL or insufficient packet size
            warn!(
                "Invalid IHL ({}) or insufficient packet size, using zero as the flow ID.",
                ihl
            );

            return 0;
        }

        let (src_port, dst_port) = if packet_size >= transport_header_start + 4 {
            match buf[9] {
                6 | 17 => {
                    // TCP (6) or UDP (17): both have ports at the same offset
                    let src_port = u16::from_be_bytes([
                        buf[transport_header_start],
                        buf[transport_header_start + 1],
                    ]);
                    let dst_port = u16::from_be_bytes([
                        buf[transport_header_start + 2],
                        buf[transport_header_start + 3],
                    ]);

                    (src_port, dst_port)
                }
                _ => {
                    // Unknown protocol: no port extraction
                    warn!("Unknown protocol: {}, no ports are extracted", buf[9]);

                    (0, 0)
                }
            }
        } else {
            // Insufficient data for transport header
            warn!("Insufficient data for transport header, no ports are extracted.");

            (0, 0)
        };

        // packs complete 4-tuple into 128-bit flow_id without compression:
        // src_ip(32) + dst_ip(32) + src_port(16) + dst_port(16) + reserved(32)
        let flow_id = ((src_ip as u128) << 96)
            | ((dst_ip as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32);

        flow_id
    }
}
