use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use serde_json::Value;
use tracing::debug;

use crate::dataplane::{FlowId, PacketBuf};

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub packet_size: usize,
    // pub stream_id: SocketId,
    // pub has_stream_id: bool,
    pub buf: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, buf: PacketBuf) -> Self {
        // Validate IP packet before processing
        Self::debug_validate_packet(&buf, packet_size);
        
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf, packet_size),
            packet_size,
            // stream_id: (0, 0),
            // has_stream_id: false,
            buf,
        }
    }


    fn get_flow_id_from_buf(buf: &PacketBuf, packet_size: usize) -> FlowId {
        // Check if it's an IPv4 packet
        if packet_size < 20 || buf[0] >> 4 != 4 {
            debug!("FlowID: Non-IPv4 or insufficient data, using fallback calculation");
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
            debug!("FlowID: Invalid IHL {} or insufficient packet size", ihl);
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
                    debug!("FlowID: Extracted ports {}:{} -> {}:{}", 
                           Self::format_ip(src_ip), src_port, Self::format_ip(dst_ip), dst_port);
                    (src_port, dst_port)
                }
                _ => {
                    debug!("FlowID: Protocol {} - no port extraction", buf[9]);
                    (0, 0) // Other protocols
                }
            }
        } else {
            debug!("FlowID: Insufficient data for transport header (need {}, have {})", 
                   transport_header_start + 4, packet_size);
            (0, 0) // Not enough data
        };

        // Pack complete 4-tuple into 128-bit flow_id without compression:
        // src_ip(32) + dst_ip(32) + src_port(16) + dst_port(16) + reserved(32)
        let flow_id = ((src_ip as u128) << 96)
            | ((dst_ip as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32);

        debug!("FlowID: Generated 0x{:032x} for {}:{} -> {}:{}", 
               flow_id, Self::format_ip(src_ip), src_port, Self::format_ip(dst_ip), dst_port);

        flow_id
    }

    fn format_ip(ip: u32) -> String {
        format!("{}.{}.{}.{}", 
                (ip >> 24) & 0xFF, 
                (ip >> 16) & 0xFF, 
                (ip >> 8) & 0xFF, 
                ip & 0xFF)
    }

    fn debug_validate_packet(buf: &PacketBuf, packet_size: usize) {
        if packet_size == 0 {
            debug!("Packet: Empty packet detected");
            return;
        }

        if packet_size < 20 {
            debug!("Packet: Too small for IP header: {} bytes", packet_size);
            return;
        }

        let version = buf[0] >> 4;
        let ihl = buf[0] & 0x0F;
        let total_length = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        let protocol = buf[9];

        debug!("Packet: IPv{}, IHL={}, TotalLen={}, ActualSize={}, Protocol={}", 
               version, ihl, total_length, packet_size, protocol);

        if version != 4 {
            debug!("Packet: WARNING - Not IPv4 (version={})", version);
        }

        if ihl < 5 {
            debug!("Packet: WARNING - Invalid IHL: {}", ihl);
        }

        if total_length != packet_size {
            debug!("Packet: WARNING - Length mismatch: header={}, actual={}", 
                   total_length, packet_size);
        }

        if total_length < (ihl * 4) as usize {
            debug!("Packet: WARNING - Total length less than header length");
        }
    }



    // pub fn try_set_stream_id(&mut self) {
    //     // Check the minimum length for IPv4 header + TCP header
    //     if self.buf.len() < 20 + 4 {
    //         return;
    //     }
    //     // Check if it's an IPv4 packet
    //     if self.buf[0] >> 4 != 4 {
    //         return;
    //     }
    //     // Extract IHL value to determine start of TCP header
    //     let ihl = (self.buf[0] & 0x0F) as usize;
    //     let tcp_header_start = 4 * ihl;

    //     // Check if the next protocol is TCP (protocol number for TCP is 6)
    //     if self.buf[9] == 6 {
    //         // Check if we have enough data for the TCP header
    //         if self.buf.len() < tcp_header_start + 4 {
    //             return;
    //         }

    //         // Extract the source and destination ports
    //         let source_port =
    //             u16::from_be_bytes([self.buf[tcp_header_start], self.buf[tcp_header_start + 1]]);
    //         let dest_port = u16::from_be_bytes([
    //             self.buf[tcp_header_start + 2],
    //             self.buf[tcp_header_start + 3],
    //         ]);

    //         self.stream_id = (source_port, dest_port);
    //         self.has_stream_id = true;
    //     } else if self.buf[9] == 17 {
    //         // Check if the protocol is UDP (protocol number 17)
    //         // Calculate the start of the UDP header
    //         let udp_header_start = ihl * 4;

    //         // Extract the source and destination ports
    //         let source_port =
    //             u16::from_be_bytes([self.buf[udp_header_start], self.buf[udp_header_start + 1]]);
    //         let dest_port = u16::from_be_bytes([
    //             self.buf[udp_header_start + 2],
    //             self.buf[udp_header_start + 3],
    //         ]);

    //         self.stream_id = (source_port, dest_port);
    //         self.has_stream_id = true;
    //     }
    // }
}

pub fn json_byte_array_to_flow_id(flow_id: &Value) -> FlowId {
    // Handle both 8-byte (old format) and 16-byte (new format) arrays
    if let Some(array) = flow_id.as_array() {
        if array.len() == 16 {
            // New 16-byte format for 128-bit flow_id
            let mut bytes: [u8; 16] = [0; 16];
            for i in 0..16 {
                bytes[i] = array[i]
                    .as_u64()
                    .expect("Invalid flow_id field in JSON object, expected u8") as u8;
            }
            let mut cursor = Cursor::new(&bytes);
            cursor.read_u128::<BigEndian>().unwrap()
        } else if array.len() == 8 {
            // Legacy 8-byte format - convert to 128-bit
            let mut bytes: [u8; 8] = [0; 8];
            for i in 0..8 {
                bytes[i] = array[i]
                    .as_u64()
                    .expect("Invalid flow_id field in JSON object, expected u8") as u8;
            }
            let mut cursor = Cursor::new(&bytes);
            cursor.read_u64::<BigEndian>().unwrap() as u128
        } else {
            0u128
        }
    } else {
        0u128
    }
}
