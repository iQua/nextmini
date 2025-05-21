use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use serde_json::Value;

use crate::dataplane::{FlowId, PacketBuf, SocketId};

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub packet_size: usize,
    pub stream_id: SocketId,
    pub has_stream_id: bool,
    pub buf: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, buf: PacketBuf) -> Self {
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf),
            packet_size,
            stream_id: (0, 0),
            has_stream_id: false,
            buf,
        }
    }

    fn get_flow_id_from_buf(buf: &PacketBuf) -> FlowId {
        let mut cursor = Cursor::new(buf.get(12..20).unwrap());

        cursor.read_u64::<BigEndian>().unwrap()
    }

    pub fn update_route(&mut self, path_id: u8) {
        self.buf[14] = path_id;
        self.flow_id += (path_id as u64) << 40;
    }

    pub fn try_set_stream_id(&mut self) {
        // Check the minimum length for IPv4 header + TCP header
        if self.buf.len() < 20 + 4 {
            return;
        }
        // Check if it's an IPv4 packet
        if self.buf[0] >> 4 != 4 {
            return;
        }
        // Extract IHL value to determine start of TCP header
        let ihl = (self.buf[0] & 0x0F) as usize;
        let tcp_header_start = 4 * ihl;

        // Check if the next protocol is TCP (protocol number for TCP is 6)
        if self.buf[9] == 6 {
            // Check if we have enough data for the TCP header
            if self.buf.len() < tcp_header_start + 4 {
                return;
            }

            // Extract the source and destination ports
            let source_port =
                u16::from_be_bytes([self.buf[tcp_header_start], self.buf[tcp_header_start + 1]]);
            let dest_port = u16::from_be_bytes([
                self.buf[tcp_header_start + 2],
                self.buf[tcp_header_start + 3],
            ]);

            self.stream_id = (source_port, dest_port);
            self.has_stream_id = true;
        } else if self.buf[9] == 17 {
            // Check if the protocol is UDP (protocol number 17)
            // Calculate the start of the UDP header
            let udp_header_start = ihl * 4;

            // Extract the source and destination ports
            let source_port =
                u16::from_be_bytes([self.buf[udp_header_start], self.buf[udp_header_start + 1]]);
            let dest_port = u16::from_be_bytes([
                self.buf[udp_header_start + 2],
                self.buf[udp_header_start + 3],
            ]);

            self.stream_id = (source_port, dest_port);
            self.has_stream_id = true;
        }
    }
}

pub fn json_byte_array_to_flow_id(flow_id: &Value) -> FlowId {
    let mut bytes: [u8; 8] = [0; 8];

    for i in 0..8 {
        bytes[i] = flow_id[i]
            .as_u64()
            .expect("Invalid flow_id field in JSON object, expected u8") as u8;
    }

    let mut cursor = Cursor::new(&bytes);

    cursor.read_u64::<BigEndian>().unwrap()
}
