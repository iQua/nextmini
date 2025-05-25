use std::io::Cursor;

use crate::dataplane::{FlowId, PacketBuf};
#[cfg(test)]
use crate::dataplane::RECEIVE_BUF_SIZE;
use byteorder::{BigEndian, ReadBytesExt};

#[derive(Debug)]
pub struct Packet {
    pub flow_id: FlowId,
    pub route_id: Option<usize>,  // Carries route_id
    pub packet_size: usize,
    pub buf: PacketBuf,
}

impl Packet {
    pub fn new(packet_size: usize, buf: PacketBuf) -> Self {
        Self {
            flow_id: Self::get_flow_id_from_buf(&buf, packet_size),
            route_id: None,  // No route_id at initialization
            packet_size,
            buf,
        }
    }

    // Set route_id for the packet
    pub fn set_route_id(&mut self, route_id: usize) {
        self.route_id = Some(route_id);
    }

    // Get route_id
    pub fn get_route_id(&self) -> Option<usize> {
        self.route_id
    }

    // Check if route_id is set
    pub fn has_route_id(&self) -> bool {
        self.route_id.is_some()
    }

    // Embed route_id into IP packet Options field
    pub fn embed_route_id_to_packet(&mut self) {
        if let Some(route_id) = self.route_id {
            self.embed_route_id_to_ip_options(route_id);
        }
    }

    // Extract route_id from IP packet Options field
    pub fn extract_route_id_from_packet(&mut self) {
        if let Some(route_id) = self.extract_route_id_from_ip_options() {
            self.route_id = Some(route_id);
        }
    }

    // Embed route_id into IPv4 Options field
    fn embed_route_id_to_ip_options(&mut self, route_id: usize) {
        // Check if it's an IPv4 packet
        if self.packet_size < 20 || self.buf[0] >> 4 != 4 {
            println!("DEBUG: Not IPv4 packet, cannot embed route_id");
            return;
        }

        let ihl = (self.buf[0] & 0x0F) as usize;
        let current_header_len = ihl * 4;
        
        // Check if there's already a route_id option
        if let Some(_) = self.find_route_id_option() {
            // Route ID already exists, update it
            self.update_route_id_option(route_id);
            return;
        }

        // Calculate space needed for new option: 2 bytes header + 4 bytes route_id = 6 bytes
        // Round up to 4-byte boundary with padding
        let option_len = 8; // 6 bytes + 2 bytes padding to align to 4-byte boundary
        let new_header_len = current_header_len + option_len;
        
        // Check if we have space in the buffer
        if new_header_len + (self.packet_size - current_header_len) > self.buf.len() {
            println!("DEBUG: Not enough buffer space to embed route_id");
            return;
        }

        // Move payload data to make room for options
        let payload_size = self.packet_size - current_header_len;
        if payload_size > 0 {
            // Move payload backward
            for i in (0..payload_size).rev() {
                self.buf[new_header_len + i] = self.buf[current_header_len + i];
            }
        }

        // Insert route_id option at the end of existing options
        let option_start = current_header_len;
        self.buf[option_start] = 0x94;     // Custom option type
        self.buf[option_start + 1] = 6;    // Option length (2 + 4)
        
        // Write route_id in big-endian format
        let route_id_bytes = (route_id as u32).to_be_bytes();
        for (i, &byte) in route_id_bytes.iter().enumerate() {
            self.buf[option_start + 2 + i] = byte;
        }
        
        // Add padding to align to 4-byte boundary
        self.buf[option_start + 6] = 0x00; // NOP padding
        self.buf[option_start + 7] = 0x00; // NOP padding

        // Update IHL field
        let new_ihl = new_header_len / 4;
        self.buf[0] = (self.buf[0] & 0xF0) | (new_ihl as u8);

        // Update total length
        let new_total_len = new_header_len + payload_size;
        self.packet_size = new_total_len;
        let total_len_bytes = (new_total_len as u16).to_be_bytes();
        self.buf[2] = total_len_bytes[0];
        self.buf[3] = total_len_bytes[1];

        // Recalculate and update header checksum
        self.update_ip_checksum();

        println!("DEBUG: Embedded route_id {} into IP options", route_id);
    }

    // Extract route_id from IPv4 Options field
    fn extract_route_id_from_ip_options(&self) -> Option<usize> {
        // Check if it's an IPv4 packet
        if self.packet_size < 20 || self.buf[0] >> 4 != 4 {
            return None;
        }

        if let Some(route_id) = self.find_route_id_option() {
            println!("DEBUG: Extracted route_id {} from IP options", route_id);
            return Some(route_id);
        }

        None
    }

    // Find route_id option in IP options field
    fn find_route_id_option(&self) -> Option<usize> {
        let ihl = (self.buf[0] & 0x0F) as usize;
        let header_len = ihl * 4;
        
        if header_len <= 20 {
            return None; // No options
        }

        let mut option_offset = 20; // Start of options
        
        while option_offset < header_len {
            let option_type = self.buf[option_offset];
            
            if option_type == 0x00 {
                // End of Options List
                break;
            } else if option_type == 0x01 {
                // No Operation
                option_offset += 1;
                continue;
            }
            
            // Variable length option
            if option_offset + 1 >= header_len {
                break;
            }
            
            let option_length = self.buf[option_offset + 1] as usize;
            
            if option_type == 0x94 && option_length == 6 {
                // Found our route_id option
                if option_offset + 6 <= header_len {
                    let route_id_bytes = [
                        self.buf[option_offset + 2],
                        self.buf[option_offset + 3],
                        self.buf[option_offset + 4],
                        self.buf[option_offset + 5],
                    ];
                    let route_id = u32::from_be_bytes(route_id_bytes) as usize;
                    return Some(route_id);
                }
            }
            
            option_offset += option_length;
        }
        
        None
    }

    // Update existing route_id option
    fn update_route_id_option(&mut self, route_id: usize) {
        let ihl = (self.buf[0] & 0x0F) as usize;
        let header_len = ihl * 4;
        
        if header_len <= 20 {
            return; // No options
        }

        let mut option_offset = 20; // Start of options
        
        while option_offset < header_len {
            let option_type = self.buf[option_offset];
            
            if option_type == 0x00 {
                break;
            } else if option_type == 0x01 {
                option_offset += 1;
                continue;
            }
            
            if option_offset + 1 >= header_len {
                break;
            }
            
            let option_length = self.buf[option_offset + 1] as usize;
            
            if option_type == 0x94 && option_length == 6 {
                // Found and update route_id option
                if option_offset + 6 <= header_len {
                    let route_id_bytes = (route_id as u32).to_be_bytes();
                    for (i, &byte) in route_id_bytes.iter().enumerate() {
                        self.buf[option_offset + 2 + i] = byte;
                    }
                    
                    // Recalculate header checksum
                    self.update_ip_checksum();
                    
                    println!("DEBUG: Updated route_id to {} in IP options", route_id);
                    return;
                }
            }
            
            option_offset += option_length;
        }
    }

    // Recalculate and update IPv4 header checksum
    fn update_ip_checksum(&mut self) {
        let ihl = (self.buf[0] & 0x0F) as usize;
        let header_len = ihl * 4;
        
        // Clear existing checksum
        self.buf[10] = 0;
        self.buf[11] = 0;
        
        // Calculate checksum
        let mut sum: u32 = 0;
        
        for i in (0..header_len).step_by(2) {
            if i + 1 < header_len {
                let word = ((self.buf[i] as u32) << 8) | (self.buf[i + 1] as u32);
                sum = sum.wrapping_add(word);
            }
        }
        
        // Add carry
        while sum >> 16 != 0 {
            sum = (sum & 0xFFFF) + (sum >> 16);
        }
        
        // One's complement
        let checksum = !sum as u16;
        let checksum_bytes = checksum.to_be_bytes();
        
        self.buf[10] = checksum_bytes[0];
        self.buf[11] = checksum_bytes[1];
    }

    fn get_flow_id_from_buf(buf: &PacketBuf, packet_size: usize) -> FlowId {
        // Validate packet size first
        if packet_size == 0 {
            println!("ERROR: Zero-length packet received, returning zero flow_id");
            return 0;
        }

        if packet_size > buf.len() {
            println!("ERROR: Packet size {} exceeds buffer length {}, using buffer length", 
                     packet_size, buf.len());
        }

        // Check if it's an IPv4 packet
        if packet_size < 20 || buf[0] >> 4 != 4 {
            // Non-IPv4 or insufficient data, using fallback calculation
            println!("DEBUG: Non-IPv4 packet detected - version: {}, size: {}", 
                     buf[0] >> 4, packet_size);
            
            // Fallback to old behavior for non-IPv4 packets - convert to 128-bit
            let mut cursor = Cursor::new(buf.get(12..20).unwrap_or(&[0; 8]));
            let old_flow_id = cursor.read_u64::<BigEndian>().unwrap_or(0);
            println!("DEBUG: Non-IPv4 packet, using fallback flow_id: {:#x}", old_flow_id as u128);
            return old_flow_id as u128;
        }

        // Extract source and destination IP addresses
        let src_ip = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let dst_ip = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

        println!("DEBUG: Packet IP info - src_ip: {}.{}.{}.{}, dst_ip: {}.{}.{}.{}",
            buf[12], buf[13], buf[14], buf[15],
            buf[16], buf[17], buf[18], buf[19]);

        // Extract IHL to determine start of transport header
        let ihl = (buf[0] & 0x0F) as usize;
        let transport_header_start = 4 * ihl;

        // Validate IHL
        if ihl < 5 || transport_header_start > packet_size {
            // Invalid IHL or insufficient packet size
            println!("DEBUG: Invalid IHL ({}) or insufficient packet size, using zero flow_id", ihl);
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
                    // Debug info
                    println!("DEBUG: Protocol: {}, src_port: {}, dst_port: {}", 
                        if buf[9] == 6 { "TCP" } else { "UDP" }, src_port, dst_port);
                    (src_port, dst_port)
                }
                _ => {
                    // Protocol - no port extraction
                    println!("DEBUG: Unknown protocol: {}, no ports extracted", buf[9]);
                    (0, 0) // Other protocols
                }
            }
        } else {
            // Insufficient data for transport header
            println!("DEBUG: Insufficient data for transport header, no ports extracted");
            (0, 0) // Not enough data
        };

        // Pack complete 4-tuple into 128-bit flow_id without compression:
        // src_ip(32) + dst_ip(32) + src_port(16) + dst_port(16) + reserved(32)
        let flow_id = ((src_ip as u128) << 96)
            | ((dst_ip as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32);

        println!("DEBUG: Created flow_id: {:#x} from src_ip: {}, dst_ip: {}, src_port: {}, dst_port: {}", 
            flow_id, src_ip, dst_ip, src_port, dst_port);

        flow_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embed_and_extract_route_id() {
        // Create a minimal IPv4 packet
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        
        // IPv4 header (20 bytes)
        buf[0] = 0x45;  // Version 4, IHL 5 (20 bytes)
        buf[1] = 0x00;  // DSCP/ECN
        buf[2] = 0x00;  // Total length (will be updated)
        buf[3] = 0x28;  // Total length = 40 bytes initially
        buf[9] = 0x06;  // Protocol TCP
        buf[12] = 10; buf[13] = 0; buf[14] = 0; buf[15] = 1;  // src IP
        buf[16] = 10; buf[17] = 0; buf[18] = 0; buf[19] = 4;  // dst IP
        
        // TCP header (20 bytes)
        buf[20] = 0x1f; buf[21] = 0x90;  // src port 8080
        buf[22] = 0x00; buf[23] = 0x50;  // dst port 80
        
        let mut packet = Packet::new(40, buf);
        
        // Test embedding route_id
        packet.set_route_id(12345);
        packet.embed_route_id_to_packet();
        
        // Verify packet structure changed
        assert!(packet.packet_size > 40);  // Should be larger due to options
        let ihl = (packet.buf[0] & 0x0F) as usize;
        assert!(ihl > 5);  // IHL should be larger than 5 (20 bytes)
        
        // Test extracting route_id
        let mut new_packet = Packet::new(packet.packet_size, packet.buf);
        new_packet.extract_route_id_from_packet();
        
        assert_eq!(new_packet.get_route_id(), Some(12345));
    }
    
    #[test]
    fn test_update_existing_route_id() {
        // Create a packet with route_id
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        buf[0] = 0x45;  // IPv4 header
        buf[2] = 0x00; buf[3] = 0x28;  // Total length = 40
        buf[9] = 0x06;  // TCP
        buf[12] = 10; buf[13] = 0; buf[14] = 0; buf[15] = 1;
        buf[16] = 10; buf[17] = 0; buf[18] = 0; buf[19] = 4;
        
        let mut packet = Packet::new(40, buf);
        
        // Embed first route_id
        packet.set_route_id(100);
        packet.embed_route_id_to_packet();
        let first_size = packet.packet_size;
        
        // Update to new route_id
        packet.set_route_id(200);
        packet.embed_route_id_to_packet();
        
        // Size should remain the same (no new option added)
        assert_eq!(packet.packet_size, first_size);
        
        // Extract and verify new route_id
        let mut test_packet = Packet::new(packet.packet_size, packet.buf);
        test_packet.extract_route_id_from_packet();
        assert_eq!(test_packet.get_route_id(), Some(200));
    }
    
    #[test]
    fn test_non_ipv4_packet() {
        // Create non-IPv4 packet
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        buf[0] = 0x60;  // IPv6 version
        
        let mut packet = Packet::new(40, buf);
        packet.set_route_id(123);
        packet.embed_route_id_to_packet();
        
        // Should not embed anything for non-IPv4
        assert_eq!(packet.packet_size, 40);  // Size unchanged
        
        let mut test_packet = Packet::new(40, buf);
        test_packet.extract_route_id_from_packet();
        assert_eq!(test_packet.get_route_id(), None);
    }
}
