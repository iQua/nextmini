#[cfg(test)]
mod tests {
    use crate::dataplane::packet::Packet;
    
    const RECEIVE_BUF_SIZE: usize = 6404; // MAX_MTU (6400) + 4

    fn create_ipv4_packet(src_ip: [u8; 4], dst_ip: [u8; 4], src_port: u16, dst_port: u16, protocol: u8) -> [u8; RECEIVE_BUF_SIZE] {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        
        // IPv4 header (20 bytes minimum)
        buf[0] = 0x45; // Version (4) + IHL (5) - 20 byte header
        buf[1] = 0x00; // Type of Service
        buf[2] = 0x00; // Total Length (high)
        buf[3] = 0x28; // Total Length (low) - 40 bytes total
        buf[4] = 0x00; // Identification (high)
        buf[5] = 0x00; // Identification (low)
        buf[6] = 0x40; // Flags + Fragment Offset (high)
        buf[7] = 0x00; // Fragment Offset (low)
        buf[8] = 0x40; // TTL
        buf[9] = protocol; // Protocol
        buf[10] = 0x00; // Header Checksum (high)
        buf[11] = 0x00; // Header Checksum (low)
        
        // Source IP (bytes 12-15)
        buf[12..16].copy_from_slice(&src_ip);
        
        // Destination IP (bytes 16-19)
        buf[16..20].copy_from_slice(&dst_ip);
        
        // Transport header (TCP/UDP ports at bytes 20-23)
        buf[20] = (src_port >> 8) as u8;
        buf[21] = (src_port & 0xFF) as u8;
        buf[22] = (dst_port >> 8) as u8;
        buf[23] = (dst_port & 0xFF) as u8;
        
        buf
    }

    fn create_ipv6_packet(src_ip: [u8; 16], dst_ip: [u8; 16], src_port: u16, dst_port: u16, next_header: u8) -> [u8; RECEIVE_BUF_SIZE] {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        
        // IPv6 header (40 bytes)
        buf[0] = 0x60; // Version (6) + Traffic Class (high 4 bits)
        buf[1] = 0x00; // Traffic Class (low 4 bits) + Flow Label (high 4 bits)
        buf[2] = 0x00; // Flow Label (middle 8 bits)
        buf[3] = 0x00; // Flow Label (low 8 bits)
        buf[4] = 0x00; // Payload Length (high)
        buf[5] = 0x08; // Payload Length (low) - 8 bytes payload
        buf[6] = next_header; // Next Header
        buf[7] = 0x40; // Hop Limit
        
        // Source IP (bytes 8-23)
        buf[8..24].copy_from_slice(&src_ip);
        
        // Destination IP (bytes 24-39)
        buf[24..40].copy_from_slice(&dst_ip);
        
        // Transport header (TCP/UDP ports at bytes 40-43)
        buf[40] = (src_port >> 8) as u8;
        buf[41] = (src_port & 0xFF) as u8;
        buf[42] = (dst_port >> 8) as u8;
        buf[43] = (dst_port & 0xFF) as u8;
        
        buf
    }

    #[test]
    fn test_ipv4_tcp_flow_id() {
        let src_ip = [192, 168, 1, 1];
        let dst_ip = [10, 0, 0, 1];
        let src_port = 12345u16;
        let dst_port = 80u16;
        
        let buf = create_ipv4_packet(src_ip, dst_ip, src_port, dst_port, 6); // TCP
        let packet = Packet::new(40, buf);
        
        // Should generate a non-zero flow ID for valid IPv4 TCP packet
        assert_ne!(packet.flow_id, 0);
        
        // Test that the same parameters generate the same flow ID
        let buf2 = create_ipv4_packet(src_ip, dst_ip, src_port, dst_port, 6);
        let packet2 = Packet::new(40, buf2);
        assert_eq!(packet.flow_id, packet2.flow_id);
        
        // Test that different parameters generate different flow IDs
        let buf3 = create_ipv4_packet(src_ip, dst_ip, src_port + 1, dst_port, 6);
        let packet3 = Packet::new(40, buf3);
        assert_ne!(packet.flow_id, packet3.flow_id);
    }

    #[test]
    fn test_ipv4_udp_flow_id() {
        let src_ip = [172, 16, 0, 1];
        let dst_ip = [8, 8, 8, 8];
        let src_port = 53535u16;
        let dst_port = 53u16;
        
        let buf = create_ipv4_packet(src_ip, dst_ip, src_port, dst_port, 17); // UDP
        let packet = Packet::new(40, buf);
        
        // Should generate a non-zero flow ID for valid IPv4 UDP packet
        assert_ne!(packet.flow_id, 0);
    }

    #[test]
    fn test_ipv6_tcp_flow_id() {
        let src_ip = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 
                      0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34];
        let dst_ip = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00,
                      0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x35];
        let src_port = 12345u16;
        let dst_port = 80u16;
        
        let buf = create_ipv6_packet(src_ip, dst_ip, src_port, dst_port, 6); // TCP
        let packet = Packet::new(48, buf);
        
        // Should generate a non-zero flow ID for valid IPv6 TCP packet
        assert_ne!(packet.flow_id, 0);
        
        // Test that the same parameters generate the same flow ID
        let buf2 = create_ipv6_packet(src_ip, dst_ip, src_port, dst_port, 6);
        let packet2 = Packet::new(48, buf2);
        assert_eq!(packet.flow_id, packet2.flow_id);
        
        // Test that different parameters generate different flow IDs
        let buf3 = create_ipv6_packet(src_ip, dst_ip, src_port + 1, dst_port, 6);
        let packet3 = Packet::new(48, buf3);
        assert_ne!(packet.flow_id, packet3.flow_id);
    }

    #[test]
    fn test_ipv6_udp_flow_id() {
        let src_ip = [0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                      0x02, 0x00, 0x5e, 0x10, 0x00, 0x00, 0x00, 0x01];
        let dst_ip = [0xff, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let src_port = 5353u16;
        let dst_port = 5353u16;
        
        let buf = create_ipv6_packet(src_ip, dst_ip, src_port, dst_port, 17); // UDP
        let packet = Packet::new(48, buf);
        
        // Should generate a non-zero flow ID for valid IPv6 UDP packet
        assert_ne!(packet.flow_id, 0);
    }

    #[test]
    fn test_ipv4_unknown_protocol() {
        let src_ip = [192, 168, 1, 1];
        let dst_ip = [10, 0, 0, 1];
        let src_port = 0u16;
        let dst_port = 0u16;
        
        let buf = create_ipv4_packet(src_ip, dst_ip, src_port, dst_port, 1); // ICMP
        let packet = Packet::new(40, buf);
        
        // Should still generate a flow ID (with zero ports) for unknown protocols
        assert_ne!(packet.flow_id, 0);
    }

    #[test]
    fn test_ipv6_unknown_protocol() {
        let src_ip = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 
                      0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34];
        let dst_ip = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00,
                      0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x35];
        let src_port = 0u16;
        let dst_port = 0u16;
        
        let buf = create_ipv6_packet(src_ip, dst_ip, src_port, dst_port, 58); // ICMPv6
        let packet = Packet::new(48, buf);
        
        // Should still generate a flow ID (with zero ports) for unknown protocols
        assert_ne!(packet.flow_id, 0);
    }

    #[test]
    fn test_invalid_packets() {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        
        // Test zero-length packet
        let packet = Packet::new(0, buf);
        assert_eq!(packet.flow_id, 0);
        
        // Test IPv4 packet too small
        buf[0] = 0x45; // IPv4
        let packet = Packet::new(15, buf);
        assert_eq!(packet.flow_id, 0);
        
        // Test IPv6 packet too small
        buf[0] = 0x60; // IPv6
        let packet = Packet::new(30, buf);
        assert_eq!(packet.flow_id, 0);
        
        // Test unsupported IP version
        buf[0] = 0x30; // IP version 3 (invalid)
        let packet = Packet::new(40, buf);
        assert_eq!(packet.flow_id, 0);
    }

    #[test]
    fn test_ipv4_variable_header_length() {
        let src_ip = [192, 168, 1, 1];
        let dst_ip = [10, 0, 0, 1];
        let src_port = 12345u16;
        let dst_port = 80u16;
        
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        
        // IPv4 header with options (IHL = 6, 24-byte header)
        buf[0] = 0x46; // Version (4) + IHL (6) - 24 byte header
        buf[1] = 0x00; // Type of Service
        buf[8] = 0x40; // TTL
        buf[9] = 6;    // Protocol (TCP)
        
        // Source and Destination IPs
        buf[12..16].copy_from_slice(&src_ip);
        buf[16..20].copy_from_slice(&dst_ip);
        
        // Add some options (4 bytes)
        buf[20] = 0x01; // Option
        buf[21] = 0x02; // Option
        buf[22] = 0x03; // Option
        buf[23] = 0x04; // Option
        
        // Transport header starts at byte 24 due to IHL=6
        buf[24] = (src_port >> 8) as u8;
        buf[25] = (src_port & 0xFF) as u8;
        buf[26] = (dst_port >> 8) as u8;
        buf[27] = (dst_port & 0xFF) as u8;
        
        let packet = Packet::new(44, buf);
        
        // Should generate a non-zero flow ID even with variable header length
        assert_ne!(packet.flow_id, 0);
    }

    #[test]
    fn test_flow_id_consistency() {
        // Test that IPv4 and IPv6 generate different flow IDs even with same ports
        let src_port = 12345u16;
        let dst_port = 80u16;
        
        let ipv4_buf = create_ipv4_packet([192, 168, 1, 1], [10, 0, 0, 1], src_port, dst_port, 6);
        let ipv4_packet = Packet::new(40, ipv4_buf);
        
        let ipv6_src = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 
                        0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34];
        let ipv6_dst = [0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00,
                        0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x35];
        let ipv6_buf = create_ipv6_packet(ipv6_src, ipv6_dst, src_port, dst_port, 6);
        let ipv6_packet = Packet::new(48, ipv6_buf);
        
        // IPv4 and IPv6 packets should generate different flow IDs
        assert_ne!(ipv4_packet.flow_id, ipv6_packet.flow_id);
        
        // Both should be non-zero
        assert_ne!(ipv4_packet.flow_id, 0);
        assert_ne!(ipv6_packet.flow_id, 0);
    }
}