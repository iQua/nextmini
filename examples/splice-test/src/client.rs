use std::io::{Read, Write};
use std::net::{TcpStream, Ipv4Addr};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // SOCKS5 proxy address
    let proxy_addr = "172.16.8.5:8081";
    
    // target server address
    let target_ip = Ipv4Addr::new(172, 16, 8, 8);
    let target_port = 8080u16;
    
    println!("Connecting to SOCKS5 proxy: {}", proxy_addr);
    
    let mut stream = TcpStream::connect(proxy_addr)?;
    
    let auth_request = [0x05, 0x01, 0x00]; // version 5, 1 method, no authentication
    stream.write_all(&auth_request)?;
    
    // reads server response
    let mut auth_response = [0u8; 2];
    stream.read_exact(&mut auth_response)?;
    
    if auth_response[0] != 0x05 {
        panic!("Invalid SOCKS version response: {}", auth_response[0]);
    }
    
    if auth_response[1] != 0x00 {
        panic!("Authentication failed, method: {}", auth_response[1]);
    }
    
    println!("Authentication successful");
    

    let mut connect_request = Vec::new();
    connect_request.push(0x05); // Version 5
    connect_request.push(0x01); // Command: CONNECT
    connect_request.push(0x00); // Reserved field
    connect_request.push(0x01); // Address type: IPv4
    
    // adds IPv4 address (4 bytes)
    connect_request.extend_from_slice(&target_ip.octets());
    
    // adds port (2 bytes, big endian)
    connect_request.push((target_port >> 8) as u8);
    connect_request.push((target_port & 0xff) as u8);
    
    stream.write_all(&connect_request)?;
    
    println!("Sent connection request to {}:{}", target_ip, target_port);
    
    // reads connection response
    let mut connect_response = [0u8; 10]; // minimum response length
    stream.read_exact(&mut connect_response)?;
    
    if connect_response[0] != 0x05 {
        panic!("Invalid SOCKS version in response: {}", connect_response[0]);
    }
    
    match connect_response[1] {
        0x00 => println!("Connection successful!"),
        0x01 => panic!("General SOCKS server failure"),
        0x02 => panic!("Connection not allowed by ruleset"),
        0x03 => panic!("Network unreachable"),
        0x04 => panic!("Host unreachable"),
        0x05 => panic!("Connection refused"),
        0x06 => panic!("TTL expired"),
        0x07 => panic!("Command not supported"),
        0x08 => panic!("Address type not supported"),
        code => panic!("Unknown error code: {}", code),
    }
    
    // continuously sends data packets through the proxy
    let payload = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]; // sample fixed payload
    let mut buffer = [0u8; 1024];
    
    println!("Starting to send data packets in a loop...");
    
    loop {
        // just sends the raw payload
        stream.write_all(&payload)?;
        
        // reads any responses
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                println!("Received {} bytes", n);
            },
            Ok(0) => {
                println!("Connection closed by server");
                break;
            },
            Err(e) => {
                println!("Error reading from server: {}", e);
                break;
            },
            _ => {}
        }
    }
    
    Ok(())
}
