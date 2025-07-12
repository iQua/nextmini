use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    thread::sleep(Duration::from_secs(15)); // Initial delay to ensure environment is ready
                                            // SOCKS5 proxy address
    let proxy_addr = "172.16.8.5:8081";

    // target server address
    let target_ip = Ipv4Addr::new(172, 16, 8, 8);
    let target_port = 8080u16;

    println!("Connecting to SOCKS5 proxy: {}", proxy_addr);

    let mut stream = TcpStream::connect(proxy_addr)?;

    // Try to disable Nagle's algorithm to avoid buffering issues
    stream.set_nodelay(true)?;

    // Step 1: Authentication handshake
    println!("Sending auth request");
    let auth_request = [0x05, 0x01, 0x00]; // version 5, 1 method, no authentication
    stream.write_all(&auth_request)?;
    stream.flush()?; // Make sure request is sent immediately

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

    // Add delay between authentication and connection request
    thread::sleep(Duration::from_millis(100));

    // Step 2: Connection request
    // Try sending request in parts to avoid any potential buffering issues
    println!("Sending connection request header");

    // First send the header
    let header = [
        0x05, // VER: SOCKS5
        0x01, // CMD: CONNECT
        0x00, // RSV: Reserved
        0x01, // ATYP: IPv4
    ];
    stream.write_all(&header)?;
    stream.flush()?;

    // Small delay between parts
    thread::sleep(Duration::from_millis(50));

    // Then send the address
    println!("Sending target address: {}", target_ip);
    stream.write_all(&target_ip.octets())?;
    stream.flush()?;

    // Small delay between parts
    thread::sleep(Duration::from_millis(50));

    // Finally send the port
    println!("Sending target port: {}", target_port);
    let port_bytes = [(target_port >> 8) as u8, (target_port & 0xff) as u8];
    stream.write_all(&port_bytes)?;
    stream.flush()?;

    println!(
        "Full connection request sent to target: {}:{}",
        target_ip, target_port
    );

    // reads connection response
    let mut connect_response = [0u8; 10]; // minimum response length

    // Add timeout to read to avoid hanging
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;

    match stream.read(&mut connect_response) {
        Ok(n) if n >= 2 => {
            println!(
                "Received response of {} bytes: {:02X?}",
                n,
                &connect_response[..n]
            );

            if connect_response[0] != 0x05 {
                println!("Invalid SOCKS version in response: {}", connect_response[0]);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid SOCKS version",
                ));
            }

            match connect_response[1] {
                0x00 => println!("Connection successful!"),
                0x01 => {
                    println!("General SOCKS server failure");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "General SOCKS server failure",
                    ));
                }
                0x02 => {
                    println!("Connection not allowed by ruleset");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection not allowed",
                    ));
                }
                0x03 => {
                    println!("Network unreachable");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Network unreachable",
                    ));
                }
                0x04 => {
                    println!("Host unreachable");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Host unreachable",
                    ));
                }
                0x05 => {
                    println!("Connection refused");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection refused",
                    ));
                }
                0x06 => {
                    println!("TTL expired");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "TTL expired",
                    ));
                }
                0x07 => {
                    println!("Command not supported");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Command not supported",
                    ));
                }
                0x08 => {
                    println!("Address type not supported");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Address type not supported",
                    ));
                }
                code => {
                    println!("Unknown error code: {}", code);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Unknown error {}", code),
                    ));
                }
            }
        }
        Ok(n) => {
            println!(
                "Received incomplete response: {} bytes: {:02X?}",
                n,
                &connect_response[..n]
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Incomplete response",
            ));
        }
        Err(e) => {
            println!("Failed to read response: {}", e);
            return Err(e);
        }
    }

    println!(
        "SOCKS5 tunnel established successfully to {}:{}",
        target_ip, target_port
    );

    let mut buffer = vec![0xAA; 655350];
    loop {
        match stream.write_all(&buffer) {
            Ok(()) => {
                // println!("Sent {} bytes", buffer.len());
            }
            Err(e) => {
                println!("Failed to write to stream: {}", e);
                break;
            }
        };
    }

    Ok(())
}
