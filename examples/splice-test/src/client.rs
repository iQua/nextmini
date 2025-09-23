use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    thread::sleep(Duration::from_secs(30)); // Initial delay to ensure environment is ready
                                            // SOCKS5 proxy address
    let proxy_addr = "172.16.8.5:8081";

    // target server address
    let target_ip = Ipv4Addr::new(172, 16, 8, 8);
    let target_port = 8080u16;

    println!("Connecting to SOCKS5 proxy: {}.", proxy_addr);

    let mut stream = TcpStream::connect(proxy_addr)?;

    // Try to disable Nagle's algorithm to avoid buffering issues
    stream.set_nodelay(true)?;

    // Step 1: Authentication handshake
    println!("Sending auth request.");
    let auth_request = [0x05, 0x01, 0x00]; // version 5, 1 method, no authentication
    stream.write_all(&auth_request)?;
    stream.flush()?; // Make sure request is sent immediately

    // reads server response
    let mut auth_response = [0u8; 2];
    stream.read_exact(&mut auth_response)?;

    if auth_response[0] != 0x05 {
        panic!("Invalid SOCKS version response: {}.", auth_response[0]);
    }

    if auth_response[1] != 0x00 {
        panic!("Authentication failed, method: {}.", auth_response[1]);
    }

    println!("Authentication successful.");

    // Add delay between authentication and connection request
    thread::sleep(Duration::from_millis(100));

    // Step 2: Connection request
    // Try sending request in parts to avoid any potential buffering issues
    println!("Sending connection request header.");

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
    println!("Sending target address: {}.", target_ip);
    stream.write_all(&target_ip.octets())?;
    stream.flush()?;

    // Small delay between parts
    thread::sleep(Duration::from_millis(50));

    // Finally send the port
    println!("Sending target port: {}.", target_port);
    let port_bytes = [(target_port >> 8) as u8, (target_port & 0xff) as u8];
    stream.write_all(&port_bytes)?;
    stream.flush()?;

    println!(
        "Full connection request sent to target: {}:{}.",
        target_ip, target_port
    );

    // reads connection response
    let mut connect_response = [0u8; 10]; // minimum response length

    // Add timeout to read to avoid hanging
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;

    match stream.read(&mut connect_response) {
        Ok(n) if n >= 2 => {
            println!(
                "Received response of {} bytes: {:02X?}.",
                n,
                &connect_response[..n]
            );

            if connect_response[0] != 0x05 {
                println!(
                    "Invalid SOCKS version in response: {}.",
                    connect_response[0]
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid SOCKS version.",
                ));
            }

            match connect_response[1] {
                0x00 => println!("Connection successful."),
                0x01 => {
                    println!("General SOCKS server failure.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "General SOCKS server failure.",
                    ));
                }
                0x02 => {
                    println!("Connection not allowed by ruleset.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection not allowed.",
                    ));
                }
                0x03 => {
                    println!("Network unreachable.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Network unreachable.",
                    ));
                }
                0x04 => {
                    println!("Host unreachable.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Host unreachable.",
                    ));
                }
                0x05 => {
                    println!("Connection refused.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection refused.",
                    ));
                }
                0x06 => {
                    println!("TTL expired.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "TTL expired.",
                    ));
                }
                0x07 => {
                    println!("Command not supported.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Command not supported.",
                    ));
                }
                0x08 => {
                    println!("Address type not supported.");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Address type not supported.",
                    ));
                }
                code => {
                    println!("Unknown error code: {}.", code);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Unknown error {}.", code),
                    ));
                }
            }
        }
        Ok(n) => {
            println!(
                "Received incomplete response: {} bytes: {:02X?}.",
                n,
                &connect_response[..n]
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Incomplete response.",
            ));
        }
        Err(e) => {
            println!("Failed to read response: {}.", e);
            return Err(e);
        }
    }

    println!(
        "SOCKS5 tunnel established successfully to {}:{}.",
        target_ip, target_port
    );

    let bytes_sent = Arc::new(AtomicU64::new(0));
    let stats_counter = Arc::clone(&bytes_sent);
    thread::spawn(move || {
        print_stats(stats_counter);
    });

    let data = vec![0xAA; 65536]; // 64KB buffer
    let mut total_sent = 0u64;

    loop {
        match stream.write_all(&data) {
            Ok(()) => {
                total_sent += data.len() as u64;
                bytes_sent.store(total_sent, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("error sending data: {}.", e);
                break;
            }
        }
    }

    Ok(())
}

fn print_stats(bytes_sent: Arc<AtomicU64>) {
    let mut last_bytes = 0u64;
    let mut last_time = Instant::now();

    loop {
        // logs every second
        thread::sleep(Duration::from_secs(1));

        let current_bytes = bytes_sent.load(Ordering::Relaxed);
        let current_time = Instant::now();

        let elapsed = current_time.duration_since(last_time).as_secs_f64();
        let bytes_diff = current_bytes - last_bytes;
        let throughput_gbps = (bytes_diff as f64 * 8.0) / (elapsed * 1000.0 * 1000.0 * 1000.0);

        println!(
            "Send Throughput: {:.2} Gbps, Total sent: {:.2} GB.",
            throughput_gbps,
            current_bytes as f64 / (1000.0 * 1000.0 * 1000.0)
        );

        last_bytes = current_bytes;
        last_time = current_time;
    }
}
