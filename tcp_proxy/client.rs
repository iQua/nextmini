use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    // Connect to the SOCKS5 proxy
    let mut proxy_stream = TcpStream::connect("127.0.0.1:1080")?;
    // SOCKS5 handshake: version 5, no authentication
    proxy_stream.write_all(&[5, 1, 0])?;
    let mut response = [0; 2];
    proxy_stream.read_exact(&mut response)?;
    if response != [5, 0] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "SOCKS5 handshake failed",
        ));
    }

    // SOCKS5 request: connect to 127.0.0.1:8080 (IPv4)
    let request = [
        5, 1, 0, 1, // Version 5, CONNECT, reserved, IPv4
        127, 0, 0, 1, // IP: 127.0.0.1
        31, 144, // Port: 8080 (31*256 + 144 = 8080)
    ];
    proxy_stream.write_all(&request)?;
    let mut reply = [0; 10];
    proxy_stream.read_exact(&mut reply)?;
    if reply[1] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "SOCKS5 connection failed",
        ));
    }

    // Send a test message to the server
    let message = b"Hello, Server!";
    proxy_stream.write_all(message)?;
    proxy_stream.flush()?;

    // Read response from the server
    let mut buffer = [0; 1024];
    let bytes_read = proxy_stream.read(&mut buffer)?;
    println!(
        "Received from server: {}",
        String::from_utf8_lossy(&buffer[..bytes_read])
    );

    Ok(())
}
