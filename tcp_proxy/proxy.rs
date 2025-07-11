use std::io::{self, Ipv4Addr, Ipv6Addr, Read, Shutdown, TcpStream, Write};
use std::net::TcpListener;
use std::thread;

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:1080")?;
    println!("SOCKS5 proxy listening on 127.0.0.1:1080");
    for stream in listener.incoming() {
        let stream = stream?;
        thread::spawn(move || {
            if let Err(e) = handle_client(stream) {
                println!("Error handling client: {}", e);
            }
        });
    }
    Ok(())
}

fn handle_client(mut client: TcpStream) -> io::Result<()> {
    // Read version and number of methods
    let mut header = [0u8; 2];
    client.read_exact(&mut header)?;
    if header[0] != 5 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Not SOCKS5"));
    }
    let nmethods = header[1] as usize;
    let mut methods = vec![0u8; nmethods];
    client.read_exact(&mut methods)?;
    if !methods.contains(&0) {
        client.write_all(&[5, 0xff])?;
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No supported auth method",
        ));
    }
    client.write_all(&[5, 0])?;

    // Read request header
    let mut req_header = [0u8; 4];
    client.read_exact(&mut req_header)?;
    if req_header[0] != 5 {
        return Err(io::new(io::ErrorKind::InvalidData, "Not SOCKS5 request"));
    }
    let cmd = req_header[1];
    if cmd != 1 {
        // Only support CONNECT
        let mut reply = [5u8, 7, 0, 1, 0, 0, 0, 0, 0, 0];
        client.write_all(&reply)?;
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unsupported command",
        ));
    }
    let atyp = req_header[3];

    let target: String;
    match atyp {
        1 => {
            // IPv4
            let mut addr_bytes = [0u8; 4];
            client.read_exact(&mut addr_bytes)?;
            let ip = Ipv4Addr::from(addr_bytes);
            let mut port_bytes = [0u8; 2];
            client.read_exact(&mut port_bytes)?;
            let port = u16::from_be_bytes(port_bytes);
            target = format!("{}:}", ip, port);
        }
        3 => {
            // Domain
            let mut len_buf = [0u8; 1];
            client.read_exact(&mut len_buf)?;
            let len = len_buf[0] as usize;
            let mut host_bytes = vec![0u8; len];
            client.read_exact(&mut host_bytes)?;
            let host = String::from_utf8(host_bytes)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
            let mut port_bytes = [0u8; 2];
            client.read_exact(&mut port_bytes)?;
            let port = u16::from_be_bytes(port_bytes);
            target = format!("{}:{}", host, port);
        }
        4 => {
            // IPv6
            let mut addr_bytes = [0u8; 16];
            client.read_exact(&mut addr_bytes)?;
            let ip = Ipv6Addr::from(addr_bytes);
            let mut port_bytes = [0u8; 2];
            client.read_exact(&mut port_bytes)?;
            let port = u16::from_be_bytes(port_bytes);
            target = format!("[{}]:{}", ip, port);
        }
        _ => {
            let mut reply = [5u8, 8, 0, 0, 0, 0, 0, 0, 0, 0];
            client.write_all(&reply)?;
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported address type",
            ));
        }
    }

    // Connect to target
    let mut remote = TcpStream::connect(&target)?;

    // Send success reply (with dummy bound address)
    let reply = [5u8, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    client.write_all(&reply)?;

    // Forward data in both directions
    let mut client_to_remote = client.try_clone()?;
    let mut remote_to_client = remote.try_clone()?;
    thread::spawn(move || {
        if let Err(_) = forward(&mut client_to_remote, &mut remote_to_client) {
            // Ignore errors
        }
    });
    forward(&mut remote, &mut client)
}

fn forward(from: &mut TcpStream, to: &mut TcpStream) -> io::Result<()> {
    io::copy(from, to)?;
    to.shutdown(Shutdown::Write)
}
