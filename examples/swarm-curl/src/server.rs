use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    println!("Server listening on port 8080");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0u8; 1024];

    if let Ok(n) = stream.read(&mut buffer) {
        if n > 0 {
            let response = "HTTP/1.1 200 OK\r\n\r\nHello World";
            let _ = stream.write_all(response.as_bytes());
        }
    }
}
