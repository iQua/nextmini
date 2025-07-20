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
    println!("Client connected, sending response...");
    let response = "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nHello World";
    
    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("Failed to write to stream: {}", e);
    }
    
    if let Err(e) = stream.flush() {
        eprintln!("Failed to flush stream: {}", e);
    }
    println!("Response sent and stream flushed.");
}
