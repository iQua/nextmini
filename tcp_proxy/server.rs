use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    println!("HTTP server listening on 127.0.0.1:8080");
    for stream in listener.incoming() {
        let stream = stream?;
        std::thread::spawn(|| {
            if let Err(e) = handle_client(stream) {
                println!("Error handling client: {}", e);
            }
        });
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    if bytes_read == 0 {
        return Ok(());
    }

    // Convert buffer to string for HTTP request detection
    let request = String::from_utf8_lossy(&buffer[..bytes_read]);

    // Check if the request starts with a valid HTTP method (e.g., GET)
    if request.starts_with("GET ") {
        // Simple HTTP response
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/html\r\n",
            "Connection: close\r\n",
            "\r\n",
            "<html><body><h1>Hello from Rust HTTP Server!</h1></body></html>\r\n"
        );
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
    } else {
        // Echo non-HTTP data for compatibility with client.rs
        stream.write_all(&buffer[..bytes_read])?;
        stream.flush()?;
    }

    Ok(())
}
