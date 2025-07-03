use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting client on port 8080");
    
    // Wait for proxy to be ready
    sleep(Duration::from_secs(2)).await;
    
    loop {
        match TcpStream::connect("ebpf-proxy:8081").await {
            Ok(mut stream) => {
                println!("Connected to proxy");
                
                // Send hello message
                let message = "Hello from client!";
                if let Err(e) = stream.write_all(message.as_bytes()).await {
                    eprintln!("Failed to send message: {}", e);
                    continue;
                }
                
                // Read response
                let mut buffer = [0; 1024];
                match stream.read(&mut buffer).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buffer[..n]);
                        println!("Received response: {}", response);
                    }
                    Err(e) => {
                        eprintln!("Failed to read response: {}", e);
                    }
                }
                
                // Close connection
                drop(stream);
                
                // Wait before next connection
                sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                eprintln!("Failed to connect to proxy: {}", e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}