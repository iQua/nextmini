use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    println!("Server: Handling connection");
    
    // Read message
    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await?;
    let message = String::from_utf8_lossy(&buffer[..n]);
    println!("Server: Received message: {}", message);
    
    // Send response
    let response = "Hello from server!";
    stream.write_all(response.as_bytes()).await?;
    
    println!("Server: Sent response");
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server on port 8082");
    
    let listener = TcpListener::bind("0.0.0.0:8082").await?;
    println!("Server listening on port 8082");
    
    loop {
        match listener.accept().await {
            Ok((client_stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client_stream).await {
                        eprintln!("Server: Error handling connection: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Server: Error accepting connection: {}", e);
            }
        }
    }
}