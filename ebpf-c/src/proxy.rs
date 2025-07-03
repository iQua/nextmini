use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_client(mut client_stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    println!("Proxy: Handling client connection");
    
    // Read message from client
    let mut buffer = [0; 1024];
    let n = client_stream.read(&mut buffer).await?;
    let message = String::from_utf8_lossy(&buffer[..n]);
    println!("Proxy: Received from client: {}", message);
    
    // Forward message to server
    match TcpStream::connect("ebpf-server:8082").await {
        Ok(mut server_stream) => {
            // Send message to server
            server_stream.write_all(&buffer[..n]).await?;
            
            // Read response from server
            let mut response_buffer = [0; 1024];
            let response_n = server_stream.read(&mut response_buffer).await?;
            let response = String::from_utf8_lossy(&response_buffer[..response_n]);
            println!("Proxy: Received from server: {}", response);
            
            // Forward response back to client
            client_stream.write_all(&response_buffer[..response_n]).await?;
            
            println!("Proxy: Forwarded response to client");
        }
        Err(e) => {
            eprintln!("Proxy: Failed to connect to server: {}", e);
            let error_msg = "Proxy: Server connection failed";
            client_stream.write_all(error_msg.as_bytes()).await?;
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting proxy on port 8081");
    
    let listener = TcpListener::bind("0.0.0.0:8081").await?;
    println!("Proxy listening on port 8081");
    
    loop {
        match listener.accept().await {
            Ok((client_stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_client(client_stream).await {
                        eprintln!("Proxy: Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Proxy: Error accepting connection: {}", e);
            }
        }
    }
}