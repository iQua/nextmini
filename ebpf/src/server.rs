use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub struct TcpServer {
    bind_addr: String,
}

impl TcpServer {
    pub fn new(bind_addr: String) -> Self {
        Self {
            bind_addr,
        }
    }

    pub async fn run(&self) {
        tracing_subscriber::fmt::init();
        let listener = TcpListener::bind(&self.bind_addr).await.unwrap();
        info!("Listening on {}", self.bind_addr);

        // Accept one connection
        match listener.accept().await {
            Ok((mut stream, client_addr)) => {
                info!("Client connected: {}", client_addr);
                handle_client(&mut stream).await;
                info!("Server finished processing request");
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_client(stream: &mut TcpStream) {
    let mut buffer = vec![0; 1024];

    // Read one message
    match stream.read(&mut buffer).await {
        Ok(0) => {
            info!("Client disconnected");
        }
        Ok(n) => {
            let message = String::from_utf8_lossy(&buffer[..n]);
            info!("Received: {}", message.trim());

            let response = String::from("Nice to meet you.");
            let _ = stream.write_all(response.as_bytes()).await.unwrap();
            info!("Sent response: {}", response);
        }
        Err(e) => {
            error!("Read error: {}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    let server = TcpServer::new("0.0.0.0:8080".to_string());
    server.run().await
}
