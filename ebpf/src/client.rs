use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};

pub struct TcpClient {
    proxy_addr: String,
    message: String,
}

impl TcpClient {
    pub fn new(proxy_addr: String) -> Self {
        Self {
            proxy_addr,
            message: "Hello World".to_string(),
        }
    }

    pub async fn run(&self) {
        tracing_subscriber::fmt::init();
        info!("Client starting..");

        let mut stream = match TcpStream::connect(&self.proxy_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to connect to proxy: {}", e);
                return;
            }
        };
        info!("Connected to proxy at {}", self.proxy_addr);

        // Send one message
        if let Err(e) = stream.write_all(self.message.as_bytes()).await {
            error!("Failed to send message: {}", e);
            return;
        }
        info!("Sent: {}", self.message);

        // Wait for one response
        let mut buffer = [0; 1024];
        if let Ok(n) = stream.read(&mut buffer).await {
            let response = String::from_utf8_lossy(&buffer[..n]);
            info!("Received: \" {} \" from server", response);
        }
    }
}

#[tokio::main]
async fn main() {
    let client = TcpClient::new("10.0.1.20:8080".to_string());
    client.run().await
}
