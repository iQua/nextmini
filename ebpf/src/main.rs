use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let proxy = TcpProxy::new("0.0.0.0:8080".to_string(), "10.0.1.10:8080".to_string());
    proxy.run().await
}

pub struct TcpProxy {
    bind_addr: String,
    server_addr: String,
}

impl TcpProxy {
    pub fn new(bind_addr: String, server_addr: String) -> Self {
        Self {
            bind_addr,
            server_addr,
        }
    }

    pub async fn run(&self) {
        tracing_subscriber::fmt::init();

        // Load the eBPF program

        // Create SockHash map

        // Attach eBPF program to the socket map
        
        // Accept client connection request
        let listener = TcpListener::bind(&self.bind_addr).await?;
        info!("Proxy listening on {}", self.bind_addr);

        let (client_stream, client_addr) = listener.accept().await?;
        info!("Client connected: {}", client_addr);

        // Connect to server
        let server_stream = TcpStream::connect(&self.server_addr).await?;
        info!("Connected to server: {}", self.server_addr);

        // Get raw fds
        let client_fd = client_stream.as_raw_fd() as u32;
        let server_fd = server_stream.as_raw_fd() as u32;

        // Insert remote ports into sockhash map
    }
}
