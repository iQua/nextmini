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
        let listener = TcpListener::bind(&self.bind_addr).await.unwrap();
        info!("Listening on {}", self.bind_addr);

        // Accept one connection
        match listener.accept().await {
            Ok((client_stream, client_addr)) => {
                info!("Client connected: {}", client_addr);
                handle_client(client_stream, client_addr, self.server_addr.clone()).await;
                info!("Proxy finished processing request");
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_client(
    mut client_stream: TcpStream,
    _client_addr: SocketAddr,
    server_addr: String,
) {
    let mut server_stream = match TcpStream::connect(&server_addr).await {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to connect to server: {}", e);
            return;
        }
    };
    info!("Connected to server: {}", server_addr);

    let mut buffer = vec![0; 1024];

    // Read one message from client
    match client_stream.read(&mut buffer).await {
        Ok(0) => {
            info!("Client disconnected");
            return;
        }
        Ok(n) => {
            let data = &buffer[..n];
            let message = String::from_utf8_lossy(data);
            info!("Client -> Server: {}", message.trim());

            // Forward to server and get response
            match send_to_server(data, &mut server_stream).await {
                Some(response_data) => {
                    let response = String::from_utf8_lossy(&response_data);
                    info!("Server -> Client: {}", response.trim());
                    
                    let _ = client_stream.write_all(&response_data).await.unwrap();
                    info!("Response forwarded to client");
                }
                None => {
                    error!("Server communication error");
                }
            }
        }
        Err(e) => {
            error!("Client read error: {}", e);
        }
    }
}

async fn send_to_server(
    data: &[u8],
    server_stream: &mut TcpStream,
) -> Option<Vec<u8>> {
    if let Err(e) = server_stream.write_all(data).await {
        error!("Server write error: {}", e);
        return None;
    }

    let mut response_buffer = vec![0; 1024];
    match server_stream.read(&mut response_buffer).await {
        Ok(response_len) => Some(response_buffer[..response_len].to_vec()),
        Err(e) => {
            error!("Server read error: {}", e);
            None
        }
    }
}
