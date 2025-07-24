use std::io;

use log::{error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Simple TCP echo server that accepts a single address string (e.g. "0.0.0.0:8080")
/// and echoes back whatever bytes it receives on each connection.
pub struct TcpServer {
    address: String,
}

impl TcpServer {
    pub fn new(address: String) -> Self {
        Self { address }
    }

    /// Run the echo server indefinitely.  Every accepted connection is handled
    /// on its own Tokio task where all incoming bytes are read and immediately
    /// written back to the client.
    pub async fn run(self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.address).await?;
        info!("TCP echo server listening on: {}", self.address);

        loop {
            info!("waiting for new client connection");
            let (mut socket, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    error!("failed to accept socket; error = {}", e);
                    continue;
                }
            };

            info!("new client connection");
            tokio::spawn(async move {
                let mut buf = vec![0; 1024];

                // Continuously read from socket and write the same bytes back.
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(n) => {
                            if n == 0 {
                                info!("Client disconnected");
                                return;
                            }

                            info!("Read {} bytes from the socket", n);
                            n
                        }
                        Err(e) => {
                            error!("Failed to read data from socket: {}", e);
                            return;
                        }
                    };

                    if let Err(e) = socket.write_all(&buf[..n]).await {
                        error!("Failed to write data to socket: {}", e);
                        return;
                    }
                    info!("Wrote {} bytes to the socket", n);
                }
            });
        }
    }
}

/// Dispatch helper used by `main` – currently supports only the TCP echo server.
pub async fn execute(handler: String, addr: String) -> Result<(), Box<dyn std::error::Error>> {
    match handler.as_str() {
        "tcp-echo" => {
            let server = TcpServer::new(addr);
            server
                .run()
                .await
                .map_err(|e| format!("tcp-echo error: {}", e))?;
        }
        _ => {
            return Err(format!("unknown handler: {}", handler).into());
        }
    }

    Ok(())
} 