use crate::node::PacketBuf;
use crate::node::config::CongestionControl;
use crate::node::config::LocalConfig;
// use crate::node::context::Context;
use s2n_quic::Server;
use s2n_quic::provider::congestion_controller;
use s2n_quic::stream::{ReceiveStream, SendStream};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

pub struct QuicServer {
    // context: Context,
    config: LocalConfig,
}

impl QuicServer {
    pub fn new(config: LocalConfig) -> Self {
        Self { config }
    }

    pub async fn start_listening(&mut self, addr: &str) {
        let server_addr: SocketAddr = addr.parse().unwrap();

        let mut server = match self.config.quic_congestion_control {
            CongestionControl::Cubic => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Cubic::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
            CongestionControl::Bbr => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Bbr::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
        };

        while let Some(mut connection) = server.accept().await {
            // let processor_manager = self.processor_manager.clone();
            // let context = self.context.clone();

            tokio::spawn(async move {
                info!("Connection accepted from {:?}.", connection.remote_addr());

                if let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let mut node_id_buf: [u8; 8] = [0; 8];

                    if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                        info!("Failed to read node ID: {}", e);
                        connection.close(0u32.into());
                        return;
                    }

                    let node_id = u64::from_be_bytes(node_id_buf) as usize;
                    info!("Incoming connection from node {}...", node_id);
                    // context.add_quic_node(node_id, stream).await;

                    // processor_manager.write().await.update_processors().await;

                    info!("Connected.");
                } else {
                    connection.close(0u32.into());
                }
            });
        }
    }
}

pub struct QuicReader {
    stream: ReceiveStream,
}

impl QuicReader {
    pub fn new(stream: ReceiveStream) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self, buf: &mut PacketBuf) -> usize {
        match self.stream.read_exact(&mut buf[0..4]).await {
            Ok(_) => (),
            Err(_) => {
                return 0;
            }
        }

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        match self.stream.read_exact(&mut buf[4..msg_len]).await {
            Ok(_) => (),
            Err(_) => {
                return 0;
            }
        }

        msg_len
    }
}

#[derive(Clone)]
pub struct QuicWriter {
    stream: Arc<Mutex<SendStream>>,
}

impl QuicWriter {
    pub fn new(stream: Arc<Mutex<SendStream>>) -> Self {
        Self {
            stream: stream.clone(),
        }
    }

    pub async fn write(&mut self, buf: &[u8]) {
        let mut stream_guard = self.stream.lock().await;

        match stream_guard.write_all(buf).await {
            Ok(_) => (),
            Err(e) => panic!("{e}"),
        };
    }
}
