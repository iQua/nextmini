use std::io::Cursor;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use s2n_quic::Server;
use s2n_quic::provider::congestion_controller;
use tokio::net::TcpListener;
use tokio::{io::AsyncReadExt, sync::RwLock};
use tracing::{error, info};

use nextmini_messages::Protocol;

use crate::dataplane::configs::{CongestionControl, LocalConfigs};
use crate::dataplane::context::Context;
use crate::dataplane::processor::ProcessorManager;

pub async fn start_protocols_server(
    session_id: [u8; 4],
    protocol: Protocol,
    configs: LocalConfigs,
    mut context: Context,
    processor_manager: Arc<RwLock<ProcessorManager>>,
) {
    let public_port = configs.public_network_port.clone();
    let private_port = configs.private_network_port.clone();

    match protocol {
        Protocol::Udp => {
            context.start_udp_receiver().await;
        }
        Protocol::Tcp => {
            if public_port == private_port {
                let mut tcp_server =
                    TcpServer::new(context.clone(), session_id, processor_manager.clone());
                tokio::spawn(async move {
                    tcp_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });

                return;
            }

            let mut tcp_server =
                TcpServer::new(context.clone(), session_id, processor_manager.clone());
            tokio::spawn(async move {
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            });

            let mut tcp_server =
                TcpServer::new(context.clone(), session_id, processor_manager.clone());
            tokio::spawn(async move {
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                    .await;
            });
        }
        Protocol::Quic => {
            if public_port == private_port {
                let mut quic_server = QuicServer::new(
                    context.clone(),
                    session_id,
                    configs.clone(),
                    processor_manager.clone(),
                );
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });

                return;
            }

            let mut quic_server = QuicServer::new(
                context.clone(),
                session_id,
                configs.clone(),
                processor_manager.clone(),
            );

            tokio::spawn(async move {
                quic_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            });

            let mut quic_server = QuicServer::new(
                context.clone(),
                session_id,
                configs.clone(),
                processor_manager.clone(),
            );

            tokio::spawn(async move {
                quic_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                    .await;
            });
        }
    }
}

pub struct TcpServer {
    context: Context,
    session_id: [u8; 4],
    processor_manager: Arc<RwLock<ProcessorManager>>,
}

impl TcpServer {
    pub fn new(
        context: Context,
        session_id: [u8; 4],
        processor_manager: Arc<RwLock<ProcessorManager>>,
    ) -> Self {
        Self {
            context,
            session_id,
            processor_manager,
        }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut session_id_buf = [0; 4];
        let mut node_id_buf: [u8; 8] = [0; 8];

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    continue;
                }
            };

            if let Err(e) = stream.read_exact(&mut session_id_buf).await {
                error!("Failed to read session ID: {}", e);
                continue;
            }

            if session_id_buf != self.session_id {
                continue;
            }

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            print!("Incoming connection from node {}...", node_id);

            self.context.add_tcp_node(node_id, stream).await;
            self.processor_manager
                .write()
                .await
                .update_processors()
                .await;

            info!("Connected.")
        }
    }
}

pub struct QuicServer {
    context: Context,
    session_id: [u8; 4],
    processor_manager: Arc<RwLock<ProcessorManager>>,
    configs: LocalConfigs,
}

impl QuicServer {
    pub fn new(
        context: Context,
        session_id: [u8; 4],
        configs: LocalConfigs,
        processor_manager: Arc<RwLock<ProcessorManager>>,
    ) -> Self {
        Self {
            context,
            session_id,
            processor_manager,
            configs,
        }
    }

    pub async fn start_listening(&mut self, addr: &str) {
        let server_addr: SocketAddr = addr.parse().unwrap();

        let mut server = match self.configs.quic_congestion_control {
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
            let session_id = self.session_id;
            let processor_manager = self.processor_manager.clone();
            let context = self.context.clone();

            tokio::spawn(async move {
                info!("Connection accepted from {:?}.", connection.remote_addr());

                if let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let mut session_id_buf = [0; 4];

                    if let Err(e) = stream.read_exact(&mut session_id_buf).await {
                        info!("Failed to read session ID: {}", e);
                        connection.close(0u32.into());
                        return;
                    }

                    if session_id_buf != session_id {
                        info!(
                            "Invalid session id: {:?}, expected: {:?}",
                            session_id_buf, session_id
                        );
                        connection.close(0u32.into());
                        return;
                    }

                    let mut node_id_buf: [u8; 8] = [0; 8];

                    if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                        info!("Failed to read node ID: {}", e);
                        connection.close(0u32.into());
                        return;
                    }

                    let node_id = u64::from_be_bytes(node_id_buf) as usize;
                    info!("Incoming connection from node {}...", node_id);
                    context.add_quic_node(node_id, stream).await;

                    processor_manager.write().await.update_processors().await;

                    info!("Connected.");
                } else {
                    connection.close(0u32.into());
                }
            });
        }
    }
}
