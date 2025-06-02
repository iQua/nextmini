use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::QuicServer;
use crate::node::tcp::TcpServer;

pub async fn start_server(config: LocalConfig, processor_handle: ProcessorHandle) {
    let public_port = config.public_network_port.clone();
    let private_port = config.private_network_port.clone();

    match config.protocol {
        Protocol::Udp => {
            // UDP implementation not ready yet
        }
        Protocol::Tcp => {
            if public_port == private_port {
                let mut tcp_server = TcpServer::new(config.clone(), processor_handle.clone());
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            } else {
                let mut tcp_server_public =
                    TcpServer::new(config.clone(), processor_handle.clone());
                let mut tcp_server_private = TcpServer::new(config, processor_handle);

                let public_addr = format!("{}:{}", "0.0.0.0", public_port);
                let private_addr = format!("{}:{}", "0.0.0.0", private_port);

                tokio::select! {
                    _ = tcp_server_public.start_listening(&public_addr) => {},
                    _ = tcp_server_private.start_listening(&private_addr) => {},
                }
            }
        }
        Protocol::Quic => {
            if public_port == private_port {
                let mut quic_server = QuicServer::new(config.clone(), processor_handle.clone());
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });
            } else {
                let mut quic_server = QuicServer::new(config.clone(), processor_handle.clone());

                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });

                let mut quic_server = QuicServer::new(config.clone(), processor_handle.clone());

                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                        .await;
                });
            }
        }
    }
}
