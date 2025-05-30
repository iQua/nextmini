use tracing::{error, info};

use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
// use crate::node::context::Context;

pub async fn start_protocols_server(
    protocol: Protocol,
    configs: LocalConfig,
    // mut context: Context,
    // processor_manager: Arc<RwLock<ProcessorManager>>,
) {
    let public_port = configs.public_network_port.clone();
    let private_port = configs.private_network_port.clone();

    match protocol {
        Protocol::Udp => {
            context.start_udp_receiver().await;
        }
        Protocol::Tcp => {
            if public_port == private_port {
                let mut tcp_server = TcpServer::new(context.clone(), processor_manager.clone());
                tokio::spawn(async move {
                    tcp_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });

                return;
            }

            let mut tcp_server = TcpServer::new(context.clone(), processor_manager.clone());
            tokio::spawn(async move {
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            });

            let mut tcp_server = TcpServer::new(context.clone(), processor_manager.clone());
            tokio::spawn(async move {
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                    .await;
            });
        }
        Protocol::Quic => {
            if public_port == private_port {
                let mut quic_server =
                    QuicServer::new(context.clone(), configs.clone(), processor_manager.clone());
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });

                return;
            }

            let mut quic_server =
                QuicServer::new(context.clone(), configs.clone(), processor_manager.clone());

            tokio::spawn(async move {
                quic_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            });

            let mut quic_server =
                QuicServer::new(context.clone(), configs.clone(), processor_manager.clone());

            tokio::spawn(async move {
                quic_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                    .await;
            });
        }
    }
}
