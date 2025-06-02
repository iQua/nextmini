use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::tcp::TcpServer;
use crate::node::quic::QuicServer;
pub async fn start_protocols_server(
    protocol: Protocol,
    configs: LocalConfig,
    processor_handle: ProcessorHandle,
) {
    let public_port = configs.public_network_port.clone();
    let private_port = configs.private_network_port.clone();

    match protocol {
        Protocol::Udp => {
            // UDP implementation not ready yet
        }
        Protocol::Tcp => {
            if public_port == private_port {
                let mut tcp_server = TcpServer::new(configs.clone(), processor_handle.clone());
                tcp_server
                    .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                    .await;
            } else {
                let mut tcp_server_public = TcpServer::new(configs.clone(), processor_handle.clone());
                let mut tcp_server_private = TcpServer::new(configs, processor_handle);
                
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
                let mut quic_server =
                    QuicServer::new(configs.clone(), processor_handle.clone());
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });
            }else{
                let mut quic_server =
                    QuicServer::new(configs.clone(), processor_handle.clone());
    
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                });
    
                let mut quic_server =
                    QuicServer::new(configs.clone(), processor_handle.clone());
    
                tokio::spawn(async move {
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                        .await;
                });
            }
        }
    }
}
