/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use tokio::sync::mpsc;
use tracing::info;

use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::QuicServer;
use crate::node::tcp::TcpServer;
use crate::node::udp::UdpReader;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the controller interface actor, which communicates with the controller
    controller_interface: ControllerInterfaceHandle,

    /// used by the main tokio task to shutdown the conductor
    main_shutdown_recv: Option<mpsc::UnboundedReceiver<()>>,
}

impl Conductor {
    pub async fn new(main_shutdown_recv: mpsc::UnboundedReceiver<()>) -> Self {
        let config = LocalConfig::new();

        // starts the processor actor
        let processors = ProcessorHandle::new(config.clone());

        // starts the local interface actor, providing it with a handle of the processors
        let local_interface = LocalInterfaceHandle::new(config.clone(), processors.clone());

        // connects the processors with its downstream local interface writers to send packets out
        processors.connect_local_interface(local_interface.clone());

        // starts the controller interface actor
        let controller_interface =
            ControllerInterfaceHandle::new(config.clone(), processors.clone()).await;

        Conductor {
            config,
            local_interface,
            processors,
            controller_interface,
            main_shutdown_recv: Some(main_shutdown_recv),
        }
    }

    pub async fn run(&mut self) {
        let mut main_shutdown_recv = self.main_shutdown_recv.take().unwrap();

        tokio::select! {
            _ = self.start() => {
                // At this point, the conductor actor has finished normally
            }
            _ = main_shutdown_recv.recv() => {
                // handles the shutdown signal from the main tokio task
                self.shutdown().await;
            },
        }
    }

    /// Starts the server and, if needed, listens for incoming connections.
    pub async fn start(&self) {
        info!("Nextmini is starting...");

        let public_port = self.config.public_network_port.clone();
        let private_port = self.config.private_network_port.clone();

        match self.config.protocol {
            Protocol::Udp => {
                let mut udp_reader = UdpReader::new(self.config.clone(), self.processors.clone());
                tokio::spawn(async move {
                    udp_reader.start_listening(&format!("{}:{}", "0.0.0.0", public_port)).await;
                });
            }
            Protocol::Tcp => {
                if public_port == private_port {
                    let mut tcp_server =
                        TcpServer::new(self.config.clone(), self.processors.clone());
                    tcp_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                } else {
                    let mut tcp_server_public =
                        TcpServer::new(self.config.clone(), self.processors.clone());
                    let mut tcp_server_private =
                        TcpServer::new(self.config.clone(), self.processors.clone());

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
                        QuicServer::new(self.config.clone(), self.processors.clone());
                    tokio::spawn(async move {
                        quic_server
                            .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                            .await;
                    });
                } else {
                    let mut quic_server =
                        QuicServer::new(self.config.clone(), self.processors.clone());
                    tokio::spawn(async move {
                        quic_server
                            .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                            .await;
                    });

                    let mut quic_server =
                        QuicServer::new(self.config.clone(), self.processors.clone());
                    tokio::spawn(async move {
                        quic_server
                            .start_listening(&format!("{}:{}", "0.0.0.0", private_port))
                            .await;
                    });
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        info!("Nextmini is shutting down...");

        self.local_interface.shutdown().await;
    }
}
