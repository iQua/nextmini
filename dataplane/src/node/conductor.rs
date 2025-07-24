/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the controller interface actor, the processors actor, and the local
/// interface actor.
use tracing::info;

use nextmini_messages::Protocol;

use super::controller::reporter::ControllerReporterHandle;
use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::quic::QuicServer;
use crate::node::network::tcp::TcpServer;
use crate::node::network::tcp_max::TcpMaxServer;
use crate::node::processor::ProcessorHandle;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the reporter that allows the dataplane node to communicate with the controller
    reporter: ControllerReporterHandle,
}

impl Conductor {
    pub async fn new() -> Self {
        let mut config = LocalConfig::new();

        // connects the processors with its downstream local interface writers to send packets out
        let (controller_interface, reporter) = ControllerInterfaceHandle::new(config.clone()).await;

        config = controller_interface.config.clone();
        let processors = controller_interface.processors.clone();

        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone());
        processors.connect_local_interface(local_interface.clone());

        Conductor {
            config,
            local_interface,
            processors,
            reporter,
        }
    }

    pub async fn new_for_namespace(config: LocalConfig) -> Self {
        // connects the processors with its downstream local interface writers to send packets out
        let (controller_interface, reporter) = ControllerInterfaceHandle::new(config.clone()).await;

        let config = controller_interface.config.clone();
        let processors = controller_interface.processors.clone();

        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone());
        processors.connect_local_interface(local_interface.clone());

        Conductor {
            config,
            local_interface,
            processors,
            reporter,
        }
    }

    pub async fn run(&self) {
        self.start().await;

        // At this point, the conductor actor has finished normally
        info!("Nextmini is shutting down...");

        // handle the shutdown logic
        self.local_interface.shutdown().await;
    }

    /// Starts the server and, if needed, listens for incoming connections.
    pub async fn start(&self) {
        info!(
            "Starting Nextmini node {} on {}:{}...",
            self.config.node_id, self.config.private_network_addr, self.config.private_network_port
        );

        // starts listening with either TCP or QUIC on published ports (private and/or public)
        let public_port = self.config.public_network_port.clone();
        let private_port = self.config.private_network_port.clone();
        let max_server_port = self.config.max_server_port;

        match self.config.protocol {
            Protocol::Tcp => {
                // uses TcpMaxServer to handle the connections for max operating mode
                let mut tcp_max_server =
                    TcpMaxServer::new(self.config.clone(), self.processors.clone());

                // uses TcpServer to handle the connections for normal operating mode
                if public_port == private_port {
                    let mut tcp_server = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", max_server_port);
                    let tcp_server_addr = format!("{}:{}", "0.0.0.0", public_port);

                    tokio::select! {
                        _ = tcp_max_server.start_listening(&tcp_max_server_addr) => {},
                        _ = tcp_server.start_listening(&tcp_server_addr) => {},
                    }
                } else {
                    let mut tcp_server_public = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut tcp_server_private = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let tcp_server_public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let tcp_server_private_addr = format!("{}:{}", "0.0.0.0", private_port);
                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", max_server_port);

                    tokio::select! {
                        _ = tcp_server_public.start_listening(&tcp_server_public_addr) => {},
                        _ = tcp_server_private.start_listening(&tcp_server_private_addr) => {},
                        _ = tcp_max_server.start_listening(&tcp_max_server_addr) => {},
                    }
                }
            }
            Protocol::Quic => {
                if public_port == private_port {
                    let mut quic_server = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                } else {
                    let mut quic_server_public = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut quic_server_private = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let private_addr = format!("{}:{}", "0.0.0.0", private_port);

                    tokio::select! {
                        _ = quic_server_public.start_listening(&public_addr) => {},
                        _ = quic_server_private.start_listening(&private_addr) => {},
                    }
                }
            }
        }
    }
}
