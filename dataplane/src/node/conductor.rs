/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the controller interface actor, the processor actor, and the local
/// interface actor.
use tokio::sync::mpsc;
use tracing::info;

use nextmini_messages::Protocol;

use super::controller::reporter::ControllerReporterHandle;
use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::quic::QuicServer;
use crate::node::network::tcp::TcpServer;
use crate::node::processor::ProcessorHandle;
use crate::node::splice::connector::ConnectorHandle;
use crate::node::splice::tcp_max::TcpMaxServer;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the connector
    connector: ConnectorHandle,

    /// the reporter that allows the dataplane node to communicate with the controller
    reporter: ControllerReporterHandle,

    /// used by the main tokio task to shutdown the conductor
    main_shutdown_recv: Option<mpsc::UnboundedReceiver<()>>,
}

impl Conductor {
    pub async fn new(main_shutdown_recv: mpsc::UnboundedReceiver<()>) -> Self {
        let mut config = LocalConfig::new();

        // connects the processors with its downstream local interface writers to send packets out
        let (controller_interface, reporter) = ControllerInterfaceHandle::new(config.clone()).await;

        config = controller_interface.config.clone();
        let processors = controller_interface.processors.clone();
        let connector = controller_interface.connector.clone();
        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone());
        processors.connect_local_interface(local_interface.clone());

        Conductor {
            config,
            local_interface,
            processors,
            connector,
            reporter,
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
        info!(
            "Starting Nextmini node {} on {}:{}...",
            self.config.node_id, self.config.private_network_addr, self.config.private_network_port
        );

        // starts listening with either TCP or QUIC on published ports (private and/or public)
        let public_port = self.config.public_network_port.clone();
        let private_port = self.config.private_network_port.clone();
        let tcp_max_server_port = self.config.tcp_max_server_port.clone();

        match self.config.protocol {
            Protocol::Tcp => {
                // uses TcpMaxServer to handle the connections for max operating mode.
                let mut tcp_max_server = TcpMaxServer::new(self.config.clone(), self.connector.clone());

                // uses TcpServer to handle the connections for normal operating mode.
                if public_port == private_port {
                    let mut tcp_server = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    
                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", tcp_max_server_port);
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
                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", tcp_max_server_port);

                    tokio::select! {
                        _ = tcp_server_public.start_listening(&tcp_server_public_addr) => {},
                        _ = tcp_server_private.start_listening(&tcp_server_private_addr) => {},
                        _ = tcp_max_server.start_listening(&tcp_max_server_addr) => {},
                    }
                }
            },
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

    pub async fn shutdown(&self) {
        info!("Nextmini is shutting down...");

        self.local_interface.shutdown().await;
    }
}
