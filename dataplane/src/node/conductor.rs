/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the controller interface actor, the processor actor, and the local
/// interface actor.
use tokio::sync::mpsc;
use tracing::info;

use nextmini_messages::Protocol;

use super::reporter::ControllerReporterHandle;
use crate::node::config::LocalConfig;
use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::QuicServer;
use crate::node::tcp::TcpServer;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

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

        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone());
        processors.connect_local_interface(local_interface.clone());

        Conductor {
            config,
            local_interface,
            processors,
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

        match self.config.protocol {
            Protocol::Tcp => {
                if public_port == private_port {
                    let mut tcp_server = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    tcp_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
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
