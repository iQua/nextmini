/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the controller interface actor, the processors actor, and the local
/// interface actor.
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;
use tracing::{info, warn};

use nextmini_messages::Protocol;

use super::controller::interface::ControllerInterfaceHandle;
use super::controller::reporter::ControllerReporterHandle;
use crate::node::config::LocalConfig;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::quic::QuicServer;
use crate::node::network::tcp::TcpServer;
use crate::node::network::tcp_max::TcpMaxServer;
use crate::node::network::udp::UdpServer;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{Command, ReliableHandle};
use crate::node::session::manager::{PendingReceiverKey, SessionManager};

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the reporter that allows the dataplane node to communicate with the controller
    reporter: ControllerReporterHandle,

    /// controller interface handle for sending custom messages upstream
    #[cfg(feature = "python-extension")]
    controller: ControllerInterfaceHandle,

    /// reliable session subsystem handle (initialized but not yet wired)
    #[cfg(feature = "python-extension")]
    reliable: ReliableHandle,
}

impl Conductor {
    pub async fn new(config: LocalConfig) -> Self {
        // Initialize reliable subsystem handle (command loop wiring to follow).
        let (reliable, rx) = ReliableHandle::new();

        // connects the processors with its downstream local interface writers to send packets out
        let (controller_interface, reporter, flowstats_reporter) =
            ControllerInterfaceHandle::new(config.clone(), reliable.clone()).await;

        let config = controller_interface.config.clone();
        let processors = controller_interface.processors.clone();

        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone(), flowstats_reporter);
        processors.connect_local_interface(local_interface.clone());
        processors.connect_reliable_handle(reliable.clone());

        {
            let processors_for_mgr = processors.clone();
            let mut_rx = rx;
            tokio::spawn(async move {
                let manager = Arc::new(AsyncMutex::new(SessionManager::new(processors_for_mgr)));
                let mut rx = mut_rx;
                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        Command::StartSender { cfg, reply } => {
                            let sid = cfg.common.session_id;
                            let mut guard = manager.lock().await;
                            let _ = guard.spawn_sender(cfg);
                            let _ = reply.send(sid);
                        }
                        Command::StartReceiver { cfg, reply } => {
                            let sid = cfg.common.session_id;
                            let mut guard = manager.lock().await;
                            let _ = guard.spawn_receiver(cfg);
                            let _ = reply.send(sid);
                        }
                        Command::StartReceiverPending { cfg, key, reply } => {
                            let mut guard = manager.lock().await;
                            guard.enqueue_pending_receiver(key, cfg, reply);
                        }
                        Command::Stop { session } => {
                            let mut guard = manager.lock().await;
                            guard.stop(session).await;
                        }
                        Command::Deliver { session, frame } => {
                            let dest_ip = frame.dest_ip;
                            let source_node_id = frame.source_node_id;
                            let (sender, pending_reply) = {
                                let mut guard = manager.lock().await;
                                if let Some(tx) = guard.input_sender(session) {
                                    (Some(tx), None)
                                } else if let (Some(dip), Some(src)) = (dest_ip, source_node_id) {
                                    if let Some((cfg, reply)) = guard.adopt_pending_receiver(
                                        PendingReceiverKey {
                                            dest_ip: dip,
                                            source_node_id: src,
                                        },
                                        session,
                                    ) {
                                        let _ = guard.spawn_receiver(cfg);
                                        (guard.input_sender(session), Some(reply))
                                    } else {
                                        (None, None)
                                    }
                                } else {
                                    (None, None)
                                }
                            };
                            if let Some(tx) = sender {
                                if tx.send(frame).await.is_err() {
                                    warn!(
                                        session_id = session,
                                        "Reliable runtime: receiver dropped inbound frame"
                                    );
                                }
                                if let Some(reply) = pending_reply {
                                    let _ = reply.send(session);
                                }
                            } else {
                                warn!(
                                    session_id = session,
                                    "Reliable runtime: no receiver for inbound frame"
                                );
                            }
                        }
                        Command::Wait { session, reply } => {
                            // Spawn a separate task to handle Wait so it doesn't block the main loop
                            let manager_clone = manager.clone();
                            tokio::spawn(async move {
                                // Take ownership of the task and await completion.
                                let handle = {
                                    let mut guard = manager_clone.lock().await;
                                    guard.take_task(session)
                                };
                                if let Some(handle) = handle {
                                    let _ = handle.await; // ignore join errors; treat as completion
                                    let mut guard = manager_clone.lock().await;
                                    guard.remove_inputs(session);
                                    drop(guard);
                                    let _ = reply.send(true);
                                } else {
                                    let _ = reply.send(false);
                                }
                            });
                        }
                        Command::AllocateSession { reply } => {
                            let mut guard = manager.lock().await;
                            let sid = guard.allocate_session_id();
                            let _ = reply.send(sid);
                        }
                        Command::SetTopologyReady { ready } => {
                            let mut guard = manager.lock().await;
                            guard.set_topology_ready(ready);
                        }
                    }
                }
                warn!("Reliable command loop terminated.");
            });
        }

        Conductor {
            config,
            local_interface,
            processors,
            reporter,
            #[cfg(feature = "python-extension")]
            controller: controller_interface,
            #[cfg(feature = "python-extension")]
            reliable,
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
                let mut tcp_max_server = TcpMaxServer::new(self.processors.clone());

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
            Protocol::Udp => {
                if public_port == private_port {
                    let mut udp_server = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let udp_addr = format!("{}:{}", "0.0.0.0", public_port);
                    udp_server.start_listening(&udp_addr).await;
                } else {
                    let mut udp_server_public = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut udp_server_private = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let private_addr = format!("{}:{}", "0.0.0.0", private_port);

                    tokio::select! {
                        _ = udp_server_public.start_listening(&public_addr) => {},
                        _ = udp_server_private.start_listening(&private_addr) => {},
                    }
                }
            }
        }
    }

    /// Returns a clone of the processor handle so external callers can attach additional interfaces.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn processor_handle(&self) -> ProcessorHandle {
        // Used by the optional `nextmini_py` extension to wire the in-process interface.
        self.processors.clone()
    }

    /// Exposes the loaded `LocalConfig`, useful when bridging with language bindings.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn local_config(&self) -> LocalConfig {
        // Consumed by `nextmini_py` to mirror dataplane configuration inside Python.
        self.config.clone()
    }

    /// Exposes a controller handle so bindings can emit DataplaneToController messages.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn controller_handle(&self) -> ControllerInterfaceHandle {
        self.controller.clone()
    }

    /// Returns a clone of the reliable handle for language bindings.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn reliable_handle(&self) -> ReliableHandle {
        self.reliable.clone()
    }
}
