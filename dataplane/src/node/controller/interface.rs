use std::collections::HashMap;
use std::net::Ipv4Addr;
#[cfg(feature = "python-extension")]
use std::sync::Arc;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use tokio::net::TcpStream;
#[cfg(feature = "python-extension")]
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};
use tracing::{error, info, warn};

use nextmini_messages::{
    ControllerToDataplane, DataplaneToController, Flow, FlowTransport, GroupId,
};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::client::UserSpaceClientHandle;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::processor::ProcessorHandle;
#[cfg(feature = "python-extension")]
use crate::node::python::interface::{PythonEvent, PythonInterfaceHandle};
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::session::api::ReliableRuntimeHandle;
use crate::node::session::unicast::ReliableUnicastFlowHandle;

#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    pub config: LocalConfig,
    pub processors: ProcessorHandle,
    northbridge_sender: mpsc::UnboundedSender<DataplaneToController>,
    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,
}

/// The handle for the controller interface, which allows sending messages to the controller.
impl ControllerInterfaceHandle {
    pub async fn new(
        config: LocalConfig,
    ) -> (
        Self,
        ReliableRuntimeHandle,
        ControllerReporterHandle,
        FlowStatsReporterHandle,
    ) {
        // creates an unbounded channel, the 'northbridge', for sending messages to the controller
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();

        // connects to the controller over WebSockets
        let (config, processors, ws_stream) = ControllerInterfaceHandle::connect(config).await;

        let (sender_stream, receiver_stream) = ws_stream.split();

        // initializes the controller sender and receiver
        let mut controller_sender = DataplaneToControllerSender {
            sender_stream,
            northbridge_receiver,
        };

        #[cfg(feature = "python-extension")]
        let python_interface = Arc::new(Mutex::new(None));

        let controller_interface = Self {
            config: config.clone(),
            processors: processors.clone(),
            northbridge_sender,
            #[cfg(feature = "python-extension")]
            python_interface: python_interface.clone(),
        };

        let reporter = ControllerReporterHandle::new(controller_interface.clone());

        let flowstats_reporter =
            FlowStatsReporterHandle::new(controller_interface.clone(), config.clone());

        // passes flowstats reporter to routing table for automatic route assignment reporting
        processors
            .set_flowstats_reporter(flowstats_reporter.clone())
            .await;

        // adds flowstats reporter to report flow finish
        let user_space_client = UserSpaceClientHandle::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
        );

        // creates the server handle for the processor to use
        let user_space_server = UserSpaceServerHandle::new(config.clone(), processors.clone());
        processors.connect_server(user_space_server.clone());

        // creates a TCP max client for the processor to use
        let tcp_max_client =
            TcpMaxClient::new(config.clone(), processors.clone(), reporter.clone());
        processors.connect_tcp_max_client(tcp_max_client).await;

        // Creates the reliable runtime actor with the correct processors
        let reliable_runtime = ReliableRuntimeHandle::new(processors.clone());
        processors.connect_reliable_handle(reliable_runtime.clone());

        let reliable_unicast = ReliableUnicastFlowHandle::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
            reliable_runtime.clone(),
        );

        let mut controller_receiver = ControllerToDataplaneReceiver {
            controller: controller_interface.clone(),
            config: config.clone(),
            receiver_stream,
            processors: processors.clone(),
            reporter: reporter.clone(),
            user_space_client,
            user_space_server,
            #[cfg(feature = "python-extension")]
            python_interface,
            reliable_runtime: reliable_runtime.clone(),
            group_ip_by_id: HashMap::new(),
            reliable_unicast,
            topology_ready: false,
            pending_tcp_flows: Vec::new(),
            pending_reliable_flows: Vec::new(),
            expected_neighbor_count: 0,
            connected_neighbor_count: 0,
            routes_installed: false,
            group_directory_installed: false,
            local_topology_ready_sent: false,
        };

        tokio::spawn(async move {
            controller_sender.run().await;
        });
        tokio::spawn(async move {
            controller_receiver.run().await;
        });

        (
            controller_interface,
            reliable_runtime,
            reporter,
            flowstats_reporter,
        )
    }

    pub async fn connect(
        mut config: LocalConfig,
    ) -> (
        LocalConfig,
        ProcessorHandle,
        WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) {
        let url = url::Url::parse(&config.controller_addr).unwrap();
        let mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>;

        loop {
            let connect_fut = connect_async(url.as_str());

            match timeout(Duration::from_secs(5), connect_fut).await {
                Ok(Ok((ws, _))) => {
                    ws_stream = ws;
                    info!("WebSocket handshake has been successfully completed.");
                    break;
                }
                Ok(Err(e)) => {
                    // Connection attempt failed quickly (e.g., refused, handshake error)
                    warn!("Failed to connect to the controller: {}. Retrying...", e);
                }
                Err(_) => {
                    // Timed out
                    warn!(
                        "Timed out attempting to connect to the controller after 5s. Retrying..."
                    );
                }
            }

            // Linear backoff with small jitter (0 - 500ms)
            let jitter_ms = rand::rng().random_range(0..500);
            let backoff = Duration::from_secs(2) + Duration::from_millis(jitter_ms);

            tokio::time::sleep(backoff).await;
        }

        let startup_msg = DataplaneToController::StartUp {
            private_network_name: config.private_network_name.clone(),
            private_network_addr: config.private_network_addr.clone()
                + ":"
                + &config.private_network_port.clone(),
            public_network_addr: config.public_network_addr.clone()
                + ":"
                + &config.public_network_port.clone(),
            node_id: config.node_id.to_string().parse().ok(),
        };

        ws_stream
            .send(Message::binary(rmp_serde::to_vec(&startup_msg).unwrap()))
            .await
            .expect("Failed to send the startup message to the controller");

        // waits for the controller's response
        if let Some(response) = ws_stream.next().await {
            // updates the local configuration with settings from the controller
            config.update(response);
        } else {
            error!("No response has been received from controller.");
        }

        // starts the processor actor
        let processors = ProcessorHandle::new(config.clone());

        (config, processors, ws_stream)
    }

    /// Sends a message to the controller.
    pub async fn send(&self, msg: DataplaneToController) {
        if let Err(e) = self.northbridge_sender.send(msg) {
            error!(
                "Error sending messages to the controller interface actor: {}",
                e
            );
        };
    }

    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub async fn attach_python_interface(&self, interface: PythonInterfaceHandle) {
        let mut guard = self.python_interface.lock().await;
        *guard = Some(interface);
    }
}

#[cfg(test)]
impl ControllerInterfaceHandle {
    pub fn test_handle() -> (Self, mpsc::UnboundedReceiver<DataplaneToController>) {
        let config = LocalConfig::default();
        let processors = ProcessorHandle::new(config.clone());
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();
        let python_interface = Arc::new(Mutex::new(None));

        (
            Self {
                config,
                processors,
                northbridge_sender,
                python_interface,
            },
            northbridge_receiver,
        )
    }
}

/// An actor used for sending messages from the dataplane to the controller over WebSockets.
pub struct DataplaneToControllerSender {
    northbridge_receiver: mpsc::UnboundedReceiver<DataplaneToController>,
    sender_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl DataplaneToControllerSender {
    pub async fn run(&mut self) {
        let mut ping_interval = interval(Duration::from_secs(30)); // Send ping every 30 seconds

        loop {
            tokio::select! {
                Some(msg) = self.northbridge_receiver.recv() => {
                    self.sender_stream
                        .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                        .await
                        .expect("Failed to send message to controller");
                }
                _ = ping_interval.tick() => {
                    self.sender_stream
                        .send(Message::Ping(vec![].into()))
                        .await
                        .expect("Failed to send ping to controller");
                }
            }
        }
    }
}

/// An actor used for receiving messages from the controller and broadcasts them to the processors.
pub struct ControllerToDataplaneReceiver {
    controller: ControllerInterfaceHandle,
    config: LocalConfig,
    receiver_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    processors: ProcessorHandle,

    // reports metrics to controller
    reporter: ControllerReporterHandle,

    // handles for user-space TCP flows
    user_space_client: UserSpaceClientHandle,
    user_space_server: UserSpaceServerHandle,

    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,

    group_ip_by_id: HashMap<GroupId, Ipv4Addr>,
    reliable_runtime: ReliableRuntimeHandle,
    reliable_unicast: ReliableUnicastFlowHandle,
    topology_ready: bool,
    pending_tcp_flows: Vec<Flow>,
    pending_reliable_flows: Vec<Flow>,
    expected_neighbor_count: usize,
    connected_neighbor_count: usize,
    routes_installed: bool,
    group_directory_installed: bool,
    local_topology_ready_sent: bool,
}

impl ControllerToDataplaneReceiver {
    pub async fn run(&mut self) {
        loop {
            let msg = match self.receiver_stream.next().await.unwrap() {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Disconnected from the controller. Restarting the node...");
                    error!("{:?}", e);

                    break;
                }
            };

            match msg {
                Message::Binary(data) => {
                    let ctrl_msg: ControllerToDataplane =
                        rmp_serde::from_slice(&data).expect("Failed to parse control message");
                    self.process_control_msg(ctrl_msg).await;
                }
                Message::Pong(_) => {
                    // received a ping message to keep the connection alive. Do nothing.
                    continue;
                }
                _ => {
                    error!("Received a message that is not a binary or a ping message.");
                }
            };
        }
    }

    async fn process_control_msg(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::AddNode {
                remote_node_id,
                remote_addr,
            } => {
                // creates a new persistent TCP connection to the remote node
                self.expected_neighbor_count += 1;

                let network_interface = NetworkInterfaceHandle::new_as_client(
                    self.config.clone(),
                    remote_node_id,
                    remote_addr.clone(),
                    self.processors.clone(),
                    self.reporter.clone(),
                )
                .await;

                let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

                let _ = self.processors.add_node(remote_node_id, scheduler);

                self.record_neighbor_connected(remote_node_id).await;
            }

            ControllerToDataplane::AddNodeAddress {
                remote_node_id,
                remote_max_server_addr,
            } => {
                self.processors
                    .add_node_address(remote_node_id, remote_max_server_addr)
                    .await;
            }

            ControllerToDataplane::SetLinkRate { node_id, spec } => {
                info!(
                    "The link rate from node {} to node {} is now set to {} bytes/second, with a bucket size of {} bytes.",
                    self.config.node_id, node_id, spec.rate, spec.bucket_size,
                );

                self.processors.limit_rate(node_id, spec);
            }

            ControllerToDataplane::InstallRoutes { routes } => {
                info!(
                    "Installing {} routes on node {}.",
                    routes.len(),
                    self.config.node_id
                );

                self.processors.update_routing_table(routes).await;
                self.routes_installed = true;
                self.maybe_send_local_topology_ready().await;
            }

            ControllerToDataplane::AddFlows { flows } => {
                let mut tcp_flows = Vec::new();
                let mut reliable_flows = Vec::new();

                for flow in &flows {
                    match flow.flow_spec.transport {
                        FlowTransport::Tcp => {
                            if flow.dst_node_id == self.config.node_id {
                                // this node is the server for this flow
                                self.user_space_server.store_flow_spec(flow.clone());
                            }
                            if flow.src_node_id == self.config.node_id {
                                // this node is the client for this flow
                                tcp_flows.push(flow.clone());
                            }
                        }
                        FlowTransport::ReliableUnicast => {
                            if flow.src_node_id == self.config.node_id
                                || flow.dst_node_id == self.config.node_id
                            {
                                reliable_flows.push(flow.clone());
                            }
                        }
                    }
                }

                if !tcp_flows.is_empty() {
                    if self.topology_ready {
                        self.start_tcp_flows(tcp_flows);
                    } else {
                        info!(
                            "Deferring {} user-space TCP flows on node {} until the topology is ready.",
                            tcp_flows.len(),
                            self.config.node_id
                        );
                        self.pending_tcp_flows.extend(tcp_flows.into_iter());
                    }
                }

                if !reliable_flows.is_empty() {
                    if self.topology_ready {
                        info!(
                            "Adding {} reliable unicast flows to node {}.",
                            reliable_flows.len(),
                            self.config.node_id
                        );
                        self.reliable_unicast.add_flows(reliable_flows);
                    } else {
                        info!(
                            "Deferring {} reliable unicast flows on node {} until the topology is ready.",
                            reliable_flows.len(),
                            self.config.node_id
                        );
                        self.pending_reliable_flows
                            .extend(reliable_flows.into_iter());
                    }
                }
            }

            ControllerToDataplane::TopologyReady => {
                info!(
                    "Controller signaled that all nodes are connected; topology state is ready on node {}.",
                    self.config.node_id
                );

                self.topology_ready = true;
                self.reliable_runtime.set_topology_ready(true);

                self.flush_pending_flows();
            }

            ControllerToDataplane::GroupCreated {
                group_id,
                group_ip,
                src_node_id,
            } => {
                info!(
                    "Registered multicast group {} ({}) owned by node {}.",
                    group_id, group_ip, src_node_id
                );

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupCreated {
                            group_id,
                            src_node_id,
                            group_ip,
                        })
                        .await;
                }
            }

            ControllerToDataplane::InstallGroupDirectory { groups } => {
                info!(
                    "Installing multicast group directory ({} entries) on node {}.",
                    groups.len(),
                    self.config.node_id
                );
                self.processors.update_group_directory(groups.clone()).await;
                self.group_ip_by_id.clear();
                for entry in &groups {
                    self.group_ip_by_id.insert(entry.group_id, entry.group_ip);
                }

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupDirectoryUpdated { entries: groups })
                        .await;
                }

                self.group_directory_installed = true;
                self.maybe_send_local_topology_ready().await;
            }

            ControllerToDataplane::InstallGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                info!(
                    "Installing multicast routes for group {} from src {} on node {} ({} entries).",
                    group_id,
                    src_node_id,
                    self.config.node_id,
                    routes.len()
                );

                #[cfg(feature = "python-extension")]
                let cloned_routes = routes.clone();

                self.processors
                    .update_group_routes(group_id, src_node_id, routes)
                    .await;

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupRoutesInstalled {
                            group_id,
                            src_node_id,
                            routes: cloned_routes.clone(),
                        })
                        .await;

                    let node_id = self.config.node_id;
                    for entry in &cloned_routes {
                        if entry.next_hops.contains(&node_id) {
                            py_if
                                .publish_event(PythonEvent::LocalMemberJoined { group_id, node_id })
                                .await;
                            break;
                        }
                    }
                }
            }

            _ => error!("Received a message with an unknown type from the controller."),
        }
    }

    async fn record_neighbor_connected(&mut self, remote_node_id: usize) {
        self.connected_neighbor_count += 1;
        info!(
            "Dataplane node {} connected to neighbor {} ({}/{} ready).",
            self.config.node_id,
            remote_node_id,
            self.connected_neighbor_count,
            self.expected_neighbor_count
        );
        self.maybe_send_local_topology_ready().await;
    }

    async fn maybe_send_local_topology_ready(&mut self) {
        if self.local_topology_ready_sent {
            return;
        }

        if self.connected_neighbor_count < self.expected_neighbor_count {
            return;
        }

        if !self.routes_installed || !self.group_directory_installed {
            return;
        }

        self.local_topology_ready_sent = true;
        info!(
            "Local topology ready on node {}; notifying controller.",
            self.config.node_id
        );

        self.controller
            .send(DataplaneToController::NodeTopologyReady {
                node_id: self.config.node_id,
            })
            .await;
    }

    fn start_tcp_flows(&mut self, flows: Vec<Flow>) {
        if flows.is_empty() {
            return;
        }

        info!(
            "Adding {} user-space TCP flows to node {}.",
            flows.len(),
            self.config.node_id
        );
        self.user_space_client.add_flows(flows);
    }

    fn flush_pending_flows(&mut self) {
        if !self.pending_tcp_flows.is_empty() {
            let pending = std::mem::take(&mut self.pending_tcp_flows);

            info!(
                "Topology ready on node {}; starting {} deferred user-space flows.",
                self.config.node_id,
                pending.len()
            );

            self.start_tcp_flows(pending);
        }

        if !self.pending_reliable_flows.is_empty() {
            let pending = std::mem::take(&mut self.pending_reliable_flows);

            info!(
                "Topology ready on node {}; starting {} deferred reliable unicast flows.",
                self.config.node_id,
                pending.len()
            );

            self.reliable_unicast.add_flows(pending);
        }
    }

    #[cfg(feature = "python-extension")]
    async fn python_handle(&self) -> Option<PythonInterfaceHandle> {
        self.python_interface.lock().await.clone()
    }
}
