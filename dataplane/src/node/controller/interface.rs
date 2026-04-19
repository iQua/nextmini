use std::collections::HashMap;
use std::net::Ipv4Addr;
#[cfg(feature = "python-extension")]
use std::sync::Arc;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
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
use crate::node::controller::lossless_unicast::LosslessUnicastFlowManager;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::client::UserSpaceClientHandle;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
#[cfg(feature = "python-extension")]
use crate::node::python::interface::{PythonEvent, PythonInterfaceHandle};
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::session::api::LosslessRuntimeHandle;

pub type ProbeSchedulerRegistration = (usize, SchedulerHandle);

#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    pub config: LocalConfig,
    pub processors: ProcessorHandle,
    northbridge_sender: mpsc::UnboundedSender<DataplaneToController>,
    pub probe_scheduler_sender: mpsc::UnboundedSender<ProbeSchedulerRegistration>,
    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,
}

/// The handle for the controller interface, which allows sending messages to the controller.
impl ControllerInterfaceHandle {
    pub async fn new(
        config: LocalConfig,
    ) -> (
        Self,
        LosslessRuntimeHandle,
        ControllerReporterHandle,
        FlowStatsReporterHandle,
    ) {
        // creates an unbounded channel, the 'northbridge', for sending messages to the controller
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();
        let (probe_scheduler_sender, probe_scheduler_receiver) = mpsc::unbounded_channel();

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
            probe_scheduler_sender: probe_scheduler_sender.clone(),
            #[cfg(feature = "python-extension")]
            python_interface: python_interface.clone(),
        };

        let reporter = ControllerReporterHandle::new(
            controller_interface.clone(),
            config.metrics_collection_interval,
        );

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

        // creates the lossless runtime handle with the correct processors
        let lossless_runtime =
            LosslessRuntimeHandle::new(processors.clone(), config.lossless_runtime_config.clone());
        processors.connect_lossless_handle(lossless_runtime.clone());

        // creates the lossless unicast flow manager with the correct processors
        let lossless_unicast = LosslessUnicastFlowManager::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
            lossless_runtime.clone(),
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
            lossless_runtime: lossless_runtime.clone(),
            group_ip_by_id: HashMap::new(),
            lossless_unicast,
            topology_ready: false,
            pending_tcp_flows: Vec::new(),
            pending_lossless_flows: Vec::new(),
            expected_neighbor_count: 0,
            connected_neighbor_count: 0,
            routes_installed: false,
            group_directory_installed: false,
            local_topology_ready_sent: false,
            schedulers: HashMap::new(),
            probe_scheduler_receiver,
        };

        tokio::spawn(async move {
            controller_sender.run().await;
        });
        tokio::spawn(async move {
            controller_receiver.run().await;
        });

        (
            controller_interface,
            lossless_runtime,
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
        let connect_timeout = Duration::from_millis(config.controller_connect_timeout_ms.max(1));

        loop {
            let connect_fut = connect_async(url.as_str());

            match timeout(connect_timeout, connect_fut).await {
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
                        "Timed out attempting to connect to the controller after {:?}. Retrying...",
                        connect_timeout
                    );
                }
            }

            // Linear backoff with small jitter (0 - 500ms)
            let jitter_ms = rand::random_range(0..500);
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
                    if let Err(e) = self.sender_stream
                        .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                        .await
                    {
                        error!("Failed to send message to controller: {}. Closing sender.", e);
                        break;
                    }
                }
                _ = ping_interval.tick() => {
                    if let Err(e) = self
                        .sender_stream
                        .send(Message::Ping(Vec::new().into()))
                        .await
                    {
                        warn!("Failed to send ping to controller: {}. Closing sender.", e);
                        break;
                    }
                }
            }
        }

        info!("DataplaneToController sender stopped.");
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
    lossless_runtime: LosslessRuntimeHandle,
    lossless_unicast: LosslessUnicastFlowManager,
    topology_ready: bool,
    pending_tcp_flows: Vec<Flow>,
    pending_lossless_flows: Vec<Flow>,
    expected_neighbor_count: usize,
    connected_neighbor_count: usize,
    routes_installed: bool,
    group_directory_installed: bool,
    local_topology_ready_sent: bool,
    /// Scheduler handles cloned for direct probe sending (bypasses processor).
    schedulers: HashMap<usize, SchedulerHandle>,
    probe_scheduler_receiver: mpsc::UnboundedReceiver<ProbeSchedulerRegistration>,
}

impl ControllerToDataplaneReceiver {
    pub async fn run(&mut self) {
        loop {
            let msg = tokio::select! {
                Some((remote_node_id, scheduler)) = self.probe_scheduler_receiver.recv() => {
                    self.schedulers.insert(remote_node_id, scheduler);
                    continue;
                }
                msg = self.receiver_stream.next() => {
                    match msg {
                        Some(Ok(msg)) => msg,
                        Some(Err(e)) => {
                            error!("Disconnected from the controller. Restarting the node...");
                            error!("{:?}", e);

                            break;
                        }
                        None => break,
                    }
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

                self.schedulers.insert(remote_node_id, scheduler.clone());
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
                let mut lossless_flows = Vec::new();

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
                        FlowTransport::LosslessUnicast => {
                            if flow.src_node_id == self.config.node_id
                                || flow.dst_node_id == self.config.node_id
                            {
                                lossless_flows.push(flow.clone());
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

                if !lossless_flows.is_empty() {
                    if self.topology_ready {
                        info!(
                            "Adding {} lossless unicast flows to node {}.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.lossless_unicast.add_flows(lossless_flows);
                    } else {
                        info!(
                            "Deferring {} lossless unicast flows on node {} until the topology is ready.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.pending_lossless_flows.extend(lossless_flows);
                    }
                }
            }

            ControllerToDataplane::TopologyReady => {
                info!(
                    "Controller signaled that all nodes are connected; topology state is ready on node {}.",
                    self.config.node_id
                );

                self.topology_ready = true;
                self.lossless_runtime.set_topology_ready(true);

                // Emit Python event so Python code can wait for topology ready
                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if.publish_event(PythonEvent::TopologyReady).await;
                }

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

            ControllerToDataplane::ProbeLink {
                remote_node_id,
                probe_id,
                probe_bytes,
            } => {
                self.send_probe(remote_node_id, probe_id, probe_bytes);
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

    /// Probe payload:  [flags:1][probe_id:8][sender_node_id:8][padding]
    /// 1360 bytes of TCP payload + 20 IP + 20 TCP header = 1400-byte virtual packet.
    const PROBE_PAYLOAD_SIZE: usize = 1360;

    fn send_probe(&self, remote_node_id: usize, probe_id: u64, probe_bytes: usize) {
        let scheduler = match self.schedulers.get(&remote_node_id) {
            Some(s) => s,
            None => {
                error!("ProbeLink: no connection to node {}.", remote_node_id);
                return;
            }
        };

        let num_packets = (probe_bytes / Self::PROBE_PAYLOAD_SIZE).max(1);
        let sender_id = self.config.node_id as u64;
        let mut packets = Vec::with_capacity(num_packets);

        for i in 0..num_packets {
            let is_last = i == num_packets - 1;
            let mut payload = vec![0u8; Self::PROBE_PAYLOAD_SIZE];
            payload[0] = u8::from(is_last); // 0x00 data, 0x01 last
            payload[1..9].copy_from_slice(&probe_id.to_be_bytes());
            payload[9..17].copy_from_slice(&sender_id.to_be_bytes());

            packets.push(Packet::build_ipv4_tcp_packet(
                std::net::Ipv4Addr::new(127, 0, 0, 1),
                0,
                std::net::Ipv4Addr::new(127, 0, 0, 2),
                0,
                &payload,
            ));
        }

        info!(
            "Probe {}: sending {} packets (~{} bytes) to node {}.",
            probe_id,
            num_packets,
            num_packets * Self::PROBE_PAYLOAD_SIZE,
            remote_node_id,
        );
        scheduler.send_probe_bypass(packets);
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

        if !self.pending_lossless_flows.is_empty() {
            let pending = std::mem::take(&mut self.pending_lossless_flows);

            info!(
                "Topology ready on node {}; starting {} deferred lossless unicast flows.",
                self.config.node_id,
                pending.len()
            );

            self.lossless_unicast.add_flows(pending);
        }
    }

    #[cfg(feature = "python-extension")]
    async fn python_handle(&self) -> Option<PythonInterfaceHandle> {
        self.python_interface.lock().await.clone()
    }
}

#[cfg(test)]
impl ControllerInterfaceHandle {
    pub fn test_handle() -> (Self, mpsc::UnboundedReceiver<DataplaneToController>) {
        let config = LocalConfig::default();
        let processors = ProcessorHandle::new(config.clone());
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();
        #[cfg(feature = "python-extension")]
        let python_interface = Arc::new(Mutex::new(None));

        (
            Self {
                config,
                processors,
                northbridge_sender,
                #[cfg(feature = "python-extension")]
                python_interface,
            },
            northbridge_receiver,
        )
    }
}
