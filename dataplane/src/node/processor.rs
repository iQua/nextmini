/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
#[cfg(feature = "reliable")]
use std::time::{Duration, Instant};

use ahash::AHashMap;
use tokio;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[cfg(feature = "reliable")]
use crate::node::reliable::trace::{manifest_from_bytes, manifest_from_packet};
#[cfg(feature = "reliable")]
use nextmini_messages::INVALID;
#[cfg(feature = "reliable")]
use nextmini_messages::rlm;
use nextmini_messages::{
    GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, OperatingMode, RoutingTableEntry,
    TokenBucketSpec,
};

use crate::node::config::{Feature, LocalConfig};
use crate::node::connector::Connector;
use crate::node::connector::ConnectorMessage;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::{Packet, PyPayloadSegHeader};
#[cfg(feature = "reliable")]
use crate::node::python::fragment::{
    EvictedMessage, FragmentAssembler, FragmentAssemblerConfig, FragmentResult,
};
use crate::node::python::interface::PythonInterfaceHandle;
#[cfg(feature = "reliable")]
use crate::node::reliable::api::{InboundFrame as ReliableInboundFrame, ReliableHandle};
use crate::node::route::RoutingTable;
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
}

#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    UpdateGroupDirectory(Vec<GroupDirectoryEntry>),
    UpdateGroupRoutes {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    AddNode(NodeId, SchedulerHandle),
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectServerHandle(Box<UserSpaceServerHandle>),
    ConnectUserSpaceSender {
        flow_id: FlowId,
        sender: UserSpaceSender,
    },
    DisconnectUserSpaceSender(FlowId),
    RateLimit(NodeId, TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
    SetFlowStatsReporter(Box<FlowStatsReporterHandle>),
    #[allow(dead_code)] // Only emitted when the python bridge is active.
    ConnectPythonInterface(PythonInterfaceHandle),
    #[cfg(feature = "reliable")]
    ConnectReliableHandle(ReliableHandle),
}

#[derive(Clone, Debug)]
pub enum ProcessorHandle {
    Sequential(SequentialProcHandle),
    Concurrent(ConcurrentProcHandle),
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        match config.feature {
            Feature::Sequential => ProcessorHandle::Sequential(SequentialProcHandle::new(config)),
            Feature::Concurrent => ProcessorHandle::Concurrent(ConcurrentProcHandle::new(config)),
        }
    }

    pub fn broadcast_sender(&self) -> &broadcast::Sender<ProcessorMessage> {
        match self {
            ProcessorHandle::Sequential(handle) => &handle.broadcast_sender,
            ProcessorHandle::Concurrent(handle) => &handle.broadcast_sender,
        }
    }

    pub fn connector_message_sender(&self) -> &mpsc::Sender<ConnectorMessage> {
        match self {
            ProcessorHandle::Sequential(handle) => &handle.connector_message_sender,
            ProcessorHandle::Concurrent(handle) => &handle.connector_message_sender,
        }
    }

    pub fn add_node(
        &self,
        node_id: NodeId,
        scheduler: SchedulerHandle,
    ) -> Result<(), SendError<ProcessorMessage>> {
        let _ = self
            .broadcast_sender()
            .send(ProcessorMessage::AddNode(node_id, scheduler))?;

        Ok(())
    }

    // connects the local interface to the processor
    pub fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectLocalInterface(local_interface))
        {
            error!(
                "Error connecting the processors to the local interface: {}.",
                e
            );
        };
    }

    /// Connects the client handle to the processor.
    pub fn connect_user_space_sender(&self, flow_id: FlowId, sender: UserSpaceSender) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectUserSpaceSender { flow_id, sender })
        {
            error!(
                "Error connecting the client handle to the processors: {}.",
                e
            );
        };
    }

    /// Connects the in-process Python interface so local packets can be delivered directly.
    #[allow(dead_code)] // Only invoked from the python bindings crate.
    pub fn connect_python_interface(&self, interface: PythonInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectPythonInterface(interface))
        {
            error!(
                "Error sending the ConnectPythonInterface message to the processors: {}",
                e
            );
        };
    }

    #[cfg(feature = "reliable")]
    pub fn connect_reliable_handle(&self, handle: ReliableHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectReliableHandle(handle))
        {
            error!(
                "Error sending the ConnectReliableHandle message to the processors: {}",
                e
            );
        };
    }

    /// Disconnects the user-space packet sender from the processor's hashmap of senders.
    /// This is needed when a user-space TCP flow finishes.
    pub fn disconnect_user_space_sender(&self, flow_id: FlowId) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::DisconnectUserSpaceSender(flow_id))
        {
            error!(
                "Error sending the DisconnectUserSpaceSender message to the processors: {}",
                e
            );
        }
    }

    pub async fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        // broadcasts to all processors
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateRoutingTable(routes.clone()))
        {
            error!(
                "Error sending the UpdateRoutingTable message to the processors: {}",
                e
            );
        };

        // sends to the connector
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::UpdateRoutingTable(routes))
            .await
        {
            error!(
                "Error sending the UpdateRoutingTable message to the connector: {}",
                e
            );
        }
    }

    pub async fn update_group_directory(&self, groups: Vec<GroupDirectoryEntry>) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateGroupDirectory(groups))
        {
            error!(
                "Error sending the UpdateGroupDirectory message to the processors: {}",
                e
            );
        }
    }

    pub async fn update_group_routes(
        &self,
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    ) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            })
        {
            error!(
                "Error sending the UpdateGroupRoutes message to the processors: {}",
                e
            );
        }
    }

    pub fn limit_rate(&self, node_id: NodeId, spec: TokenBucketSpec) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::RateLimit(node_id, spec))
        {
            error!(
                "Error sending the SetRateLimiter message to the processors: {}",
                e
            );
        };
    }

    pub fn process_packet(&self, packet: Packet) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.process_packet(packet),
            ProcessorHandle::Concurrent(handle) => handle.process_packet(packet),
        }
    }

    pub fn connect_server(&self, server: UserSpaceServerHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectServerHandle(Box::new(server)))
        {
            error!(
                "Error sending the ConnectServerHandle message to the processors: {}",
                e
            );
        };
    }

    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "Error sending the SetFlowWeight message to the processors: {}",
                e
            );
        };
    }

    pub async fn set_flowstats_reporter(&self, flowstats_reporter: FlowStatsReporterHandle) {
        let broadcast_reporter = flowstats_reporter.clone();
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::SetFlowStatsReporter(Box::new(
                broadcast_reporter,
            )))
        {
            error!(
                "Error sending the SetFlowStatsReporter message to the processors: {}",
                e
            );
        };

        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::SetFlowStatsReporter(Box::new(
                flowstats_reporter,
            )))
            .await
        {
            error!(
                "Error sending the SetFlowStatsReporter message to the connector: {}",
                e
            );
        };
    }

    pub async fn add_node_address(&self, node_id: NodeId, remote_addr: String) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::AddNodeAddress(node_id, remote_addr))
            .await
        {
            error!(
                "Error sending the AddNodeAddress message to the connector: {}",
                e
            );
        }
    }

    pub async fn connect_tcp_max_client(&self, tcp_max_client: TcpMaxClient) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::ConnectTcpMaxClient(Box::new(
                tcp_max_client,
            )))
            .await
        {
            error!(
                "Error sending the ConnectTcpMaxClient message to the connector: {}",
                e
            );
        }
    }

    pub async fn inbound_max_request(&self, flow_id: FlowId, stream: TcpStream) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::InboundMaxRequest(flow_id, stream))
            .await
        {
            error!(
                "Error sending the InboundMaxRequest message to the connector: {}",
                e
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequentialProcHandle {
    config: LocalConfig,
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorPacket>>,
    connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    connector_message_sender: mpsc::Sender<ConnectorMessage>,
}

impl SequentialProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        for _ in 0..config.num_packet_processors {
            // creates an mpsc channel for each processor
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let mut proc = Processor::new(
                PacketReceiver::Sequential(packet_receiver),
                broadcast_sender.subscribe(),
                config.clone(),
            );

            tokio::spawn(async move {
                proc.run().await;
            });
        }

        // creates a packet channel for the connector
        let (connector_packet_sender, connector_packet_receiver) =
            mpsc::channel(config.channel_capacity);

        // creates a message channel for the connector
        let (connector_message_sender, connector_message_receiver) =
            mpsc::channel(config.channel_capacity);

        // creates a new connector
        let mut connector = Connector::new(
            connector_packet_receiver,
            connector_message_receiver,
            config.clone(),
        );

        // spawns a single connector task
        tokio::spawn(async move {
            connector.run().await;
        });

        Self {
            config,
            broadcast_sender,
            packet_senders,
            connector_packet_sender,
            connector_message_sender,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash(self.packet_senders.len());
        let sender = &self.packet_senders[idx];

        let packet_flow_id = packet.flow_id;
        let dst_node_id = self.config.ip_to_node_id(packet_flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
                warn!(
                    "SequentialProcHandle: Error sending a packet to the processor: {}.",
                    e
                );
            }
        } else {
            // sends according to the operating mode at src node
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
                        warn!(
                            "SequentialProcHandle: Error sending a packet to the processor: {}.",
                            e
                        );
                    }
                }
                OperatingMode::Max => {
                    if let Err(e) = self
                        .connector_packet_sender
                        .try_send(ProcessorPacket::ProcessPacket(packet))
                    {
                        warn!(
                            "SequentialProcHandle: Error sending a packet to the connector: {}.",
                            e
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConcurrentProcHandle {
    config: LocalConfig,
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_sender: flume::Sender<ProcessorPacket>,
    connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    connector_message_sender: mpsc::Sender<ConnectorMessage>,
}

impl ConcurrentProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for _ in 0..config.num_packet_processors {
            let mut proc = Processor::new(
                PacketReceiver::Concurrent(packet_receiver.clone()),
                broadcast_sender.subscribe(),
                config.clone(),
            );

            tokio::spawn(async move {
                proc.run().await;
            });
        }

        // create a new connector
        let (connector_packet_sender, connector_packet_receiver) =
            mpsc::channel(config.channel_capacity);
        let (connector_message_sender, connector_message_receiver) =
            mpsc::channel(config.channel_capacity);
        let mut connector = Connector::new(
            connector_packet_receiver,
            connector_message_receiver,
            config.clone(),
        );

        tokio::spawn(async move {
            connector.run().await;
        });

        Self {
            config,
            broadcast_sender,
            packet_sender,
            connector_packet_sender,
            connector_message_sender,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        let packet_flow_id = packet.flow_id;
        let dst_node_id = self.config.ip_to_node_id(packet_flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            if let Err(e) = self
                .packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet))
            {
                warn!(
                    "ConcurrentProcHandle: Error sending a packet to the processor: {}.",
                    e
                );
            }
        } else {
            // sends according to the operating mode at src node
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    if let Err(e) = self
                        .packet_sender
                        .try_send(ProcessorPacket::ProcessPacket(packet))
                    {
                        warn!(
                            "ConcurrentProcHandle: Error sending a packet to the processor: {}.",
                            e
                        );
                    }
                }
                OperatingMode::Max => {
                    if let Err(e) = self
                        .connector_packet_sender
                        .try_send(ProcessorPacket::ProcessPacket(packet))
                    {
                        warn!(
                            "ConcurrentProcHandle: Error sending a packet to the connector: {}.",
                            e
                        );
                    }
                }
            }
        }
    }
}

pub enum PacketReceiver {
    Sequential(mpsc::Receiver<ProcessorPacket>),
    Concurrent(flume::Receiver<ProcessorPacket>),
}

#[derive(Debug)]
pub enum PacketTryRecvError {
    FlumeRecvError(flume::TryRecvError),
    MpscRecvError(mpsc::error::TryRecvError),
}

impl Display for PacketTryRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketTryRecvError::FlumeRecvError(err) => {
                write!(f, "Error receiving from a flume mpmc channel: {}", err)
            }
            PacketTryRecvError::MpscRecvError(err) => {
                write!(f, "Error receiving from an MPSC channel: {}", err)
            }
        }
    }
}

impl PacketReceiver {
    pub async fn recv(&mut self) -> Option<ProcessorPacket> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver.recv().await,
            PacketReceiver::Concurrent(receiver) => receiver.recv_async().await.ok(),
        }
    }

    pub fn try_recv(&mut self) -> Result<ProcessorPacket, PacketTryRecvError> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver
                .try_recv()
                .map_err(PacketTryRecvError::MpscRecvError),
            PacketReceiver::Concurrent(receiver) => receiver
                .try_recv()
                .map_err(PacketTryRecvError::FlumeRecvError),
        }
    }
}

// Processes packets and forwards them to the next hop.
struct Processor {
    config: LocalConfig,

    // receives packets from the network interface, local interface, or user-space TCP flows
    packet_receiver: PacketReceiver,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,

    // the local TUN interface
    local_interface: Option<Arc<LocalInterfaceHandle>>,

    // channel senders for packets in user-space TCP flows
    user_space_senders: AHashMap<FlowId, UserSpaceSender>,

    // the user-space TCP server handle
    server: Option<UserSpaceServerHandle>,

    // the routing table
    routing_table: RoutingTable,

    // optional flow stats reporter for route telemetry
    flowstats_reporter: Option<FlowStatsReporterHandle>,

    // a unified hashmap for schedulers in normal mode
    schedulers: AHashMap<NodeId, SchedulerHandle>,

    // optional in-process Python delivery path
    python_interface: Option<PythonInterfaceHandle>,
    #[cfg(feature = "reliable")]
    reliable_handle: Option<ReliableHandle>,
    #[cfg(feature = "reliable")]
    reliable_fragment_enabled: bool,
    #[cfg(feature = "reliable")]
    reliable_fragment_assembler: FragmentAssembler,
}

impl Processor {
    pub fn new(
        packet_receiver: PacketReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
        config: LocalConfig,
    ) -> Self {
        #[cfg(feature = "reliable")]
        let reliable_fragment_cfg = FragmentAssemblerConfig {
            enabled: config.python_fragmentation_enabled,
            max_message_bytes: config.python_fragmentation_max_message_bytes as usize,
            reassembly_window_bytes: config.python_fragmentation_reassembly_window_bytes as usize,
            fragment_timeout: Duration::from_millis(
                config.python_fragmentation_fragment_timeout_ms as u64,
            ),
        };

        Self {
            packet_receiver,
            broadcast_receiver,
            local_interface: None,
            user_space_senders: AHashMap::new(),
            server: None,
            routing_table: RoutingTable::new(config.clone()),
            flowstats_reporter: None,
            schedulers: AHashMap::new(),
            config,
            python_interface: None,
            #[cfg(feature = "reliable")]
            reliable_handle: None,
            #[cfg(feature = "reliable")]
            reliable_fragment_enabled: reliable_fragment_cfg.enabled,
            #[cfg(feature = "reliable")]
            reliable_fragment_assembler: FragmentAssembler::new(reliable_fragment_cfg),
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            // starts a batch with the first packet
                            self.process_packet(first_packet).await;

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet).await;
                            }
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ProcessorMessage) {
        match msg {
            ProcessorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ProcessorMessage::UpdateGroupDirectory(groups) => {
                self.routing_table.install_group_directory(groups);
            }
            ProcessorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                self.routing_table
                    .install_group_routes(group_id, src_node_id, routes);
            }
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers.insert(node_id, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMessage::ConnectUserSpaceSender { flow_id, sender } => {
                self.user_space_senders.insert(flow_id, sender);
            }
            ProcessorMessage::DisconnectUserSpaceSender(flow_id) => {
                self.user_space_senders.remove(&flow_id);
            }
            ProcessorMessage::RateLimit(node_id, spec) => {
                if let Some(scheduler) = self.schedulers.get(&node_id) {
                    scheduler.limit_rate(spec);
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(*user_space_server);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for all schedulers
                for (_, scheduler) in self.schedulers.iter_mut() {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
            ProcessorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
            ProcessorMessage::ConnectPythonInterface(interface) => {
                self.python_interface = Some(interface);
            }
            #[cfg(feature = "reliable")]
            ProcessorMessage::ConnectReliableHandle(handle) => {
                self.reliable_handle = Some(handle);
            }
        }
    }

    /// Processes inbound packets for outbound delivery.
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;

        let reporter = self.flowstats_reporter.as_ref();
        match self
            .routing_table
            .get_next_hops_by_flow(packet_flow_id, reporter)
        {
            Ok(next_hops) => {
                if next_hops.is_empty() {
                    error!("No next hops available for flow {}.", packet_flow_id);
                    return;
                }

                let last = next_hops.len() - 1;
                let mut primary_packet = Some(packet);

                for (idx, next_hop_id) in next_hops.into_iter().enumerate() {
                    let pkt = if idx == last {
                        primary_packet
                            .take()
                            .expect("packet already dispatched to last hop")
                    } else {
                        primary_packet
                            .as_ref()
                            .expect("packet missing during multicast fan-out")
                            .clone()
                    };

                    self.send_packet(pkt, next_hop_id).await;
                }
            }
            Err(e) => error!("Error resolving route for flow {}: {}", packet_flow_id, e),
        }
    }

    /// Locates a channel sender for delivering packets in user-space flows, based on the flow ID.
    fn user_space_sender(&mut self, flow_id: FlowId) -> Option<UserSpaceSender> {
        if let Some(sender) = self.user_space_senders.get(&flow_id) {
            Some(sender.clone())
        } else {
            if flow_id.dst_port() != self.config.user_space_server_port {
                return None;
            }

            let server_handle = self
                .server
                .clone()
                .expect("The user-space server has not yet been connected.");

            let sender = server_handle.add_server(flow_id);
            self.user_space_senders.insert(flow_id, sender.clone());

            Some(sender)
        }
    }

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface,
    /// a user-space TCP client, or a user-space TCP server.
    async fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId) {
        #[allow(unused_mut)]
        let mut packet = packet;
        #[cfg(feature = "reliable")]
        let manifest_meta = manifest_from_packet(&packet);
        // checks if the next hop is the dst node
        if next_hop_id == self.routing_table.local_id {
            #[cfg(feature = "reliable")]
            {
                if self.try_deliver_reliable(&packet) {
                    return;
                }
            }
            // local delivery: use the destination IP address to distinguish between the TUN interface
            // and user-space TCP clients or servers
            if packet.flow_id.dst_ip() == self.config.local_address {
                if let Some(ref local_interface) = self.local_interface {
                    local_interface.write_packet(packet);
                } else {
                    error!("The local interface has not yet been connected.");
                }
            } else {
                if let Some(ref py_if) = self.python_interface {
                    match py_if.deliver(packet).await {
                        Ok(()) => return,
                        Err(returned_packet) => {
                            packet = returned_packet;
                        }
                    }
                }

                let flow_id = packet.flow_id;

                let dest = self.user_space_sender(flow_id);
                if let Some(sender) = dest
                    && sender.try_send(packet).is_err()
                {
                    tracing::error!(
                        "Failed to send a packet in user-space flows to its local destination."
                    );
                }
            }
        } else if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
            #[cfg(feature = "reliable")]
            if let Some(meta) = manifest_meta {
                tracing::debug!(
                    session_id = meta.session_id,
                    flow = %packet.flow_id,
                    src_node = self.routing_table.local_id,
                    next_hop = next_hop_id,
                    "RLM MANIFEST queued for scheduler hop"
                );
            }
            scheduler.send(packet);
        }
    }

    #[cfg(feature = "reliable")]
    fn try_deliver_reliable(&mut self, packet: &Packet) -> bool {
        let Some(handle) = self.reliable_handle.clone() else {
            return false;
        };
        let Some(payload) = packet.tcp_payload() else {
            return false;
        };
        let mut owned_payload: Option<Vec<u8>> = None;
        let mut payload_slice: &[u8] = payload;
        if self.reliable_fragment_enabled {
            match self.defragment_reliable(packet.flow_id, payload) {
                DefragOutcome::Ready(Cow::Borrowed(bytes)) => {
                    payload_slice = bytes;
                }
                DefragOutcome::Ready(Cow::Owned(bytes)) => {
                    owned_payload = Some(bytes);
                    payload_slice = owned_payload.as_ref().unwrap();
                }
                DefragOutcome::Pending => return true,
                DefragOutcome::Dropped => return true,
            }
        }
        let manifest_meta = manifest_from_bytes(payload_slice);

        let session_id = if let Some((hdr, _, _)) = rlm::decode_data(payload_slice) {
            hdr.session_id
        } else if let Some((hdr, _)) = rlm::decode_control(payload_slice) {
            hdr.session_id
        } else {
            return false;
        };

        let src_node = self.config.ip_to_node_id(packet.flow_id.src_ip());
        let peer_id = if src_node == INVALID {
            None
        } else {
            Some(src_node)
        };

        let payload_vec = if let Some(buf) = owned_payload {
            buf
        } else {
            payload_slice.to_vec()
        };

        handle.deliver(
            session_id,
            ReliableInboundFrame {
                bytes: payload_vec,
                peer_id,
                group_ip: Some(packet.flow_id.dst_ip()),
                source_node_id: peer_id,
            },
        );
        if let Some(meta) = manifest_meta {
            tracing::debug!(
                session_id = meta.session_id,
                local_node = self.routing_table.local_id,
                source = ?peer_id,
                dst_ip = %packet.flow_id.dst_ip(),
                "RLM MANIFEST delivered to reliable stack on this node"
            );
        }
        true
    }

    #[cfg(feature = "reliable")]
    fn defragment_reliable<'a>(&mut self, flow_id: FlowId, payload: &'a [u8]) -> DefragOutcome<'a> {
        match PyPayloadSegHeader::decode_from(payload) {
            Ok((header, remainder)) => {
                let expected = header.fragment_payload_len as usize;
                if header.fragment_count <= 1 && !header.fragmented {
                    if expected <= remainder.len() {
                        return DefragOutcome::Ready(Cow::Owned(remainder[..expected].to_vec()));
                    } else {
                        tracing::warn!(
                            flow = %flow_id,
                            expected,
                            available = remainder.len(),
                            "RLM defragmenter: fragment payload exceeds available bytes."
                        );
                        return DefragOutcome::Dropped;
                    }
                }
                if expected > remainder.len() {
                    tracing::warn!(
                        flow = %flow_id,
                        expected,
                        available = remainder.len(),
                        "RLM defragmenter: fragment payload truncated."
                    );
                    return DefragOutcome::Dropped;
                }
                let chunk = remainder[..expected].to_vec();
                let report = self.reliable_fragment_assembler.insert_fragment(
                    flow_id,
                    header,
                    chunk,
                    None,
                    Instant::now(),
                );
                self.handle_fragment_evictions(&report.evicted);
                match report.result {
                    FragmentResult::Pending => DefragOutcome::Pending,
                    FragmentResult::Complete(message) => {
                        DefragOutcome::Ready(Cow::Owned(message.payload))
                    }
                    FragmentResult::Dropped(drop) => {
                        tracing::warn!(
                            flow = %flow_id,
                            detail = drop.detail,
                            kind = ?drop.kind,
                            "RLM defragmenter dropped message."
                        );
                        DefragOutcome::Dropped
                    }
                    FragmentResult::Bypassed => DefragOutcome::Ready(Cow::Borrowed(payload)),
                }
            }
            Err(_) => DefragOutcome::Ready(Cow::Borrowed(payload)),
        }
    }

    #[cfg(feature = "reliable")]
    fn handle_fragment_evictions(&self, evicted: &[EvictedMessage]) {
        for msg in evicted {
            tracing::warn!(
                flow = %msg.flow_id,
                message_id = msg.message_id,
                missing = msg.missing_fragments,
                "RLM defragmenter evicted message after timeout."
            );
        }
    }
}

#[cfg(feature = "reliable")]
enum DefragOutcome<'a> {
    Ready(Cow<'a, [u8]>),
    Pending,
    Dropped,
}
