/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use ahash::AHashMap;
use flume;
use tokio;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info, warn};

use nextmini_messages::{OperatingMode, RoutingTableEntry, TokenBucketSpec};

use crate::node::config::{Feature, LocalConfig};
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

#[derive(Eq, PartialEq, Hash, Clone, Copy, Debug)]
enum SchedulerKey {
    Node(NodeId),
    Flow(FlowId),
}

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
    InboundMaxRequest(FlowId, TcpStream),
}

#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    // for normal mode
    AddNode(NodeId, SchedulerHandle),
    // for max mode
    AddNodeAddress(NodeId, String),
    ConnectTcpMaxClient(TcpMaxClient),
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectUserSpaceSender {
        flow_id: FlowId,
        sender: UserSpaceSender,
    },
    DisconnectUserSpaceSender(FlowId),
    ConnectServerHandle(UserSpaceServerHandle),
    RateLimit(NodeId, TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
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

    pub fn add_node_address(&self, node_id: NodeId, remote_addr: String) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::AddNodeAddress(node_id, remote_addr))
        {
            error!(
                "Error sending the AddNodeAddress message to the processors: {}",
                e
            );
        }
    }

    pub fn connect_tcp_max_client(&self, tcp_max_client: TcpMaxClient) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectTcpMaxClient(tcp_max_client))
        {
            error!(
                "Error sending the ConnectTcpMaxClient message to the processors: {}",
                e
            );
        }
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

    // connects the client handle to the processor
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

    // Disconnects the user-space packet sender from the processor's hashmap of senders.
    // This is needed when a user-space TCP flow finishes.
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

    pub fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateRoutingTable(routes))
        {
            error!(
                "Error sending the UpdateRoutingTable message to the processors: {}",
                e
            );
        };
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
            .send(ProcessorMessage::ConnectServerHandle(server))
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

    // TODO: changed thr name to inbound max_request
    pub fn splice_connection(&self, flow_id: FlowId, stream: TcpStream) {
        let inbound_max_request = ProcessorPacket::InboundMaxRequest(flow_id, stream);
        // TODO: no need to match
        match self {
            ProcessorHandle::Sequential(handle) => {
                let idx = flow_id.hash(handle.packet_senders.len());
                let sender = &handle.packet_senders[idx];
                if let Err(e) = sender.try_send(incoming_request) {
                    warn!(
                        "SequentialProcMaxHandle: Error sending a splice connection to the processor: {}.",
                        e
                    );
                }
            }
            ProcessorHandle::Concurrent(handle) => {
                if let Err(e) = handle.packet_sender.try_send(packet) {
                    warn!(
                        "ConcurrentProcMaxHandle: Error sending a splice connection to the processor: {}.",
                        e
                    );
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequentialProcHandle {
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorPacket>>,
}

impl SequentialProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        // TODO: spawns a new single task for connector

        let mut connector = Connector::new();

        for _ in 0..config.num_packet_processors {
            // for each Processor, creates one MPSC channel
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

        // TODO:
        tokio::spawn(async move {
            connector.run().await;
        });

        Self {
            broadcast_sender,
            packet_senders,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash(self.packet_senders.len());
        let sender = &self.packet_senders[idx];

        if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
            warn!(
                "SequentialProcHandle: Error sending a packet to the processor: {}.",
                e
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConcurrentProcHandle {
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_sender: flume::Sender<ProcessorPacket>,
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

        Self {
            broadcast_sender,
            packet_sender,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        if let Err(e) = self
            .packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!(
                "ConcurrentProcHandle: Error sending a packet to the processor: {}",
                e
            );
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
                write!(f, "Error receiving from a mpsc channel: {}", err)
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

    // a unified hashmap for schedulers in both normal and max modes
    schedulers: AHashMap<SchedulerKey, SchedulerHandle>,

    // the remote nodes addresses (for max mode)
    node_addresses: AHashMap<NodeId, String>,

    // the tcp max client
    tcp_max_client: Option<TcpMaxClient>,
}

impl Processor {
    pub fn new(
        packet_receiver: PacketReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
        config: LocalConfig,
    ) -> Self {
        Self {
            packet_receiver,
            broadcast_receiver,
            local_interface: None,
            user_space_senders: AHashMap::new(),
            server: None,
            routing_table: RoutingTable::new(config.clone()),
            schedulers: AHashMap::new(),
            node_addresses: AHashMap::new(),
            config,
            tcp_max_client: None,
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
                        // for relay nodes
                        ProcessorPacket::InboundMaxRequest(flow_id, stream) => {
                            self.handle_inbound_max_request(flow_id, stream).await;
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
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers
                    .insert(SchedulerKey::Node(node_id), scheduler);
            }
            ProcessorMessage::AddNodeAddress(node_id, remote_addr) => {
                self.node_addresses.insert(node_id, remote_addr);
            }
            ProcessorMessage::ConnectTcpMaxClient(tcp_max_client) => {
                self.tcp_max_client = Some(tcp_max_client);
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
                if let Some(scheduler) = self.schedulers.get(&SchedulerKey::Node(node_id)) {
                    scheduler.limit_rate(spec);
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(user_space_server);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for all schedulers
                for (_, scheduler) in self.schedulers.iter_mut() {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
        }
    }

    async fn handle_inbound_max_request(&mut self, flow_id: FlowId, mut inbound_stream: TcpStream) {
        let route_id = self.routing_table.select_route_for_flow(flow_id).unwrap();
        let next_hop_id = self.routing_table.get_next_hop_by_route(route_id).unwrap();

        // handles the case where we are at the dst node.
        if next_hop_id == self.routing_table.local_id {
            let scheduler = self
                .tcp_max_client
                .as_ref()
                .unwrap()
                .connect_as_dst_node(inbound_stream, next_hop_id)
                .await;

            // inserts reversed flow id.
            self.schedulers
                .insert(SchedulerKey::Flow(flow_id.reverse()), scheduler);

            return;
        }

        // handles the case where we are at a relay node.
        let next_hop_addr = self.node_addresses.get(&next_hop_id).cloned().unwrap();

        let mut outbound_stream = self
            .tcp_max_client
            .as_ref()
            .unwrap()
            .connect_as_relay(flow_id, &next_hop_addr)
            .await;

        // TODO: We might don't need to spawn the task;
        // spawns a new task to handle the connection splicing.
        tokio::spawn(async move {
            match zero_copy_bidirectional(&mut inbound_stream, &mut outbound_stream).await {
                Ok((upstream_bytes, downstream_bytes)) => {
                    info!(
                        "Spliced connection for flow {} to {} (upstream: {} bytes, downstream: {} bytes).",
                        flow_id, next_hop_addr, upstream_bytes, downstream_bytes
                    );
                }
                Err(e) => {
                    error!("Error during splicing for flow {}: {}.", flow_id, e);
                }
            }
        });
    }

    /// Processes inbound packets for outbound delivery
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;

        // selects the route ID for a new flow
        if let Some(route_id) = self.routing_table.select_route_for_flow(packet_flow_id) {
            if route_id == 0 {
                // No route can be possible as the flow ID is not valid (represented as a value of 0)
                // perhaps a non-IPv4 packet? Drops the packet without forwarding it.
                error!("No route can be selected.");
            }

            // routes the packet to its next hop
            if let Some(next_hop_id) = self.routing_table.get_next_hop_by_route(route_id) {
                self.send_packet(packet, next_hop_id).await;
            } else {
                error!(
                    "No next hop is found for route id {} on flow {}: routing inconsistency detected.",
                    route_id, packet_flow_id
                );
            }
        } else {
            error!(
                "No route is found for flow {}: the routing table may be misconfigured.",
                packet_flow_id
            );
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
        if next_hop_id == self.routing_table.local_id {
            // local delivery: use the destination IP address to distinguish between the TUN interface
            // and user-space TCP clients or servers
            if packet.flow_id.dst_ip() == self.config.local_address {
                if let Some(ref local_interface) = self.local_interface {
                    local_interface.write_packet(packet);
                } else {
                    error!("The local interface has not yet been connected.");
                }
            } else {
                let flow_id = packet.flow_id;

                let dest = self.user_space_sender(flow_id);
                if let Some(sender) = dest {
                    if sender.try_send(packet).is_err() {
                        tracing::error!(
                            "Failed to send a packet in user-space flows to its local destination."
                        );
                    }
                }
            }
        } else {
            match self.config.operating_mode {
                OperatingMode::Max => {
                    let scheduler_key = SchedulerKey::Flow(packet.flow_id);

                    if let Some(scheduler) = self.schedulers.get(&scheduler_key) {
                        scheduler.send(packet);
                    } else {
                        // We are on the src node and the tcp connection is not spliced yet

                        // gets the remote node address
                        let remote_addr = self.node_addresses[&next_hop_id].clone();

                        let scheduler = self
                            .tcp_max_client
                            .as_ref()
                            .unwrap()
                            .connect_as_src_node(packet.flow_id, &remote_addr, next_hop_id)
                            .await;

                        // sends the packet
                        scheduler.send(packet);

                        // inserts the scheduler into the hashmap
                        self.schedulers.insert(scheduler_key, scheduler);
                    }
                }

                OperatingMode::Normal => {
                    if let Some(scheduler) = self.schedulers.get(&SchedulerKey::Node(next_hop_id)) {
                        scheduler.send(packet);
                    }
                }
            }
        }
    }
}
