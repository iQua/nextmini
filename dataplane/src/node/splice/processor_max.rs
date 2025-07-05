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
use tracing::{error, warn};

use nextmini_messages::{RoutingTableEntry, TokenBucketSpec};

use crate::node::config::{Feature, LocalConfig};
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

// Message types for the processor actor.
pub enum ProcessorMaxPacket {
    ProcessPacket(Packet),
    SpliceConnection(FlowId, TcpStream),
}

#[derive(Debug, Clone)]
pub enum ProcessorMaxMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    AddNode(NodeId, String),
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectUserSpaceSender {
        flow_id: FlowId,
        sender: UserSpaceSender,
    },
    DisconnectUserSpaceSender(FlowId),
    ConnectServerHandle(UserSpaceServerHandle),
    SetFlowWeight(FlowId, usize),
}

#[derive(Clone, Debug)]
pub enum ProcessorMaxHandle {
    Sequential(SequentialProcMaxHandle),
    Concurrent(ConcurrentProcMaxHandle),
}

impl ProcessorMaxHandle {
    pub fn new(config: LocalConfig) -> Self {
        match config.feature {
            Feature::Sequential => {
                ProcessorMaxHandle::Sequential(SequentialProcMaxHandle::new(config))
            }
            Feature::Concurrent => {
                ProcessorMaxHandle::Concurrent(ConcurrentProcMaxHandle::new(config))
            }
        }
    }

    pub fn broadcast_sender(&self) -> &broadcast::Sender<ProcessorMaxMessage> {
        match self {
            ProcessorMaxHandle::Sequential(handle) => &handle.broadcast_sender,
            ProcessorMaxHandle::Concurrent(handle) => &handle.broadcast_sender,
        }
    }

    // connects the local interface to the processor
    pub fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMaxMessage::ConnectLocalInterface(local_interface))
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
            .send(ProcessorMaxMessage::ConnectUserSpaceSender { flow_id, sender })
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
            .send(ProcessorMaxMessage::DisconnectUserSpaceSender(flow_id))
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
            .send(ProcessorMaxMessage::UpdateRoutingTable(routes))
        {
            error!(
                "Error sending the UpdateRoutingTable message to the processors: {}",
                e
            );
        };
    }

    pub fn add_node(&self, node_id: NodeId, remote_addr: String) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMaxMessage::AddNode(node_id, remote_addr))
        {
            error!("Error sending the AddNode message to the processors: {}", e);
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        match self {
            ProcessorMaxHandle::Sequential(handle) => handle.process_packet(packet),
            ProcessorMaxHandle::Concurrent(handle) => handle.process_packet(packet),
        }
    }

    pub fn connect_server(&self, server: UserSpaceServerHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMaxMessage::ConnectServerHandle(server))
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
            .send(ProcessorMaxMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "Error sending the SetFlowWeight message to the processors: {}",
                e
            );
        };
    }

    pub fn splice_connection(&self, flow_id: FlowId, stream: TcpStream) {
        let packet = ProcessorMaxPacket::SpliceConnection(flow_id, stream);
        match self {
            ProcessorMaxHandle::Sequential(handle) => {
                let idx = flow_id.hash(handle.packet_senders.len());
                let sender = &handle.packet_senders[idx];
                if let Err(e) = sender.try_send(packet) {
                    warn!(
                        "SequentialProcMaxHandle: Error sending a splice connection to the processor: {}.",
                        e
                    );
                }
            }
            ProcessorMaxHandle::Concurrent(handle) => {
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
pub struct SequentialProcMaxHandle {
    broadcast_sender: broadcast::Sender<ProcessorMaxMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorMaxPacket>>,
}

impl SequentialProcMaxHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        for _ in 0..config.num_packet_processors {
            // for each Processor, creates one MPSC channel
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let mut proc = ProcessorMax::new(
                PacketMaxReceiver::Sequential(packet_receiver),
                broadcast_sender.subscribe(),
                config.clone(),
            );

            tokio::spawn(async move {
                proc.run().await;
            });
        }

        Self {
            broadcast_sender,
            packet_senders,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash(self.packet_senders.len());
        let sender = &self.packet_senders[idx];

        if let Err(e) = sender.try_send(ProcessorMaxPacket::ProcessPacket(packet)) {
            warn!(
                "SequentialProcMaxHandle: Error sending a packet to the processor: {}.",
                e
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConcurrentProcMaxHandle {
    broadcast_sender: broadcast::Sender<ProcessorMaxMessage>,
    packet_sender: flume::Sender<ProcessorMaxPacket>,
}

impl ConcurrentProcMaxHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for _ in 0..config.num_packet_processors {
            let mut proc = ProcessorMax::new(
                PacketMaxReceiver::Concurrent(packet_receiver.clone()),
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
            .try_send(ProcessorMaxPacket::ProcessPacket(packet))
        {
            warn!(
                "ConcurrentProcMaxHandle: Error sending a packet to the processor: {}",
                e
            );
        }
    }
}

pub enum PacketMaxReceiver {
    Sequential(mpsc::Receiver<ProcessorMaxPacket>),
    Concurrent(flume::Receiver<ProcessorMaxPacket>),
}

#[derive(Debug)]
pub enum PacketMaxTryRecvError {
    FlumeRecvError(flume::TryRecvError),
    MpscRecvError(mpsc::error::TryRecvError),
}

impl Display for PacketMaxTryRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketMaxTryRecvError::FlumeRecvError(err) => {
                write!(f, "Error receiving from a flume mpmc channel: {}", err)
            }
            PacketMaxTryRecvError::MpscRecvError(err) => {
                write!(f, "Error receiving from a mpsc channel: {}", err)
            }
        }
    }
}

impl PacketMaxReceiver {
    pub async fn recv(&mut self) -> Option<ProcessorMaxPacket> {
        match self {
            PacketMaxReceiver::Sequential(receiver) => receiver.recv().await,
            PacketMaxReceiver::Concurrent(receiver) => receiver.recv_async().await.ok(),
        }
    }

    pub fn try_recv(&mut self) -> Result<ProcessorMaxPacket, PacketMaxTryRecvError> {
        match self {
            PacketMaxReceiver::Sequential(receiver) => receiver
                .try_recv()
                .map_err(PacketMaxTryRecvError::MpscRecvError),
            PacketMaxReceiver::Concurrent(receiver) => receiver
                .try_recv()
                .map_err(PacketMaxTryRecvError::FlumeRecvError),
        }
    }
}

// Processes packets and forwards them to the next hop.
struct ProcessorMax {
    config: LocalConfig,

    // receives packets from the network interface, local interface, or user-space TCP flows
    packet_receiver: PacketMaxReceiver,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMaxMessage>,

    // the local TUN interface
    local_interface: Option<Arc<LocalInterfaceHandle>>,

    // channel senders for packets in user-space TCP flows
    user_space_senders: AHashMap<FlowId, UserSpaceSender>,

    // the user-space TCP server handle
    server: Option<UserSpaceServerHandle>,

    // the routing table
    routing_table: RoutingTable,

    // the schedulers (for upstream packets)
    schedulers: AHashMap<FlowId, SchedulerHandle>,

    // the remote nodes addresses
    node_addresses: AHashMap<NodeId, String>,
}

impl ProcessorMax {
    pub fn new(
        packet_receiver: PacketMaxReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMaxMessage>,
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
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        // for src node and dst node
                        ProcessorMaxPacket::ProcessPacket(first_packet) => {
                            // starts a batch with the first packet
                            self.process_packet(first_packet);

                            // starts processing packets in batches
                            while let Ok(ProcessorMaxPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet);
                            }
                        }
                        // for relay nodes
                        ProcessorMaxPacket::SpliceConnection(flow_id, stream) => {
                            // TODO: Implement splice logic
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ProcessorMaxMessage) {
        match msg {
            ProcessorMaxMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ProcessorMaxMessage::AddNode(node_id, remote_addr) => {
                self.node_addresses.insert(node_id, remote_addr);
            }
            ProcessorMaxMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMaxMessage::ConnectUserSpaceSender { flow_id, sender } => {
                self.user_space_senders.insert(flow_id, sender);
            }
            ProcessorMaxMessage::DisconnectUserSpaceSender(flow_id) => {
                self.user_space_senders.remove(&flow_id);
            }
            ProcessorMaxMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(user_space_server);
            }
            ProcessorMaxMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for a scheduler
                if let Some(scheduler) = self.schedulers.get_mut(&flow_id) {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
        }
    }

    /// Process inbound packets for outbound delivery
    fn process_packet(&mut self, packet: Packet) {
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
                self.send_packet(packet, next_hop_id);
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
    fn send_packet(&mut self, packet: Packet, _next_hop_id: NodeId) {
        if _next_hop_id == self.routing_table.local_id {
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
            let scheduler_key = packet.flow_id;

            if let Some(scheduler) = self.schedulers.get(&scheduler_key) {
                scheduler.send(packet);
            } else {
                // We are on the src node and the tcp connection is not spliced yet

                let tcp_max_client = TcpMaxClient::new(self.config.clone());

                // To get the TCP stream for the client, needs to know remote node addr.
                let remote_addr = self.node_addresses[&packet.flow_id.dst_node_id].clone();
                let stream = tcp_max_client.connect(remote_addr);

                // Create network interface and scheduler from the TCP stream
                let network_interface = NetworkInterfaceHandle::new(self.config.clone(), stream);
                let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

                // Send the packet through this scheduler
                scheduler.send(packet);

                // Insert the scheduler into the hashmap
                self.schedulers.insert(scheduler_key, scheduler);
            }
        }
    }
}
