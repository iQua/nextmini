/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use ahash::AHashMap;
use flume;
use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tracing::{error, warn};

use nextmini_messages::{RoutingTableEntry, TokenBucketSpec};

use crate::node::LocalDestination;
use crate::node::config::{Feature, LocalConfig};
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
}

#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    AddNode(NodeId, SchedulerHandle),
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectLocalDestination {
        flow_id: FlowId,
        destination: Arc<dyn LocalDestination>,
    },
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
    pub fn connect_local_destination(
        &self,
        flow_id: FlowId,
        destination: Arc<dyn LocalDestination>,
    ) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectLocalDestination {
                flow_id,
                destination,
            })
        {
            error!(
                "Error connecting the client handle to the processors: {}.",
                e
            );
        };
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

    pub fn connect_server_handle(&self, server_handle: UserSpaceServerHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectServerHandle(server_handle))
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

    // the local TUN interface and local destinations (for destination packets)
    local_interface: Option<Arc<LocalInterfaceHandle>>,
    local_destinations: AHashMap<FlowId, Arc<dyn LocalDestination>>,
    // the user-space TCP server handle
    server_handle: Option<UserSpaceServerHandle>,

    // the routing table
    routing_table: RoutingTable,

    // the schedulers (for upstream packets)
    schedulers: AHashMap<NodeId, SchedulerHandle>,
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
            local_destinations: AHashMap::new(),
            server_handle: None,
            routing_table: RoutingTable::new(config.clone()),
            schedulers: AHashMap::new(),
            config,
        }
    }

    /// Locates a local destination based on the destination IP address and port number.
    fn local_destination(&mut self, flow_id: FlowId) -> Option<Arc<dyn LocalDestination>> {
        if flow_id.dst_ip() == self.config.local_address {
            self.local_interface
                .clone()
                .map(|l| l as Arc<dyn LocalDestination>)
        } else {
            if let Some(destination) = self.local_destinations.get(&flow_id) {
                Some(destination.clone())
            } else {
                if let Some(server_handle) = &mut self.server_handle {
                    server_handle.add_server(flow_id);
                }

                None
            }
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
                            self.process_packet(first_packet);

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet);
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
            ProcessorMessage::AddNode(node_id, scheduler) => {
                // updates the scheduler for a given node ID
                self.schedulers.insert(node_id, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMessage::ConnectLocalDestination {
                flow_id,
                destination,
            } => {
                self.local_destinations.insert(flow_id, destination);
            }
            ProcessorMessage::RateLimit(node_id, spec) => {
                if let Some(scheduler) = self.schedulers.get(&node_id) {
                    scheduler.limit_rate(spec);
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server_handle) => {
                self.server_handle = Some(user_space_server_handle);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for all schedulers
                for (_, scheduler) in self.schedulers.iter_mut() {
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

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface,
    /// a user-space TCP client, or a user-space TCP server.
    fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId) {
        if next_hop_id == self.routing_table.local_id {
            // local delivery: use the destination IP address to distinguish between the TUN interface
            // and user-space TCP clients or servers
            let flow_id = packet.flow_id;

            if let Some(dest) = self.local_destination(flow_id) {
                dest.send_packet(packet);
            } else {
                error!("No local destination found for flow_id: {}", flow_id);
            }
        } else {
            if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
                scheduler.send(packet);
            }
        }
    }
}
