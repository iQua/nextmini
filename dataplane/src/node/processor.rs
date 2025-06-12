/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::collections::HashMap;

use flume;
use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tracing::{error, warn};

use nextmini_messages::RoutingTableEntry;

use crate::node::config::{Feature, LocalConfig};
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
}

#[derive(Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    AddNode(NodeId, SchedulerHandle),
    ConnectLocalInterface(LocalInterfaceHandle),
}

#[derive(Clone)]
pub enum ProcessorHandle {
    Sequential(SequentialProcHandle),
    Concurrent(ConcurrentProcHandle),
}

#[derive(Clone)]
pub struct ConcurrentProcHandle {
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_sender: flume::Sender<ProcessorPacket>,
}

impl ConcurrentProcHandle {
    pub fn new(config: LocalConfig, broadcast_sender: broadcast::Sender<ProcessorMessage>) -> Self {
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for _ in 0..config.num_packet_processors {
            let proc = Processor::Concurrent(ConcurrentProcessor {
                packet_receiver: packet_receiver.clone(),
                broadcast_receiver: broadcast_sender.subscribe(),
                routing_table: RoutingTable::new(config.node_id),
                local_interface: None,
                schedulers: HashMap::new(),
            });

            tokio::spawn(async move {
                let mut proc = proc;
                proc.run().await;
            });
        }

        Self {
            broadcast_sender,
            packet_sender,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        self.packet_sender
            .send(ProcessorPacket::ProcessPacket(packet))
            .unwrap();
    }
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);

        match config.feature {
            Feature::Sequential => {
                ProcessorHandle::Sequential(SequentialProcHandle::new(config, broadcast_sender))
            }
            Feature::Concurrent => {
                ProcessorHandle::Concurrent(ConcurrentProcHandle::new(config, broadcast_sender))
            }
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

    pub fn process_packet(&self, packet: Packet) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.process_packet(packet),
            ProcessorHandle::Concurrent(handle) => handle.process_packet(packet),
        }
    }
}

#[derive(Clone)]
pub struct SequentialProcHandle {
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorPacket>>,
}

impl SequentialProcHandle {
    pub fn new(config: LocalConfig, broadcast_sender: broadcast::Sender<ProcessorMessage>) -> Self {
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        for _ in 0..config.num_packet_processors {
            // for each Processor, creates one MPSC channel
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let proc = Processor::Sequential(SequentialProcessor {
                packet_receiver,
                broadcast_receiver: broadcast_sender.subscribe(),
                routing_table: RoutingTable::new(config.node_id),
                local_interface: None,
                schedulers: HashMap::new(),
            });

            tokio::spawn(async move {
                let mut proc = proc;
                proc.run().await;
            });
        }

        Self {
            broadcast_sender,
            packet_senders,
        }
    }

    pub fn process_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash() % self.packet_senders.len();
        let sender = &self.packet_senders[idx];

        if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
            warn!(
                "Error sending a packet to the processor: {}. The processor may be overloaded.",
                e
            );
        }
    }
}

enum Processor {
    Sequential(SequentialProcessor),
    Concurrent(ConcurrentProcessor),
}

pub trait ProcessorExt {
    async fn run(&mut self);
    // same methods for both Sequential and Concurrent processors
    fn routing_table_mut(&mut self) -> &mut RoutingTable;
    fn local_interface_mut(&mut self) -> &mut Option<LocalInterfaceHandle>;
    fn schedulers_mut(&mut self) -> &mut HashMap<NodeId, SchedulerHandle>;

    // implementation for handling messages
    async fn handle_message(&mut self, msg: ProcessorMessage) {
        match msg {
            ProcessorMessage::UpdateRoutingTable(routes) => {
                self.routing_table_mut().install_routes(routes);
            }
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers_mut().insert(node_id, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                *self.local_interface_mut() = Some(local_interface); // &mut Option<LocalInterfaceHandle>
            }
        }
    }
}

impl ProcessorExt for Processor {
    async fn run(&mut self) {
        match self {
            Processor::Sequential(proc) => proc.run().await,
            Processor::Concurrent(proc) => proc.run().await,
        }
    }

    fn routing_table_mut(&mut self) -> &mut RoutingTable {
        match self {
            Processor::Sequential(proc) => proc.routing_table_mut(),
            Processor::Concurrent(proc) => proc.routing_table_mut(),
        }
    }

    fn local_interface_mut(&mut self) -> &mut Option<LocalInterfaceHandle> {
        match self {
            Processor::Sequential(proc) => proc.local_interface_mut(),
            Processor::Concurrent(proc) => proc.local_interface_mut(),
        }
    }

    fn schedulers_mut(&mut self) -> &mut HashMap<NodeId, SchedulerHandle> {
        match self {
            Processor::Sequential(proc) => proc.schedulers_mut(),
            Processor::Concurrent(proc) => proc.schedulers_mut(),
        }
    }
}

// for sequential feature
struct SequentialProcessor {
    // receives packets from the network interface or local interface
    packet_receiver: mpsc::Receiver<ProcessorPacket>,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,

    // the routing table
    routing_table: RoutingTable,

    // the local interface
    local_interface: Option<LocalInterfaceHandle>,

    // schedulers, one for each outbound network interface
    schedulers: HashMap<NodeId, SchedulerHandle>,
}

// for concurrent feature
struct ConcurrentProcessor {
    // receives packets from the network interface or local interface
    packet_receiver: flume::Receiver<ProcessorPacket>,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,

    // the routing table
    routing_table: RoutingTable,

    // the local interface
    local_interface: Option<LocalInterfaceHandle>,

    // schedulers, one for each outbound network interface
    schedulers: HashMap<NodeId, SchedulerHandle>,
}

impl ProcessorExt for SequentialProcessor {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Wait for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            // Start a batch with the first packet
                            self.process_packet(first_packet);

                            // Start processing packets in batches
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

    fn routing_table_mut(&mut self) -> &mut RoutingTable {
        &mut self.routing_table
    }

    fn local_interface_mut(&mut self) -> &mut Option<LocalInterfaceHandle> {
        &mut self.local_interface
    }

    fn schedulers_mut(&mut self) -> &mut HashMap<NodeId, SchedulerHandle> {
        &mut self.schedulers
    }
}

impl SequentialProcessor {
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

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface.
    fn send_packet(&self, packet: Packet, next_hop_id: NodeId) {
        if next_hop_id == self.routing_table.local_id {
            // local delivery
            if let Some(ref local_interface) = self.local_interface {
                local_interface.write_packet(packet);
            } else {
                error!("The local interface has not yet been connected.");
            }
        } else {
            if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
                scheduler.send(packet);
            }
        }
    }
}

impl ProcessorExt for ConcurrentProcessor {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Ok(msg) = self.packet_receiver.recv_async() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(packet) => {
                            let _ = self.process_packet(packet).await;
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
        }
    }

    fn routing_table_mut(&mut self) -> &mut RoutingTable {
        &mut self.routing_table
    }

    fn local_interface_mut(&mut self) -> &mut Option<LocalInterfaceHandle> {
        &mut self.local_interface
    }

    fn schedulers_mut(&mut self) -> &mut HashMap<NodeId, SchedulerHandle> {
        &mut self.schedulers
    }
}

impl ConcurrentProcessor {
    /// Process inbound packets for outbound delivery
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;

        // Select route_id for new flow at source node
        if let Some(route_id) = self.routing_table.select_route_for_flow(packet_flow_id) {
            if route_id == 0 {
                // no route can be possible as the flow ID is not valid (represented as a value of 0)
                // perhaps a non-IPv4 packet?
                // drops the packet without forwarding it
                error!("No route can be selected.");
                return;
            }

            // Route the packet to its next hop
            if let Some(next_hop_id) = self.routing_table.get_next_hop_by_route(route_id) {
                let _ = self.send_packet(packet, next_hop_id, packet_flow_id).await;
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

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface.
    async fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId, packet_flow_id: FlowId) {
        if next_hop_id == self.routing_table.local_id {
            // local delivery
            if let Some(ref local_interface) = self.local_interface {
                if let Err(e) = local_interface.write_packet_async(packet).await {
                    error!(
                        "Failed to send a packet with flow ID {} to the local interface: {}",
                        packet_flow_id, e
                    );
                }
            } else {
                error!("The local interface has not yet been connected.");
            }
        } else {
            if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
                if let Err(e) = scheduler.send_async(packet).await {
                    error!(
                        "Failed to send a packet with flow ID {} to the scheduler for next hop {}: {}",
                        packet_flow_id, next_hop_id, e
                    );
                }
            } else {
                error!(
                    "Next hop node {} is offline or unreachable for flow {}: connection may have been lost.",
                    next_hop_id, packet_flow_id
                );
            }
        }
    }
}
