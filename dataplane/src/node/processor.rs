/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::collections::HashMap;

use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tracing::{error, warn};

use nextmini_messages::RoutingTableEntry;

use crate::node::config::LocalConfig;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowIdExt, NodeId};

// Message types for the processor actor.
#[derive(Clone)]
pub enum ProcessorMessage {
    ProcessPacket(Packet),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    AddNode(NodeId, SchedulerHandle),
    ConnectLocalInterface(LocalInterfaceHandle),
}

#[derive(Clone)]
pub struct ProcessorHandle {
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorMessage>>,
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        for _ in 0..config.num_packet_processors {
            // for each Processor, creates one MPSC channel
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let mut proc = Processor {
                packet_receiver,
                broadcast_receiver: broadcast_sender.subscribe(),
                routing_table: RoutingTable::new(config.node_id),
                local_interface: None,
                schedulers: HashMap::new(),
            };

            tokio::spawn(async move {
                proc.run().await;
            });
        }
        Self {
            broadcast_sender,
            packet_senders,
        }
    }

    pub fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender
            .send(ProcessorMessage::ConnectLocalInterface(local_interface))
        {
            error!(
                "Error connecting the processors to the local interface: {}.",
                e
            );
        };
    }

    pub async fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        if let Err(e) = self
            .broadcast_sender
            .send(ProcessorMessage::UpdateRoutingTable(routes))
        {
            error!(
                "Error sending the UpdateRoutingTable message to the processors: {}",
                e
            );
        };
    }

    pub async fn add_node(
        &self,
        node_id: NodeId,
        scheduler: SchedulerHandle,
    ) -> Result<(), SendError<ProcessorMessage>> {
        let _ = self
            .broadcast_sender
            .send(ProcessorMessage::AddNode(node_id, scheduler))?;

        Ok(())
    }

    pub fn process_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash() % self.packet_senders.len();
        let sender = &self.packet_senders[idx];

        if let Err(e) = sender.try_send(ProcessorMessage::ProcessPacket(packet)) {
            warn!(
                "Error sending a packet to the processor: {}. The processor may be overloaded.",
                e
            );
        }
    }
}

// Processes packets and forwards them to the next hop.
struct Processor {
    // receives packets from the network interface or local interface
    packet_receiver: mpsc::Receiver<ProcessorMessage>,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,

    // the routing table
    routing_table: RoutingTable,

    // the local interface
    local_interface: Option<LocalInterfaceHandle>,

    // schedulers, one for each outbound network interface
    schedulers: HashMap<NodeId, SchedulerHandle>,
}

impl Processor {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Wait for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorMessage::ProcessPacket(first_packet) => {
                            // Start a batch with the first packet
                            self.process_packet(first_packet);

                            // Start processing packets in batches
                            while let Ok(message) = self.packet_receiver.try_recv() {
                                match message {
                                    ProcessorMessage::ProcessPacket(packet) => {
                                        self.process_packet(packet);
                                    }
                                    _ => {
                                        break;
                                    }
                                }
                            }
                        }
                        other_msg => {
                            // Handle other messages that are not packets
                            self.handle_message(other_msg).await;
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
        }
    }

    // New helper method to handle non-packet messages
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
                self.local_interface = Some(local_interface);
            }
            ProcessorMessage::ProcessPacket(_) => {
                error!("Unexpected ProcessPacket message found. Something went wrong.");
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
