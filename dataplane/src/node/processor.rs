/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::collections::HashMap;

use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;

use flume;
use tracing::error;

use nextmini_messages::RoutingTableEntry;

use crate::node::config::LocalConfig;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::route::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowId, NodeId};

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
    packet_sender: flume::Sender<ProcessorMessage>,
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for _ in 0..config.num_packet_processors {
            let mut proc = Processor {
                packet_receiver: packet_receiver.clone(),
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
            packet_sender,
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

    pub async fn process_packet(&self, packet: Packet) {
        self.packet_sender
            .send(ProcessorMessage::ProcessPacket(packet))
            .unwrap();
    }
}

// Processes packets and forwards them to the next hop.
struct Processor {
    // receives packets from the network interface or local interface
    packet_receiver: flume::Receiver<ProcessorMessage>,

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
            let msg = tokio::select! {
                // packets from the inbound network or local interfaces
                Ok(packet) = self.packet_receiver.recv_async() => {
                    Some(packet)
                }
                // messages from the controller interface or the conductor
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    Some(broadcast_msg)
                }
            };

            match msg {
                Some(ProcessorMessage::ProcessPacket(packet)) => {
                    let _ = self.process_packet(packet).await;
                }
                Some(ProcessorMessage::UpdateRoutingTable(routes)) => {
                    self.routing_table.install_routes(routes);
                }
                Some(ProcessorMessage::AddNode(node_id, scheduler)) => {
                    // updates the scheduler for a given node ID
                    self.schedulers.insert(node_id, scheduler);
                }
                Some(ProcessorMessage::ConnectLocalInterface(local_interface)) => {
                    self.local_interface = Some(local_interface);
                }
                None => {
                    error!("Processor received an unexpected message");
                    break;
                }
            }
        }
    }

    /// Process inbound packets for outbound delivery
    async fn process_packet(&mut self, packet: Packet) -> Result<(), String> {
        let packet_flow_id = packet.flow_id;

        // Select route_id for new flow at source node
        let route_id = self
            .routing_table
            .select_route_for_flow(packet_flow_id)
            .ok_or_else(|| {
                let error = format!(
                    "No route is found for flow {}: the routing table may be misconfigured",
                    packet_flow_id
                );
                error!("{}", error);

                error
            })?;

        if route_id == 0 {
            // no route can be possible as the flow ID is not valid (represented as a value of 0)
            // perhaps a non-IPv4 packet?
            // drops the packet without forwarding it
            return Err("No route can be selected".to_string());
        }

        // Route the packet to its next hop
        let next_hop_id = self
            .routing_table
            .get_next_hop_by_route(route_id)
            .ok_or_else(|| {
                let error = format!(
                    "No next hop is found for route id {} on flow {}: routing inconsistency detected",
                    route_id, packet_flow_id
                );
                error!("{}", error);

                error
            })?;

        self.send_packet(packet, next_hop_id, packet_flow_id).await
    }

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface.
    async fn send_packet(
        &mut self,
        packet: Packet,
        next_hop_id: NodeId,
        packet_flow_id: FlowId,
    ) -> Result<(), String> {
        if next_hop_id == self.routing_table.local_id {
            // local delivery
            if let Some(ref local_interface) = self.local_interface {
                if let Err(e) = local_interface.write_packet(packet).await {
                    let error = format!(
                        "Failed to send a packet with flow ID {} to the local interface: {}",
                        packet_flow_id, e
                    );
                    error!("{}", error);

                    Err(error)
                } else {
                    Ok(())
                }
            } else {
                Err("The local interface has not yet been connected.".to_string())
            }
        } else {
            match self.schedulers.get_mut(&next_hop_id) {
                Some(scheduler) => {
                    if let Err(e) = scheduler.send(packet).await {
                        let error = format!(
                            "Failed to send a packet with flow ID {} to the scheduler for next hop {}: {}",
                            packet_flow_id, next_hop_id, e
                        );
                        error!("{}", error);

                        Err(error)
                    } else {
                        Ok(())
                    }
                }
                None => {
                    error!(
                        "Next hop node {} is offline or unreachable for flow {}: connection may have been lost.",
                        next_hop_id, packet_flow_id
                    );

                    Ok(())
                }
            }
        }
    }
}
