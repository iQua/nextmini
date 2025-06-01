// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.
use std::collections::HashMap;

use flume;
use tokio;
use tokio::sync::broadcast;
use tracing::{debug, error};

use nextmini_messages::RoutingTableEntry;

use crate::node::config::LocalConfig;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::routes::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

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
        self.broadcast_sender
            .send(ProcessorMessage::ConnectLocalInterface(local_interface));
    }

    pub async fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        self.broadcast_sender
            .send(ProcessorMessage::UpdateRoutingTable(routes));
    }

    pub async fn add_node(&self, node_id: NodeId, scheduler_handle: SchedulerHandle) {
        self.broadcast_sender
            .send(ProcessorMessage::AddNode(node_id, scheduler_handle));
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
                Ok(packet_msg) = self.packet_receiver.recv_async() => {
                    Some(packet_msg)
                }
                // messages from the controller interface or the conductor
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    Some(broadcast_msg)
                }
            };

            match msg {
                Some(ProcessorMessage::ProcessPacket(packet)) => {
                    self.process_packet(packet).await;
                }
                Some(ProcessorMessage::UpdateRoutingTable(routes)) => {
                    self.routing_table.install_routes(routes);
                }
                Some(ProcessorMessage::AddNode(node_id, scheduler_handle)) => {
                    self.schedulers.insert(node_id, scheduler_handle);
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

        debug!(
            "Processing packet for flow {}:{} -> {}:{}",
            packet_flow_id.src_ip(),
            packet_flow_id.src_port(),
            packet_flow_id.dst_ip(),
            packet_flow_id.dst_port()
        );

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

        debug!(
            "Flow {}:{} -> {}:{} selected route_id {} → next_hop {}.",
            packet_flow_id.src_ip(),
            packet_flow_id.src_port(),
            packet_flow_id.dst_ip(),
            packet_flow_id.dst_port(),
            route_id,
            next_hop_id
        );

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
            // Local delivery
            if let Some(ref local_interface) = self.local_interface {
                local_interface.write_packet(packet).await;
                Ok(())
            } else {
                Err("The local interface has not yet been connected.".to_string())
            }
        } else {
            match self.schedulers.get_mut(&next_hop_id) {
                Some(scheduler_handle) => {
                    debug!(
                        "Forwarding a packet to node {} for flow {} (size: {})",
                        next_hop_id, packet_flow_id, packet.packet_size
                    );

                    scheduler_handle.send(packet).await;
                    Ok(())
                }
                None => {
                    let error = format!(
                        "Next hop node {} is offline or unreachable for flow {}: connection may have been lost.",
                        next_hop_id, packet_flow_id
                    );
                    error!("{}", error);

                    Err(error)
                }
            }
        }
    }
}
