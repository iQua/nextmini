// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.
use std::collections::HashMap;

use flume;
use tokio::select;
use tokio::sync::broadcast;
use tracing::{debug, error};

use crate::node::config::LocalConfig;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::routes::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowId, NodeId};
use nextmini_messages::RoutingTableEntry;

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
    controller_sender: broadcast::Sender<ProcessorMessage>,
    packet_sender: flume::Sender<ProcessorMessage>,
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (controller_sender, _) = broadcast::channel(config.channel_capacity);
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for i in 0..config.num_packet_processors {
            let mut actor = Processor {
                packet_receiver: packet_receiver.clone(),
                controller_receiver: controller_sender.subscribe(),
                routing_table: RoutingTable::new(config.node_id), // config.node_id is local node ID
                local_interface: None,
                schedulers: HashMap::new(),
                writer_index: i,
            };
            tokio::spawn(async move { actor.run().await });
        }
        Self {
            controller_sender,
            packet_sender,
        }
    }

    pub fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        self.controller_sender
            .send(ProcessorMessage::ConnectLocalInterface(local_interface));
    }

    pub async fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        self.controller_sender
            .send(ProcessorMessage::UpdateRoutingTable(routes));
    }

    pub async fn add_node(&self, node_id: NodeId, scheduler_handle: SchedulerHandle) {
        self.controller_sender
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
    // Processor Receiver
    packet_receiver: flume::Receiver<ProcessorMessage>, // Receive packets from local or network interfaces
    controller_receiver: broadcast::Receiver<ProcessorMessage>,

    // Data Used by the processor
    routing_table: RoutingTable,

    // Handles to send to the next stage
    local_interface: Option<LocalInterfaceHandle>,
    schedulers: HashMap<NodeId, SchedulerHandle>,

    // Index of the writer to use for local delivery
    writer_index: usize,
}

impl Processor {
    async fn run(&mut self) {
        loop {
            #[allow(unused_assignments)] // To satisfy the compiler
            let mut msg: Option<ProcessorMessage> = None;

            select! {
                // Packets from both sources (network interface, local interface)
                Ok(packet_msg) = self.packet_receiver.recv_async() => {
                    msg = Some(packet_msg);
                }
                // Control messages from the controller interface
                Ok(controller_msg) = self.controller_receiver.recv() => {
                    msg = Some(controller_msg);
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

        self.send_packet(packet, next_hop_id, packet_flow_id).await;
        Ok(())
    }

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
                Err("Local interface not connected".to_string())
            }
        } else {
            match self.schedulers.get_mut(&next_hop_id) {
                Some(scheduler_handle) => {
                    debug!(
                        "Forwarding packet to node {} for flow {} (size: {})",
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
