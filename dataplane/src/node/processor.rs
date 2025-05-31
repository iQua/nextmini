// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.

use crate::node::local_interface::TunWriterHandle;
use crate::node::packet::Packet;
use crate::node::routes::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::{FlowId, NodeId};
use nextmini_messages::RoutingTableEntry;

use flume::bounded;
use std::collections::HashMap;
use tokio::select;
use tokio::sync::broadcast;
use tracing::{debug, error};

/// Actor Model Implementation

// Message type to processor
#[derive(Clone)]
pub enum ProcessorMessage {
    ProcessPacket(Packet),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    AddNode(NodeId, SchedulerHandle),
}

// Processor: Processes packets and forwards them to the next hop
struct Processor {
    // Processor Receiver
    receiver_from_writer: flume::Receiver<ProcessorMessage>,
    receiver_from_controller: broadcast::Receiver<ProcessorMessage>,

    // Data Used by the processor
    routing_table: RoutingTable,

    // Handles to send to the next stage
    tun_writer_handle: TunWriterHandle,
    scheduler_handles: HashMap<NodeId, SchedulerHandle>,
}

impl Processor {
    async fn run(&mut self) {
        loop {
            #[allow(unused_assignments)] // To satisfy the compiler
            let mut msg: Option<ProcessorMessage> = None;

            select! {
                Ok(writer_msg) = self.receiver_from_writer.recv_async() => {
                    msg = Some(writer_msg);
                }
                Ok(controller_msg) = self.receiver_from_controller.recv() => {
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
                    self.scheduler_handles.insert(node_id, scheduler_handle);
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

        self.send_packet_to_next_hop(packet, next_hop_id, packet_flow_id)
            .await
    }

    async fn send_packet_to_next_hop(
        &mut self,
        packet: Packet,
        next_hop_id: NodeId,
        packet_flow_id: FlowId,
    ) -> Result<(), String> {
        if next_hop_id == self.routing_table.local_id {
            // Local delivery
            self.tun_writer_handle.write_packet(packet).await;
            Ok(())
        } else {
            match self.scheduler_handles.get_mut(&next_hop_id) {
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

#[derive(Clone)]
pub struct ProcessorHandle{
    controller_sender: broadcast::Sender<ProcessorMessage>,
    writer_sender: flume::Sender<ProcessorMessage>,
}

impl ProcessorHandle{
    pub fn new(
        mpmc_channel_size: usize,
        broadcast_channel_size: usize,
        num_processors: usize,
        routing_table: RoutingTable,
        tun_writer: TunWriterHandle,
        scheduler_handles: HashMap<NodeId, SchedulerHandle>,
    ) -> Self {
        let (controller_sender, _) = broadcast::channel(broadcast_channel_size);
        let (writer_sender, writer_receiver) = bounded(mpmc_channel_size);

        for _ in 0..num_processors {
            let mut actor = Processor {
                receiver_from_writer: writer_receiver.clone(),
                receiver_from_controller: controller_sender.subscribe(), 
                routing_table: routing_table.clone(),
                tun_writer_handle: tun_writer.clone(),
                scheduler_handles: scheduler_handles.clone(),
            };
            tokio::spawn(async move { actor.run().await });
        }
        Self{
            controller_sender,
            writer_sender,
        }
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
        self.writer_sender
            .send(ProcessorMessage::ProcessPacket(packet))
            .expect("Failed to send packet to processor from reader");
    }
}