// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.

use crate::node::packet::Packet;
use crate::node::routes::RoutingTable;
use crate::node::scheduler::SchedulerHandle;
use crate::node::local_interface::TunWriterHandle;
use crate::node::{FlowId, NodeId};

use flume::bounded;
use std::collections::HashMap;
use tokio;
use tracing::{debug, error};

/// Actor Model Implementation

// Message type to processor
pub enum ProcessorMessage {
    ProcessPacket(Packet),
    UpdateRoutingTable(RoutingTable),
}

// Processor: Processes packets and forwards them to the next hop
struct Processor {
    // Processor Receiver
    receiver: flume::Receiver<ProcessorMessage>,

    // Data Used by the processor
    routing_table: RoutingTable,

    // Handles to send to the next stage
    tun_writer_handle: TunWriterHandle,
    scheduler_handles: HashMap<NodeId, SchedulerHandle>,
}

impl Processor {
    async fn run(&mut self) {
        // TODO : Integrate metrics_collector_handle

        while let Ok(msg) = self.receiver.recv_async().await {
            match msg {
                ProcessorMessage::ProcessPacket(packet) => {
                    self.process_packet(packet).await;
                }
                ProcessorMessage::UpdateRoutingTable(new_table) => {
                    self.routing_table = new_table;
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
pub struct ProcessorHandle {
    sender: flume::Sender<ProcessorMessage>,
}

impl ProcessorHandle {
    pub fn new(
        // The size of the mpmc channel
        mpmc_channel_size: usize,
        // The number of processors
        num_processors: usize,

        // A copy of the routing table
        routing_table: RoutingTable,

        // The TUN writer handle
        tun_writer: TunWriterHandle,
        // The scheduler handles
        scheduler_handles: HashMap<NodeId, SchedulerHandle>,
    ) -> Self {
        let (processor_sender, processor_receiver) = bounded(mpmc_channel_size);
        for _ in 0..num_processors {
            let mut actor = Processor {
                receiver: processor_receiver.clone(),
                routing_table: routing_table.clone(),
                tun_writer_handle: tun_writer.clone(),
                scheduler_handles: scheduler_handles.clone(),
            };
            tokio::spawn(async move { actor.run().await });
        }
        Self {
            sender: processor_sender,
        }
    }

    pub async fn update_routing_table(&self, new_table: RoutingTable) {
        self.sender
            .send_async(ProcessorMessage::UpdateRoutingTable(new_table))
            .await
            .expect("Failed to send update to processor");
    }
    pub async fn process_packet(&self, packet: Packet) {
        self.sender
            .send_async(ProcessorMessage::ProcessPacket(packet))
            .await
            .expect("Failed to send packet to processor");
    }
}
