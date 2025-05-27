// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fxhash::FxHashMap;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::dataplane::NodeId;
use crate::dataplane::local_interface::TunWriter;
use crate::dataplane::node_interface::NodeSender;
use crate::dataplane::packet::Packet;
use crate::dataplane::routes::RoutingTable;
use crate::dataplane::{FlowId, FlowIdExt, context::Context, metrics::MetricsTx};
use nextmini_messages::RoutingTableEntry;
use tracing::{debug, error, warn};

pub struct ProcessorManager {
    context: Context,
    proc_handles: VecDeque<tokio::task::JoinHandle<()>>,
    proc_shutdown_flags: VecDeque<Arc<AtomicBool>>,
    receiver_rxs: VecDeque<Arc<RwLock<mpsc::Receiver<Packet>>>>,
    pub routing_table: RoutingTable,
}

impl ProcessorManager {
    pub fn new(context: Context, mut receiver_rxs: Vec<mpsc::Receiver<Packet>>) -> Self {
        let mut rxs = VecDeque::new();
        let routing_table = RoutingTable::new(context.local_id);
        for _ in 0..receiver_rxs.len() {
            let rx = receiver_rxs.pop().unwrap();
            rxs.push_back(Arc::new(RwLock::new(rx)));
        }
        Self {
            context,
            proc_handles: VecDeque::new(),
            proc_shutdown_flags: VecDeque::new(),
            receiver_rxs: rxs,
            routing_table,
        }
    }

    pub async fn update_simple_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        // Set base IPv4 address for node ID calculation (should be configurable)
        self.routing_table.set_base_ipv4_addr([10, 0, 0, 0]);

        // Install routes directly using RoutingTableEntry
        self.routing_table.install_routes(routes);
        self.swap_processors().await;
    }

    pub async fn update_processors(&mut self) {
        self.swap_processors().await;
    }

    async fn swap_processors(&mut self) {
        if self.proc_handles.is_empty() {
            // If there is no processor running, we simply spawn new processors
            self.spawn_processors().await;
        } else {
            // If there are processors running, we wait for new processors to spawn then shutdown old ones.
            // Note that the new processors will be started immediately after spawning, but will be stuck waiting
            // for rx writing guard at the beginning of the loop until the old processors are shutdown.
            self.spawn_processors().await;
            self.drop_processors().await;
        }
    }
    async fn spawn_processors(&mut self) {
        for i in 0..self.receiver_rxs.len() {
            let receiver_rx = self.receiver_rxs[i].clone();
            let shutdown_flag = Arc::new(AtomicBool::new(false));

            let flg = shutdown_flag.clone();
            let simple_table = self.routing_table.clone();
            let senders = self.context.reproduce_senders().await;

            let tun_writer = self.context.get_tun_writer(i).await;

            let metrics_tx = self.context.get_metrics_tx();
            let handle = tokio::task::spawn(async move {
                let mut proc = Processor::new(
                    receiver_rx,
                    simple_table,
                    senders,
                    tun_writer,
                    flg,
                    metrics_tx,
                );
                proc.run().await;
            });
            self.proc_shutdown_flags.push_back(shutdown_flag);
            self.proc_handles.push_back(handle);
        }
    }

    pub async fn drop_processors(&mut self) {
        for _ in 0..self.receiver_rxs.len() {
            let flg = self.proc_shutdown_flags.pop_front().unwrap();
            let hdl = self.proc_handles.pop_front().unwrap();
            flg.store(true, Ordering::Relaxed);
            //Wait for the processor to shutdown gracefully
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            //Drop the processor if it is still running
            hdl.abort();
        }
    }
}


/// Actor Model Implementation

// Processor: Processes packets and forwards them to the next hop
struct Processor {
    // Processor Receiver
    receiver: flume::Receiver<ProcessorMessage>,

    // Data Used by the processor
    routing_table: RoutingTable,
    local_id: NodeId,

    // Handles to send to the next stage
    local_writer_handle: LocalWriterHandle,
    scheduler_handles: HashMap<NodeId, SchedulerHandle>,
    metrics_collector_handle: MetricsCollectorHandle,
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
            debug!(
                "Delivering packet locally for destination {}:{} (size: {}).",
                packet_flow_id.dst_ip(),
                packet_flow_id.dst_port(),
                packet.packet_size
            );
            self.tun_writer.write_packet(packet).await;
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
struct ProcessorHandle {
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
        // The local id
        local_id: NodeId,
        
        // The local writer handle
        local_writer: LocalWriterHandle,
        // The scheduler handles
        scheduler_handles: HashMap<NodeId, SchedulerHandle>,
        // The metrics collector handle
        metrics_collector_handle: MetricsCollectorHandle,

    ) -> Self {
        let (processor_sender, processor_receiver) = bounded(mpmc_channel_size);
        for _ in 0..num_processors {
            let mut actor = Processor {
                receiver: processor_receiver.clone(),
                routing_table: routing_table.clone(),
                local_id,
                local_writer_handle: local_writer.clone(),
                scheduler_handles: scheduler_handles.clone(),
                metrics_collector_handle: metrics_collector_handle.clone(),
            };
            tokio::spawn(async move { actor.run().await });
        }
        Self { sender: processor_sender }
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
