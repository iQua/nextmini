// A processor is designed to process inbound packets from either a NodeReceiver or a TUN reader,
// and uses a routing table to determine how it should be sent out: to either a NodeSender or
// a TUN writer.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fxhash::FxHashMap;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::dataplane::INTERNAL_Q_SIZE;
use crate::dataplane::NodeId;
use crate::dataplane::local_interface::TunWriter;
use crate::dataplane::node_interface::NodeSender;
use crate::dataplane::packet::Packet;
use crate::dataplane::routes::RoutingTable;
use crate::dataplane::{FlowId, context::Context, metrics::MetricsTx};
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
            //Wait for the processor to shutdown
            tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
            //Drop the processor if it is still running
            hdl.abort();
        }
    }
}

pub struct Processor {
    // The channel receiver to obtain packets from NodeReceiver or TUN reader
    receiver_rx: Arc<RwLock<mpsc::Receiver<Packet>>>,

    // The simplified routing table
    routing_table: RoutingTable,

    // The senders that send packets to the network
    senders: FxHashMap<NodeId, NodeSender>,

    // The local interface writer
    tun_writer: TunWriter,

    // The flag to indicate if the processor should shutdown
    should_shutdown: Arc<AtomicBool>,

    // The metrics collector channel
    metrics_tx: MetricsTx,
}

impl Processor {
    pub fn new(
        receiver_rx: Arc<RwLock<mpsc::Receiver<Packet>>>,
        routing_table: RoutingTable,
        senders: FxHashMap<NodeId, NodeSender>,
        tun_writer: TunWriter,
        should_shutdown: Arc<AtomicBool>,
        metrics_tx: MetricsTx,
    ) -> Self {
        Self {
            receiver_rx,
            routing_table,
            senders,
            tun_writer,
            should_shutdown,
            metrics_tx,
        }
    }

    /// Process inbound packets for outbound delivery
    async fn process_packet(&mut self, packet: Packet) -> Result<(), String> {
        let packet_flow_id = packet.flow_id;

        debug!("Processing new packet for flow {}", packet_flow_id);

        // Select route_id for new flow at source node
        let route_id = self
            .routing_table
            .select_route_for_flow(packet_flow_id)
            .ok_or_else(|| {
                let error = format!(
                    "No route is found for flow {}: the routing table may be misconfigured.",
                    packet_flow_id
                );
                error!("{}", error);

                error
            })?;

        if route_id == 0 {
            // no route can be possible as the flow ID is not valid (represented as a value of 0)
            // perhaps a non-IPv4 packet?
            debug!(
                "No route can be selected for a flow ID of {}.",
                packet_flow_id
            );

            // drops the packet without forwarding it
            return Err("No route can be selected.".to_string());
        }

        // Route the packet to its next hop
        let next_hop_id = self
            .routing_table
            .get_next_hop_by_route(route_id)
            .ok_or_else(|| {
                let error = format!(
                    "No next hop is found for route id {} on flow {}: routing inconsistency detected.",
                    route_id, packet_flow_id
                );
                error!("{}", error);

                error
            })?;

        debug!(
            "New packet flow {} selected route_id {} -> next_hop {}.",
            packet_flow_id, route_id, next_hop_id
        );

        self.send_packet_to_next_hop(packet, next_hop_id, packet_flow_id)
            .await
    }

    /// Unified packet sending method
    async fn send_packet_to_next_hop(
        &mut self,
        packet: Packet,
        next_hop_id: usize,
        packet_flow_id: FlowId,
    ) -> Result<(), String> {
        if next_hop_id == self.routing_table.local_id {
            // Local delivery
            self.tun_writer.write_packet(packet).await;
            Ok(())
        } else {
            match self.senders.get_mut(&next_hop_id) {
                Some(sender) => {
                    sender.send(packet).await;
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

    pub async fn run(&mut self) {
        // Do some intialization before starting the main loop
        let batch_size = 256;

        // Start the main loop
        loop {
            let packets = {
                let mut receiver = self.receiver_rx.write().await;
                let mut batch = Vec::with_capacity(batch_size);

                for i in 0..batch_size {
                    if self.should_shutdown.load(Ordering::Relaxed) {
                        return;
                    }

                    // Wait for packets to be available
                    let packet = if i == 0 {
                        receiver
                            .recv()
                            .await
                            .expect("Failed to receive data from receiver interface.")
                    } else {
                        // If we can continue to receive packet, do it
                        // Otherwise, break the inner loop to wait for more packets.
                        match receiver.try_recv() {
                            Ok(p) => p,
                            Err(_) => break,
                        }
                    };

                    batch.push(packet);
                }
                batch
            };

            // Process the batch of packets
            for packet in packets {
                // Report metrics for the packet
                self.metrics_tx
                    .send((
                        packet.flow_id,
                        self.routing_table.local_id,
                        packet.packet_size,
                    ))
                    .expect("Failed to send metrics to the metrics collector.");

                if let Err(error_msg) = self.process_packet(packet).await {
                    debug!("Packet dropped with error: {}.", error_msg);

                    continue;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SenderLoadBalancer {
    txs: Vec<mpsc::Sender<Packet>>,
    tx_current: usize,
    flow2proc: FxHashMap<FlowId, usize>,
    n_proc: usize,
}

impl SenderLoadBalancer {
    pub fn new(txs: Vec<mpsc::Sender<Packet>>) -> Self {
        let len = txs.len();
        Self {
            txs,
            tx_current: 0,
            flow2proc: FxHashMap::default(),
            n_proc: len,
        }
    }

    pub fn try_send(&mut self, packet: Packet) {
        let flow_id = packet.flow_id; // Extract flow_id early to avoid borrow issues
        let proc_id;

        if let Some(id) = self.flow2proc.get(&flow_id) {
            proc_id = *id;
        } else {
            proc_id = self.tx_current;
            self.flow2proc.insert(flow_id, proc_id);
            self.tx_current = (self.tx_current + 1) % self.n_proc;
        }

        let tx = self
            .txs
            .get_mut(proc_id)
            .expect("Error: Trying to send to a processor but the channel does not exist.");

        // Perform random early drop based on internal channel capacity
        if rand::random::<f32>() * 0.75 + 0.25
            < 1.0 - (tx.capacity() as f32 / INTERNAL_Q_SIZE as f32)
        {
            warn!("Random early drop for flow {}", flow_id,);
            return;
        };

        match tx.try_send(packet) {
            Err(e) => {
                error!(
                    "Failed to send packet for flow {} to processor {}. Error: {:?}.",
                    flow_id, proc_id, e
                );

                return;
            }
            Ok(_) => {
                return;
            }
        };
    }
}
