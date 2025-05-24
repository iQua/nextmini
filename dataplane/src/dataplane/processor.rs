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
use crate::dataplane::routes::SimpleRoutingTable;
use crate::dataplane::{FlowId, context::Context, metrics::MetricsTx};
use nextmini_messages::SimpleRouteEntry;

pub struct ProcessorManager {
    context: Context,
    proc_handles: VecDeque<tokio::task::JoinHandle<()>>,
    proc_shutdown_flags: VecDeque<Arc<AtomicBool>>,
    receiver_rxs: VecDeque<Arc<RwLock<mpsc::Receiver<Packet>>>>,
    pub simple_routing_table: SimpleRoutingTable,
}

impl ProcessorManager {
    pub fn new(context: Context, mut receiver_rxs: Vec<mpsc::Receiver<Packet>>) -> Self {
        let mut rxs = VecDeque::new();
        let simple_routing_table = SimpleRoutingTable::new(context.local_id);
        for _ in 0..receiver_rxs.len() {
            let rx = receiver_rxs.pop().unwrap();
            rxs.push_back(Arc::new(RwLock::new(rx)));
        }
        Self {
            context,
            proc_handles: VecDeque::new(),
            proc_shutdown_flags: VecDeque::new(),
            receiver_rxs: rxs,
            simple_routing_table,
        }
    }

    pub async fn update_simple_routes(&mut self, routes: Vec<SimpleRouteEntry>) {
        // Set base IPv4 address for node ID calculation (should be configurable)
        self.simple_routing_table.set_base_ipv4_addr([10, 0, 0, 0]);

        // Install routes directly using SimpleRouteEntry
        self.simple_routing_table.install_routes(routes);
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
            let simple_table = self.simple_routing_table.clone();
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
    simple_routing_table: SimpleRoutingTable,

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
        simple_routing_table: SimpleRoutingTable,
        senders: FxHashMap<NodeId, NodeSender>,
        tun_writer: TunWriter,
        should_shutdown: Arc<AtomicBool>,
        metrics_tx: MetricsTx,
    ) -> Self {
        Self {
            receiver_rx,
            simple_routing_table,
            senders,
            tun_writer,
            should_shutdown,
            metrics_tx,
        }
    }

    pub async fn run(&mut self) {
        // Do some intialization before starting the main loop
        let batch_size = 256;
        let mut receiver = self.receiver_rx.write().await;

        // Start the main loop
        loop {
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

                // Report metrics for the packet (moved here as it's for non-local packets or packets to be routed)
                self.metrics_tx
                    .send((
                        packet.flow_id,
                        self.simple_routing_table.local_id,
                        packet.packet_size,
                    ))
                    .expect("Failed to send metrics to the metrics collector.");

                // Find the next hop and send the packet
                let next_hop = self.simple_routing_table.next_hop_for_flow(packet.flow_id);

                if let Some(next_hop_id) = next_hop {
                    let packet_flow_id = packet.flow_id; // Save flow_id before packet is moved

                    // Sending out the packet
                    if next_hop_id == self.simple_routing_table.local_id {
                        // Local delivery
                        self.tun_writer.write_packet(packet).await;
                    } else {
                        match self.senders.get_mut(&next_hop_id) {
                            Some(sender) => {
                                sender.send(packet).await;
                            }
                            None => {
                                // Route defined, but the node doesn't exist yet.
                                println!(
                                    "WARNING: Route defined for flow {}, but node {} is offline. Packet dropped.",
                                    packet_flow_id, next_hop_id
                                );
                                // No total processing log here as packet is dropped before full processing cycle completes in the same way
                                continue;
                            }
                        }
                    }
                } else {
                    // No route was found for the packet
                    println!("WARNING: No route was found. Packet dropped.");

                    // debug level logging
                    self.simple_routing_table.debug_print_routing_table(); 
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
        let proc_id;

        if let Some(id) = self.flow2proc.get(&packet.flow_id) {
            proc_id = *id;
        } else {
            proc_id = self.tx_current;
            self.flow2proc.insert(packet.flow_id, proc_id);
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
            return;
        };

        match tx.try_send(packet) {
            Err(_) => {
                return;
            }
            Ok(_) => {
                return;
            }
        };
    }
}
