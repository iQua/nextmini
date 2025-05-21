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
use crate::dataplane::local_interface::TunWriter;
use crate::dataplane::node_interface::NodeSender;
use crate::dataplane::packet::Packet;
use crate::dataplane::routes::RoutingTable;
use crate::dataplane::{FlowId, context::Context, metrics::MetricsTx};
use crate::dataplane::{NodeId, SocketId};

pub struct ProcessorManager {
    context: Context,
    proc_handles: VecDeque<tokio::task::JoinHandle<()>>,
    proc_shutdown_flags: VecDeque<Arc<AtomicBool>>,
    receiver_rxs: VecDeque<Arc<RwLock<mpsc::Receiver<Packet>>>>,
    stream2routes: Arc<RwLock<FxHashMap<(FlowId, SocketId), u8>>>,
    stream_counters: Arc<RwLock<FxHashMap<FlowId, usize>>>,
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
            stream2routes: Arc::new(RwLock::new(FxHashMap::default())),
            stream_counters: Arc::new(RwLock::new(FxHashMap::default())),
            routing_table,
        }
    }

    pub async fn update_routes(&mut self, routing_table: RoutingTable) {
        self.routing_table.merge(routing_table);
        // Update the routing table with local stream assignments
        let stream2routes = self.stream2routes.read().await;
        for (key, path_id) in stream2routes.iter() {
            if self.routing_table.get_path_id(&key.0, &key.1).is_none() {
                self.routing_table
                    .insert_stream_mapping(key.0, key.1, *path_id);
            }
        }
        drop(stream2routes);
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
            let table = self.routing_table.clone();
            let senders = self.context.reproduce_senders().await;
            let tun_writers = self.context.get_tun_writers(i).await;
            let stream2routes = self.stream2routes.clone();
            let stream_counters = self.stream_counters.clone();
            let metrics_tx = self.context.get_metrics_tx();
            let handle = tokio::task::spawn(async move {
                let mut proc = Processor::new(
                    receiver_rx,
                    table,
                    senders,
                    tun_writers,
                    stream2routes,
                    stream_counters,
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

const FLOW_ID_IF_MASK: u64 = 0x00000000_0000FF00;

pub struct Processor {
    // The channel receiver to obtain packets from NodeReceiver or TUN reader
    receiver_rx: Arc<RwLock<mpsc::Receiver<Packet>>>,

    // The routing table
    routing_table: RoutingTable,

    // The senders that send packets to the network
    senders: FxHashMap<NodeId, NodeSender>,

    // The mapping from stream id to route id. This is used as a local storage persistent across the life cycle
    // of processors for streams that are not assigned a path in the routing table
    stream2routes: Arc<RwLock<FxHashMap<(FlowId, SocketId), u8>>>,

    // Count the number of streams in each flow. This is used to assign a path to a stream via round robin.
    stream_counters: Arc<RwLock<FxHashMap<FlowId, usize>>>,

    // The local interface writers
    tun_writers: Vec<TunWriter>,

    // The flag to indicate if the processor should shutdown
    should_shutdown: Arc<AtomicBool>,

    // The metrics collector channel
    metrics_tx: MetricsTx,
}

impl Processor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receiver_rx: Arc<RwLock<mpsc::Receiver<Packet>>>,
        routing_table: RoutingTable,
        senders: FxHashMap<NodeId, NodeSender>,
        tun_writers: Vec<TunWriter>,
        stream2routes: Arc<RwLock<FxHashMap<(FlowId, SocketId), u8>>>,
        stream_counters: Arc<RwLock<FxHashMap<FlowId, usize>>>,
        should_shutdown: Arc<AtomicBool>,
        metrics_tx: MetricsTx,
    ) -> Self {
        Self {
            receiver_rx,
            routing_table,
            senders,
            stream2routes,
            stream_counters,
            tun_writers,
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

                // Wait for packets to be avaiable
                let mut packet = if i == 0 {
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

                // Preprocess the packet based on its stream id (only used if the multi-path method is "stream")
                if packet.has_stream_id {
                    // If we can get a stream id, then we assign a path to this stream
                    let path_id = if let Some(path_id) = self
                        .routing_table
                        .get_path_id(&packet.flow_id, &packet.stream_id)
                    {
                        // First, we check if routing table has a path assigned for this stream
                        path_id
                    } else {
                        // If not, we assign a new path to this stream
                        let path_count = *self
                            .stream_counters
                            .write()
                            .await
                            .entry(packet.flow_id)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                        let path_id =
                            path_count % self.routing_table.get_num_paths(&packet.flow_id);
                        self.stream2routes
                            .write()
                            .await
                            .insert((packet.flow_id, packet.stream_id), path_id as u8);
                        self.routing_table.insert_stream_mapping(
                            packet.flow_id,
                            packet.stream_id,
                            path_id as u8,
                        );
                        path_id as u8
                    };
                    packet.update_route(path_id); // The 14th byte is the third byte of the IPv4 addr, which we use to set the route.
                    // We are also going to report the metrics to the metrics collector
                    self.metrics_tx
                        .send((
                            packet.flow_id,
                            packet.stream_id,
                            self.routing_table.local_id,
                            packet.packet_size,
                        ))
                        .expect("Failed to send metrics to the metrics collector.");
                }

                // Find the next hop and send the packet
                if let Some(next_hop) = self.routing_table.next_hop(&packet.flow_id) {
                    // Sending out the packet
                    if *next_hop == self.routing_table.local_id {
                        // We must select the correct interface with the correct interface id
                        // e.g. For addr flow_id 0A0000010A000202 (10,0,0,1 to 10.0.2.2):
                        // 0A0000010A000202 * FLOW_ID_IFMASK >> 8 = 0000000000000200 >> 8 = 2
                        let if_id = (packet.flow_id & FLOW_ID_IF_MASK) >> 8;

                        self.tun_writers.get_mut(if_id as usize)
                            .unwrap_or_else(|| panic!("Destination interface {if_id} doesn't exist. Hint: check if the number of paths configured and set are consistent."))
                            .write_packet(packet).await;
                    } else {
                        match self.senders.get_mut(next_hop) {
                            Some(sender) => {
                                sender.send(packet).await;
                            }
                            None => {
                                // Route defined, but the node doesn't exist yet.
                                println!(
                                    "WARNING: Route {:?} defined, but the node {} is offline. Packet dropped",
                                    &packet.flow_id.to_be_bytes(),
                                    next_hop
                                );
                                continue;
                            }
                        }
                    }
                } else {
                    // Not route was found for the packet
                    // println!(
                    //     "WARNING: No route found for the packet with flow id {:?}.",
                    //     &packet.flow_id.to_be_bytes()
                    // );
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
    stream2proc: FxHashMap<(SocketId, FlowId), usize>,
    flow2proc: FxHashMap<FlowId, usize>,
    n_proc: usize,
}

impl SenderLoadBalancer {
    pub fn new(txs: Vec<mpsc::Sender<Packet>>) -> Self {
        let len = txs.len();
        Self {
            txs,
            tx_current: 0,
            stream2proc: FxHashMap::default(),
            flow2proc: FxHashMap::default(),
            n_proc: len,
        }
    }

    pub fn try_send(&mut self, packet: Packet) {
        let proc_id;

        if packet.has_stream_id {
            if let Some(id) = self.stream2proc.get(&(packet.stream_id, packet.flow_id)) {
                proc_id = *id;
            } else {
                proc_id = self.tx_current;

                self.stream2proc
                    .insert((packet.stream_id, packet.flow_id), proc_id);

                self.tx_current = (self.tx_current + 1) % self.n_proc;
            }
        } else if let Some(id) = self.flow2proc.get(&packet.flow_id) {
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
            Err(_) => return,
            Ok(_) => return,
        };
    }
}
