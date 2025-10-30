use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tokio::time::Duration;
use tracing::{error, info};
use tun_rs::{AsyncDevice, GROTable, IDEAL_BATCH_SIZE, VIRTIO_NET_HDR_LEN};

use crate::node::FlowId;
use crate::node::config::{Feature, LocalConfig};
use crate::node::local::interface::{LocalInterfaceMessage, ShutdownMessage};
use crate::node::packet::Packet;

pub enum LocalWriter {
    Sequential(SequentialLocalWriter),
    Concurrent(Box<ConcurrentLocalWriterProducer>),
}

impl LocalWriter {
    pub fn new(
        config: LocalConfig,
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    ) -> Self {
        match &config.feature {
            Feature::Sequential => LocalWriter::Sequential(SequentialLocalWriter::new(
                device,
                shutdown_receiver,
                packet_receiver,
            )),
            Feature::Concurrent => {
                LocalWriter::Concurrent(Box::new(ConcurrentLocalWriterProducer::new(
                    config,
                    device,
                    shutdown_receiver,
                    packet_receiver,
                )))
            }
        }
    }

    pub async fn run(&mut self) {
        match self {
            LocalWriter::Sequential(writer) => writer.run().await,
            LocalWriter::Concurrent(writer_producer) => writer_producer.run().await,
        }
    }
}

/// Writes a batch of packets to a TUN device.
struct BatchLocalWriter {
    device: Arc<AsyncDevice>,
    gro_table: GROTable,
    packet_buffers: Vec<Vec<u8>>,
}

impl BatchLocalWriter {
    fn new(device: Arc<AsyncDevice>) -> Self {
        Self {
            device,
            gro_table: GROTable::default(),
            packet_buffers: Vec::new(),
        }
    }

    async fn write(&mut self, packets: &mut Vec<Packet>) -> Result<(), std::io::Error> {
        if packets.is_empty() {
            return Ok(());
        }

        // prepares buffers for TSO
        self.packet_buffers.clear();
        self.packet_buffers.reserve(packets.len());

        // needs memory copying for now
        for packet in packets.iter() {
            let mut buf = vec![0; VIRTIO_NET_HDR_LEN + packet.packet_size];
            buf[VIRTIO_NET_HDR_LEN..VIRTIO_NET_HDR_LEN + packet.packet_size]
                .copy_from_slice(&packet.buf[..packet.packet_size]);

            self.packet_buffers.push(buf);
        }

        match self
            .device
            .send_multiple(
                &mut self.gro_table,
                &mut self.packet_buffers,
                VIRTIO_NET_HDR_LEN,
            )
            .await
        {
            Ok(_) => {
                packets.clear();
                Ok(())
            }
            Err(e) => {
                // if batch sending fails, falls back to sending packets individually
                for packet in packets.drain(..) {
                    let buf = &packet.buf[0..packet.packet_size];
                    let _ = self.device.send(buf).await;
                }

                Err(e)
            }
        }
    }
}

/// Writes packets to a TUN device.
pub struct SequentialLocalWriter {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
}

impl SequentialLocalWriter {
    fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    ) -> Self {
        Self {
            device,
            shutdown_receiver,
            packet_receiver,
        }
    }

    pub async fn run(&mut self) {
        let mut pending_packets: Vec<Packet> = Vec::new();
        let batch_timeout = Duration::from_millis(1);
        let mut batch_writer = BatchLocalWriter::new(self.device.clone());

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        if !pending_packets.is_empty() {
                            let _ = batch_writer.write(&mut pending_packets).await;
                        }

                        info!("LocalWriter received shutdown signal, stopping...");
                        break;
                    }
                }
                // receives packets from the mpsc channel
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        pending_packets.push(packet);

                        while let Ok(message) = self.packet_receiver.try_recv() {
                            match message {
                                LocalInterfaceMessage::WritePacket(p) => {
                                    pending_packets.push(p);
                                }
                            }
                        }

                        // sends packets
                        let _ = batch_writer.write(&mut pending_packets).await;
                        pending_packets.clear();
                    }
                }
                // sends to the TUN device anyway every once in a while (1 millisecond)
                _ = tokio::time::sleep(batch_timeout), if !pending_packets.is_empty() => {
                    let _ = batch_writer.write(&mut pending_packets).await;
                    pending_packets.clear();
                }
            }
        }
    }
}

struct SequencedPacket {
    seq: u32,
    packet: Packet,
}

impl SequencedPacket {
    fn seq_less(a: u32, b: u32) -> bool {
        let diff = a.wrapping_sub(b) as i32;
        diff < 0
    }
}

impl PartialEq for SequencedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}

impl Eq for SequencedPacket {}

impl PartialOrd for SequencedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SequencedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.seq == other.seq {
            Ordering::Equal
        } else if Self::seq_less(self.seq, other.seq) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

pub struct ConcurrentLocalWriterProducer {
    config: LocalConfig,
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    device: Arc<AsyncDevice>,
    // Map flow -> ordered map of seq -> list of packets starting at that seq
    queue_map: Arc<Mutex<HashMap<FlowId, BTreeMap<u32, Vec<Packet>>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
}

impl ConcurrentLocalWriterProducer {
    pub fn new(
        config: LocalConfig,
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    ) -> Self {
        let queue_map = Arc::new(Mutex::new(HashMap::new()));
        let active_flows = Arc::new(Mutex::new(HashSet::new()));
        let queue_not_empty = Arc::new(Notify::new());

        let consumer = ConcurrentLocalWriterConsumer {
            shutdown_receiver: shutdown_receiver.resubscribe(),
            queue_map: queue_map.clone(),
            active_flows: active_flows.clone(),
            device: device.clone(),
            queue_not_empty: queue_not_empty.clone(),
            reorder_tolerance: config.reorder_tolerance,
            expected_seq: HashMap::new(),
        };

        tokio::spawn(async move {
            let mut consumer = consumer;
            consumer.run().await;
        });

        Self {
            config,
            shutdown_receiver,
            packet_receiver,
            device,
            queue_map,
            active_flows,
            queue_not_empty,
        }
    }

    pub async fn run(&mut self) {
        let mut batch_writer = BatchLocalWriter::new(self.device.clone());

        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        // sends the packet out to the TUN device if it is not a TCP data packet, including
                        // the cases where it is SYN, FIN, RST, or pure ACK packet
                        if !packet.is_tcp_data() {
                            if let Err(e) = batch_writer.write(&mut vec![packet]).await {
                                error!("Failed to send packet to TUN device: {:?}", e);
                            }

                            continue;
                        }

                        // only when it is a TCP data packet, it is reordered when needed and stored in the map by sequence
                        let seq = packet.seq_num();

                        // adds packet to per-flow ordered map and check if we should notify while holding the lock
                        let should_notify = {
                            let mut qm = self.queue_map.lock().await;
                            let flow_map = qm.entry(flow_id).or_insert_with(BTreeMap::new);
                            flow_map.entry(seq).or_insert_with(Vec::new).push(packet);
                            // approximate readiness by total buffered segments
                            let buffered = flow_map.len();
                            buffered >= self.config.reorder_tolerance
                        };

                        // ensures the flow is tracked when we add a TCP data packet
                        {
                            let mut active_flows = self.active_flows.lock().await;
                            active_flows.insert(flow_id);
                        }

                        // notifies the consumer that the queue has accumulated packets beyond a threshold, so packets are
                        // guaranteed to be consumed in a relatively ordered manner
                        if should_notify {
                            self.queue_not_empty.notify_one();
                        }
                    }
                }
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        info!("ConcurrentLocalWriter producer received shutdown signal, stopping...");
                        break;
                    }
                }
            }
        }
    }
}

struct ConcurrentLocalWriterConsumer {
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    queue_map: Arc<Mutex<HashMap<FlowId, BTreeMap<u32, Vec<Packet>>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    device: Arc<AsyncDevice>,
    queue_not_empty: Arc<Notify>,
    reorder_tolerance: usize,
    expected_seq: HashMap<FlowId, u32>,
}

impl ConcurrentLocalWriterConsumer {
    async fn run(&mut self) {
        let mut pending_packets: Vec<Packet> = Vec::new();
        let batch_timeout = Duration::from_millis(1);
        let mut batch_writer = BatchLocalWriter::new(self.device.clone());

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        info!("ConcurrentLocalWriter consumer received shutdown signal, stopping...");
                        if !pending_packets.is_empty() {
                            let _ = batch_writer.write(&mut pending_packets).await;
                        }
                        break;
                    }
                }
                _ = self.queue_not_empty.notified() => {
                    // drains packets as long as we can make progress on any flow
                    loop {
                        let flow_ids: Vec<FlowId> = {
                            let active = self.active_flows.lock().await;
                            active.iter().copied().collect()
                        };

                        if flow_ids.is_empty() {
                            break;
                        }

                        let mut progressed_any = false;

                        for flow_id in flow_ids {
                            // snapshot the top sequence and current heap length
                            let (top_seq_opt, buffered_len) = {
                                let q = self.queue_map.lock().await;
                                if let Some(m) = q.get(&flow_id) {
                                    (m.keys().next().copied(), m.len())
                                } else {
                                    (None, 0)
                                }
                            };

                            // if there are no packets, clean up this flow
                            let top_seq = match top_seq_opt {
                                Some(s) => s,
                                None => {
                                    let mut active = self.active_flows.lock().await;
                                    active.remove(&flow_id);
                                    self.expected_seq.remove(&flow_id);
                                    continue;
                                }
                            };

                            // establishes the expected sequence once enough packets have accumulated
                            if !self.expected_seq.contains_key(&flow_id) {
                                if buffered_len >= self.reorder_tolerance {
                                    self.expected_seq.insert(flow_id, top_seq);
                                } else {
                                    // wait for more before starting delivery
                                    continue;
                                }
                            }

                            // drains contiguous prefix
                            loop {
                                // Try to drop stale entries < expected, then deliver exactly matching expected
                                let expected = self.expected_seq[&flow_id];
                                let mut just_progressed = false;
                                {
                                    let mut q = self.queue_map.lock().await;
                                    if let Some(m) = q.get_mut(&flow_id) {
                                        // drop stale sequences strictly less than expected (wrap-aware)
                                        loop {
                                            let first = m.keys().next().copied();
                                            if let Some(seq) = first {
                                                if SequencedPacket::seq_less(seq, expected) {
                                                    m.pop_first();
                                                    progressed_any = true;
                                                    just_progressed = true;
                                                    continue;
                                                }
                                            }
                                            break;
                                        }

                                        // deliver exactly expected if present
                                        if let Some(mut vec_pkts) = m.remove(&expected) {
                                            if let Some(packet) = vec_pkts.pop() {
                                                // if duplicates existed, drop the rest
                                                drop(vec_pkts);

                                                // SYN and FIN flags each consume one sequence number even with zero payload
                                                let payload = packet.tcp_payload_len() as u32;
                                                let syn_fin_adjust = if packet.has_tcp_syn_or_fin() { 1 } else { 0 };
                                                let next_expected = expected.wrapping_add(payload + syn_fin_adjust);
                                                self.expected_seq.insert(flow_id, next_expected);

                                                pending_packets.push(packet);
                                                progressed_any = true;
                                                just_progressed = true;
                                            } else {
                                                // empty vec shouldn't happen
                                            }
                                        } else {
                                            // nothing to deliver now
                                        }
                                    }
                                }

                                // proactive flush under load
                                if pending_packets.len() >= IDEAL_BATCH_SIZE {
                                    let _ = batch_writer.write(&mut pending_packets).await;
                                    pending_packets.clear();
                                }

                                // if the map is empty now for this flow, retire it
                                let is_empty = {
                                    let q = self.queue_map.lock().await;
                                    q.get(&flow_id).is_none_or(|m| m.is_empty())
                                };

                                if is_empty {
                                    let mut active = self.active_flows.lock().await;
                                    active.remove(&flow_id);
                                    self.expected_seq.remove(&flow_id);
                                    break;
                                }

                                if !just_progressed {
                                    // gap encountered, cannot progress on this flow at the moment
                                    break;
                                }
                            }
                        }

                        if !progressed_any {
                            break;
                        }
                    }

                    // sends any accumulated packets
                    let _ = batch_writer.write(&mut pending_packets).await;
                    pending_packets.clear();
                }
                // periodic flush of pending batch (does not override in-order gating)
                _ = tokio::time::sleep(batch_timeout), if !pending_packets.is_empty() => {
                    let _ = batch_writer.write(&mut pending_packets).await;
                    pending_packets.clear();
                }
            }
        }
    }
}
