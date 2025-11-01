use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tracing::{error, info};
use tun_rs::AsyncDevice;

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

    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                            info!("LocalWriter received shutdown signal, stopping...");
                            break;
                    }
                }
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let mut buffer = vec![packet];

                        while let Ok(message) = self.packet_receiver.try_recv() {
                            match message {
                                LocalInterfaceMessage::WritePacket(p) => {
                                    buffer.push(p);
                                }
                            }
                        }

                        for packet in buffer {
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(_) = self.device.try_send(buf) {
                                if let Err(e) = self.device.send(buf).await {
                                    error!(
                                        "Failed to write packet to the TUN device: {}. Dropped.",
                                        e
                                    );
                                }
                            }
                        };
                    }
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
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
}

impl ConcurrentLocalWriterProducer {
    fn new(
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

    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        // sends the packet out to the TUN device if it is not a TCP data packet, including
                        // the cases where it is SYN, FIN, RST, or pure ACK packet
                        if !packet.is_tcp_data() {
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(e) = self.device.send(buf).await {
                                error!("Failed to send packet to TUN device: {:?}", e);
                            }

                            continue;
                        }

                        // only when it is a TCP packet, it is reordered when needed and stored in the queue
                        let sequenced_packet = SequencedPacket { seq: packet.seq_num(), packet };

                        // adds this packet to the queue and check if we should notify while holding the lock
                        let should_notify = {
                            let mut queue_map = self.queue_map.lock().await;
                            let heap = queue_map.entry(flow_id).or_insert_with(BinaryHeap::new);
                            heap.push(sequenced_packet);
                            heap.len() >= self.config.reorder_tolerance
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
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    device: Arc<AsyncDevice>,
    queue_not_empty: Arc<Notify>,
    reorder_tolerance: usize,
    expected_seq: HashMap<FlowId, u32>,
}

impl ConcurrentLocalWriterConsumer {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.queue_not_empty.notified() => {
                    loop {
                        let flow_id = {
                            let active_flows = self.active_flows.lock().await;
                            if let Some(&flow_id) = active_flows.iter().next() {
                                flow_id
                            } else {
                                break;
                            }
                        };

                        // snapshot top seq and heap length
                        let (top_seq_opt, heap_len) = {
                            let queue_map = self.queue_map.lock().await;
                            if let Some(heap) = queue_map.get(&flow_id) {
                                (heap.peek().map(|sp| sp.seq), heap.len())
                            } else {
                                (None, 0)
                            }
                        };

                        // nothing buffered for this flow
                        let top_seq = match top_seq_opt {
                            Some(s) => s,
                            None => {
                                let mut active_flows = self.active_flows.lock().await;
                                active_flows.remove(&flow_id);
                                self.expected_seq.remove(&flow_id);
                                continue;
                            }
                        };

                        // establishes expected seq once enough packets have accumulated
                        if !self.expected_seq.contains_key(&flow_id) {
                            if heap_len >= self.reorder_tolerance {
                                self.expected_seq.insert(flow_id, top_seq);
                            } else {
                                // wait for more before starting delivery
                                continue;
                            }
                        }

                        // drains contiguous prefix
                        loop {
                            let maybe_sp = {
                                let mut queue_map = self.queue_map.lock().await;
                                if let Some(heap) = queue_map.get_mut(&flow_id) {
                                    if let Some(peek) = heap.peek() {
                                        if let Some(expected) = self.expected_seq.get(&flow_id) {
                                            if peek.seq == *expected {
                                                heap.pop()
                                            } else {
                                                None
                                            }
                                        } else { None }
                                    } else { None }
                                } else { None }
                            };

                            if let Some(sp) = maybe_sp {
                                let packet = sp.packet;

                                // advances the expected sequence number by payload length (wrap-aware)
                                let payload = packet.tcp_payload_len() as u32;
                                let old_expected = self.expected_seq[&flow_id];
                                let next_expected = old_expected.wrapping_add(payload);
                                self.expected_seq.insert(flow_id, next_expected);

                                let buf = &packet.buf[0..packet.packet_size];
                                if let Err(_) = self.device.try_send(buf) {
                                    if let Err(e) = self.device.send(buf).await {
                                        error!(
                                            "Failed to write packet to the TUN device: {}. Dropped.",
                                            e
                                        );
                                    }
                                }

                                // if the heap is empty now, retire the flow
                                let is_empty = {
                                    let queue_map = self.queue_map.lock().await;
                                    queue_map.get(&flow_id).map_or(true, |h| h.is_empty())
                                };
                                if is_empty {
                                    let mut active_flows = self.active_flows.lock().await;
                                    active_flows.remove(&flow_id);
                                    self.expected_seq.remove(&flow_id);
                                    break;
                                }
                            } else {
                                // gap encountered
                                break;
                            }
                        }
                    }
                }
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        info!("ConcurrentLocalWriter consumer received shutdown signal, stopping...");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_less_orders_increasing_values() {
        assert!(
            SequencedPacket::seq_less(100, 200),
            "lower sequence numbers should compare as less"
        );
        assert!(
            !SequencedPacket::seq_less(200, 100),
            "higher sequence numbers should not be considered less"
        );
    }

    #[test]
    fn seq_less_handles_wraparound_correctly() {
        let near_max = u32::MAX - 5;
        assert!(
            SequencedPacket::seq_less(near_max, 10),
            "numbers near wraparound should precede small sequence numbers"
        );
        assert!(
            !SequencedPacket::seq_less(10, near_max),
            "new sequence numbers after wrap should not precede the tail end"
        );
    }

    #[test]
    fn binary_heap_respects_sequence_order_with_wraparound() {
        let mut heap = BinaryHeap::new();
        let sequences = [u32::MAX - 1, 0, 1, 500, u32::MAX];

        for &seq in &sequences {
            heap.push(SequencedPacket {
                seq,
                packet: Packet {
                    flow_id: seq as u128,
                    packet_size: 0,
                    buf: Vec::new(),
                },
            });
        }

        let mut result = Vec::new();
        while let Some(entry) = heap.pop() {
            result.push(entry.seq);
        }

        assert_eq!(
            result,
            vec![u32::MAX - 1, u32::MAX, 0, 1, 500],
            "heap should yield packets in ascending sequence order accounting for wraparound"
        );
    }
}
