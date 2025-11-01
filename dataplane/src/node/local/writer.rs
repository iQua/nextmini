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
            Feature::Concurrent => LocalWriter::Concurrent(Box::new(
                ConcurrentLocalWriterProducer::new(device, shutdown_receiver, packet_receiver),
            )),
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
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    device: Arc<AsyncDevice>,
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
    expected_seq_map: Arc<Mutex<HashMap<FlowId, u32>>>,
}

impl ConcurrentLocalWriterProducer {
    fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    ) -> Self {
        let queue_map = Arc::new(Mutex::new(HashMap::new()));
        let active_flows = Arc::new(Mutex::new(HashSet::new()));
        let queue_not_empty = Arc::new(Notify::new());
        let expected_seq_map = Arc::new(Mutex::new(HashMap::new()));

        let consumer = ConcurrentLocalWriterConsumer {
            shutdown_receiver: shutdown_receiver.resubscribe(),
            queue_map: queue_map.clone(),
            active_flows: active_flows.clone(),
            device: device.clone(),
            queue_not_empty: queue_not_empty.clone(),
            expected_seq_map: expected_seq_map.clone(),
        };

        tokio::spawn(async move {
            let mut consumer = consumer;
            consumer.run().await;
        });

        Self {
            shutdown_receiver,
            packet_receiver,
            device,
            queue_map,
            active_flows,
            queue_not_empty,
            expected_seq_map,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        // Non-data (SYN/FIN/RST/ACK-only) are forwarded immediately,
                        // but we also seed/cleanup expected sequence on control packets.
                        if !packet.is_tcp_data() {
                            // Seed expected from SYN so we don't accidentally
                            // "skip ahead" and later drop the first data segment.
                            if packet.is_tcp_syn() {
                                let start = packet.seq_num().wrapping_add(1); // SYN consumes 1
                                let mut exp = self.expected_seq_map.lock().await;
                                exp.insert(flow_id, start);

                                // Track flow so consumer will consider it once data arrives.
                                let mut active = self.active_flows.lock().await;
                                active.insert(flow_id);
                            } else if packet.is_tcp_fin_or_rst() {
                                // Tear down expectation to avoid leaking state.
                                let mut exp = self.expected_seq_map.lock().await;
                                exp.remove(&flow_id);
                            }

                            // Send immediately to TUN.
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(_) = self.device.try_send(buf) {
                                if let Err(e) = self.device.send(buf).await {
                                    error!("Failed to write packet to the TUN device: {}. Dropped.", e);
                                }
                            }
                            continue;
                        }

                        // TCP data: enqueue for potential reordering.
                        let sequenced_packet = SequencedPacket { seq: packet.seq_num(), packet };

                        {
                            let mut q = self.queue_map.lock().await;
                            let heap = q.entry(flow_id).or_insert_with(BinaryHeap::new);
                            heap.push(sequenced_packet);
                        }
                        {
                            let mut active = self.active_flows.lock().await;
                            active.insert(flow_id);
                        }

                        self.queue_not_empty.notify_one();
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
    expected_seq_map: Arc<Mutex<HashMap<FlowId, u32>>>,
}

impl ConcurrentLocalWriterConsumer {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        info!("ConcurrentLocalWriter consumer received shutdown signal, stopping...");
                        break;
                    }
                }
                _ = self.queue_not_empty.notified() => {
                    // drain while we can make progress on any flow
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
                            // snapshot heap top & len
                            let (top_seq_opt, heap_len) = {
                                let q = self.queue_map.lock().await;
                                if let Some(h) = q.get(&flow_id) {
                                    (h.peek().map(|sp| sp.seq), h.len())
                                } else {
                                    (None, 0)
                                }
                            };

                            let top_seq = match top_seq_opt {
                                Some(s) => s,
                                None => {
                                    let mut active = self.active_flows.lock().await;
                                    active.remove(&flow_id);
                                    // expected will be removed when the flow empties;
                                    // no-op if already absent.
                                    continue;
                                }
                            };

                            // get or lazily establish expected seq
                            let expected = {
                                let mut exp = self.expected_seq_map.lock().await;
                                match exp.get(&flow_id).copied() {
                                    Some(e) => e,
                                    None => {
                                        exp.insert(flow_id, top_seq);
                                        top_seq
                                    }
                                }
                            };

                            // drop stale (< expected) so they don't block progress
                            {
                                let mut q = self.queue_map.lock().await;
                                if let Some(h) = q.get_mut(&flow_id) {
                                    while let Some(peek) = h.peek() {
                                        if SequencedPacket::seq_less(peek.seq, expected) {
                                            h.pop();
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }

                            // pop next in-order segment (== expected)
                            let maybe_sp = {
                                let mut q = self.queue_map.lock().await;
                                if let Some(h) = q.get_mut(&flow_id) {
                                    if let Some(peek) = h.peek() {
                                        let exp_now = {
                                            let exp = self.expected_seq_map.lock().await;
                                            *exp.get(&flow_id).unwrap_or(&expected)
                                        };
                                        if peek.seq == exp_now {
                                            h.pop()
                                        } else {
                                            None
                                        }
                                    } else { None }
                                } else { None }
                            };

                            if let Some(sp) = maybe_sp {
                                let packet = sp.packet;

                                // compute next expected
                                let payload = packet.tcp_payload_len() as u32;
                                let fin_inc = if packet.is_tcp_fin_or_rst() { 1 } else { 0 };
                                let next_expected = expected.wrapping_add(payload + fin_inc);
                                {
                                    let mut exp = self.expected_seq_map.lock().await;
                                    exp.insert(flow_id, next_expected);
                                }

                                // send to TUN
                                let buf = &packet.buf[0..packet.packet_size];
                                if let Err(_) = self.device.try_send(buf) {
                                    if let Err(e) = self.device.send(buf).await {
                                        error!("Failed to write packet to the TUN device: {}. Dropped.", e);
                                    }
                                }

                                progressed_any = true;

                                // retire flow if heap emptied
                                let is_empty = {
                                    let q = self.queue_map.lock().await;
                                    q.get(&flow_id).map_or(true, |h| h.is_empty())
                                };
                                if is_empty {
                                    let mut active = self.active_flows.lock().await;
                                    active.remove(&flow_id);
                                    let mut exp = self.expected_seq_map.lock().await;
                                    exp.remove(&flow_id);
                                }
                            }
                        }

                        if !progressed_any {
                            break;
                        }
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
