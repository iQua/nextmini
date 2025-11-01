use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tokio::time::{Duration, Instant};
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
                let (enforce, gap_timeout, backlog) = config.reorder_tolerances();
                LocalWriter::Concurrent(Box::new(ConcurrentLocalWriterProducer::new(
                    device,
                    shutdown_receiver,
                    packet_receiver,
                    gap_timeout,
                    backlog,
                    enforce,
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
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    device: Arc<AsyncDevice>,
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
    expected_seq_map: Arc<Mutex<HashMap<FlowId, u32>>>,
    gap_deadlines: Arc<Mutex<HashMap<FlowId, Instant>>>,
    enforce_order: bool,
}

impl ConcurrentLocalWriterProducer {
    fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
        gap_timeout: Option<Duration>,
        backlog_tolerance: usize,
        enforce_order: bool,
    ) -> Self {
        let queue_map = Arc::new(Mutex::new(HashMap::new()));
        let active_flows = Arc::new(Mutex::new(HashSet::new()));
        let queue_not_empty = Arc::new(Notify::new());
        let expected_seq_map = Arc::new(Mutex::new(HashMap::new()));
        let gap_deadlines = Arc::new(Mutex::new(HashMap::new()));

        if enforce_order {
            let consumer = ConcurrentLocalWriterConsumer {
                shutdown_receiver: shutdown_receiver.resubscribe(),
                queue_map: queue_map.clone(),
                active_flows: active_flows.clone(),
                device: device.clone(),
                queue_not_empty: queue_not_empty.clone(),
                expected_seq_map: expected_seq_map.clone(),
                gap_deadlines: gap_deadlines.clone(),
                gap_timeout,
                backlog_tolerance,
                enforce_order,
            };

            tokio::spawn(async move {
                let mut consumer = consumer;
                consumer.run().await;
            });
        }

        Self {
            shutdown_receiver,
            packet_receiver,
            device,
            queue_map,
            active_flows,
            queue_not_empty,
            expected_seq_map,
            gap_deadlines,
            enforce_order,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        if !packet.is_tcp_data() {
                            if self.enforce_order {
                                if packet.is_tcp_syn() {
                                    let start = packet.seq_num().wrapping_add(1);
                                    let mut exp = self.expected_seq_map.lock().await;
                                    exp.insert(flow_id, start);

                                    let mut active = self.active_flows.lock().await;
                                    active.insert(flow_id);

                                    let mut deadlines = self.gap_deadlines.lock().await;
                                    deadlines.remove(&flow_id);
                                } else if packet.is_tcp_fin_or_rst() {
                                    let fin_next = packet.seq_num().wrapping_add(1);
                                    let mut exp = self.expected_seq_map.lock().await;
                                    exp.entry(flow_id).and_modify(|e| {
                                        if SequencedPacket::seq_less(*e, fin_next) {
                                            *e = fin_next;
                                        }
                                    }).or_insert(fin_next);

                                    let mut deadlines = self.gap_deadlines.lock().await;
                                    deadlines.remove(&flow_id);
                                }
                            }

                            // sends the packet immediately to the TUN interface
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(_) = self.device.try_send(buf) {
                                if let Err(e) = self.device.send(buf).await {
                                    error!("Failed to write packet to the TUN device: {}. Dropped.", e);
                                }
                            }

                            continue;
                        }

                        // sends the packet immediately to the TUN interface if we are not enforcing TCP order
                        if !self.enforce_order {
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(_) = self.device.try_send(buf) {
                                if let Err(e) = self.device.send(buf).await {
                                    error!("Failed to write packet to the TUN device: {}. Dropped.", e);
                                }
                            }

                            continue;
                        }

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
    enforce_order: bool,
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    device: Arc<AsyncDevice>,
    queue_not_empty: Arc<Notify>,
    expected_seq_map: Arc<Mutex<HashMap<FlowId, u32>>>,
    gap_deadlines: Arc<Mutex<HashMap<FlowId, Instant>>>,
    gap_timeout: Option<Duration>,
    backlog_tolerance: usize,
}

impl ConcurrentLocalWriterConsumer {
    async fn run(&mut self) {
        if !self.enforce_order {
            return;
        }

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        info!("ConcurrentLocalWriter consumer received shutdown signal, stopping...");
                        break;
                    }
                }
                _ = self.queue_not_empty.notified() => {
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
                            'per_flow: loop {
                                let top_seq_opt = {
                                    let q = self.queue_map.lock().await;
                                    q.get(&flow_id).and_then(|h| h.peek().map(|sp| sp.seq))
                                };

                                let top_seq = match top_seq_opt {
                                    Some(s) => s,
                                    None => {
                                        let mut active = self.active_flows.lock().await;
                                        active.remove(&flow_id);
                                        {
                                            let mut deadlines = self.gap_deadlines.lock().await;
                                            deadlines.remove(&flow_id);
                                        }
                                        break 'per_flow;
                                    }
                                };

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

                                loop {
                                    let maybe_stale = {
                                        let mut q = self.queue_map.lock().await;
                                        if let Some(h) = q.get_mut(&flow_id) {
                                            if let Some(peek) = h.peek() {
                                                if SequencedPacket::seq_less(peek.seq, expected) {
                                                    h.pop()
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    };

                                    if let Some(stale) = maybe_stale {
                                        let packet = stale.packet;
                                        let buf = &packet.buf[0..packet.packet_size];
                                        if let Err(_) = self.device.try_send(buf) {
                                            if let Err(e) = self.device.send(buf).await {
                                                error!("Failed to write stale packet to the TUN device: {}. Dropped.", e);
                                            }
                                        }

                                        progressed_any = true;
                                        continue 'per_flow;
                                    }

                                    break;
                                }

                                if self.backlog_tolerance > 0 && top_seq != expected {
                                    let backlog_len = {
                                        let q = self.queue_map.lock().await;
                                        q.get(&flow_id).map(|h| h.len()).unwrap_or(0)
                                    };

                                    if backlog_len >= self.backlog_tolerance {
                                        {
                                            let mut deadlines = self.gap_deadlines.lock().await;
                                            deadlines.remove(&flow_id);
                                        }
                                        {
                                            let mut exp = self.expected_seq_map.lock().await;
                                            let entry = exp.entry(flow_id).or_insert(top_seq);
                                            if SequencedPacket::seq_less(*entry, top_seq) || *entry == top_seq {
                                                *entry = top_seq;
                                            }
                                        }

                                        progressed_any = true;
                                    }
                                }

                                if top_seq == expected {
                                    let maybe_sp = {
                                        let mut q = self.queue_map.lock().await;
                                        if let Some(h) = q.get_mut(&flow_id) {
                                            h.pop()
                                        } else { None }
                                    };

                                    if let Some(sp) = maybe_sp {
                                        let packet = sp.packet;

                                        let exp_now = {
                                            let exp = self.expected_seq_map.lock().await;
                                            *exp.get(&flow_id).unwrap_or(&expected)
                                        };
                                        let payload = packet.tcp_payload_len() as u32;
                                        let fin_inc = if packet.is_tcp_fin_or_rst() { 1 } else { 0 };
                                        let next_expected = exp_now.wrapping_add(payload + fin_inc);
                                        {
                                            let mut exp = self.expected_seq_map.lock().await;
                                            let e = exp.entry(flow_id).or_insert(next_expected);
                                            if SequencedPacket::seq_less(*e, next_expected) {
                                                *e = next_expected;
                                            }
                                        }

                                        let buf = &packet.buf[0..packet.packet_size];
                                        if let Err(_) = self.device.try_send(buf) {
                                            if let Err(e) = self.device.send(buf).await {
                                                error!("Failed to write packet to the TUN device: {}. Dropped.", e);
                                            }
                                        }

                                        {
                                            let mut deadlines = self.gap_deadlines.lock().await;
                                            deadlines.remove(&flow_id);
                                        }

                                        progressed_any = true;

                                        let is_empty = {
                                            let q = self.queue_map.lock().await;
                                            match q.get(&flow_id) {
                                                Some(h) => h.is_empty(),
                                                None => true,
                                            }
                                        };
                                        if is_empty {
                                            let mut active = self.active_flows.lock().await;
                                            active.remove(&flow_id);
                                            {
                                                let mut deadlines = self.gap_deadlines.lock().await;
                                                deadlines.remove(&flow_id);
                                            }
                                        }

                                        continue 'per_flow;
                                    } else {
                                        break 'per_flow;
                                    }
                                }

                                if SequencedPacket::seq_less(top_seq, expected) {
                                    let mut q = self.queue_map.lock().await;
                                    if let Some(h) = q.get_mut(&flow_id) {
                                        h.pop();
                                    }
                                    progressed_any = true;
                                    continue 'per_flow;
                                }

                                if let Some(gap_timeout) = self.gap_timeout {
                                    let now = Instant::now();
                                    let mut arm_timer = false;

                                    {
                                        let mut deadlines = self.gap_deadlines.lock().await;
                                        match deadlines.get(&flow_id).copied() {
                                            Some(deadline) => {
                                                if now >= deadline {
                                                    deadlines.remove(&flow_id);
                                                    advance_expected = true;
                                                }
                                            }
                                            None => {
                                                deadlines.insert(flow_id, now + gap_timeout);
                                                arm_timer = true;
                                            }
                                        }
                                    }

                                    if arm_timer {
                                        let notify = self.queue_not_empty.clone();
                                        tokio::spawn(async move {
                                            tokio::time::sleep(gap_timeout).await;
                                            notify.notify_one();
                                        });
                                    }
                                }

                                let mut advance_expected = false;

                                if advance_expected {
                                    {
                                        let mut exp = self.expected_seq_map.lock().await;
                                        let entry = exp.entry(flow_id).or_insert(top_seq);
                                        if SequencedPacket::seq_less(*entry, top_seq) || *entry == top_seq {
                                            *entry = top_seq;
                                        }
                                    }

                                    progressed_any = true;
                                    continue 'per_flow;
                                }

                                break 'per_flow;
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
