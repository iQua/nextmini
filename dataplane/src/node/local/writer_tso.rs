use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
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
                    device,
                    shutdown_receiver,
                    packet_receiver,
                    Duration::from_micros(config.delay_tolerance),
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
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    device: Arc<AsyncDevice>,
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
    expected_seq_map: Arc<Mutex<HashMap<FlowId, u32>>>,
    gap_deadlines: Arc<Mutex<HashMap<FlowId, Instant>>>,
}

impl ConcurrentLocalWriterProducer {
    pub fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
        gap_timeout: Duration,
    ) -> Self {
        let queue_map = Arc::new(Mutex::new(HashMap::new()));
        let active_flows = Arc::new(Mutex::new(HashSet::new()));
        let queue_not_empty = Arc::new(Notify::new());
        let expected_seq_map = Arc::new(Mutex::new(HashMap::new()));
        let gap_deadlines = Arc::new(Mutex::new(HashMap::new()));

        let consumer = ConcurrentLocalWriterConsumer {
            shutdown_receiver: shutdown_receiver.resubscribe(),
            queue_map: queue_map.clone(),
            active_flows: active_flows.clone(),
            device: device.clone(),
            queue_not_empty: queue_not_empty.clone(),
            expected_seq_map: expected_seq_map.clone(),
            gap_deadlines: gap_deadlines.clone(),
            gap_timeout,
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
            gap_deadlines,
        }
    }

    pub async fn run(&mut self) {
        let mut batch_writer = BatchLocalWriter::new(self.device.clone());

        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        if !packet.is_tcp_data() {
                            // seeds on SYN; keep expectation monotonic on FIN/RST
                            if packet.is_tcp_syn() {
                                let start = packet.seq_num().wrapping_add(1);
                                let mut exp = self.expected_seq_map.lock().await;
                                exp.insert(flow_id, start);

                                let mut active = self.active_flows.lock().await;
                                active.insert(flow_id);

                                let mut deadlines = self.gap_deadlines.lock().await;
                                deadlines.remove(&flow_id);
                            } else if packet.is_tcp_fin_or_rst() {
                                // do not remove expectation; FIN may arrive out of order
                                let fin_next = packet.seq_num().wrapping_add(1);
                                let mut exp = self.expected_seq_map.lock().await;
                                exp.entry(flow_id).and_modify(|e| {
                                    // keep it monotonic in TCP sequence space
                                    if SequencedPacket::seq_less(*e, fin_next) {
                                        *e = fin_next;
                                    }
                                }).or_insert(fin_next);

                                let mut deadlines = self.gap_deadlines.lock().await;
                                deadlines.remove(&flow_id);
                            }

                            // forwards control packets immediately
                            let mut single = vec![packet];
                            if let Err(e) = batch_writer.write(&mut single).await {
                                error!("Failed to send packet to TUN device: {:?}", e);
                            }
                            continue;
                        }

                        // TCP data -> enqueue
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
    gap_deadlines: Arc<Mutex<HashMap<FlowId, Instant>>>,
    gap_timeout: Duration,
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
                    loop {
                        // snapshots active flows
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
                                        None => { exp.insert(flow_id, top_seq); top_seq }
                                    }
                                };

                                loop {
                                    let maybe_stale = {
                                        let mut q = self.queue_map.lock().await;
                                        if let Some(h) = q.get_mut(&flow_id) {
                                            if let Some(peek) = h.peek() {
                                                if SequencedPacket::seq_less(peek.seq, expected) {
                                                    h.pop()
                                                } else { None }
                                            } else { None }
                                        } else { None }
                                    };

                                    if let Some(stale) = maybe_stale {
                                        pending_packets.push(stale.packet);
                                        progressed_any = true;

                                        if pending_packets.len() >= IDEAL_BATCH_SIZE {
                                            let _ = batch_writer.write(&mut pending_packets).await;
                                            pending_packets.clear();
                                        }

                                        continue 'per_flow;
                                    }

                                    break;
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

                                        pending_packets.push(packet);
                                        {
                                            let mut deadlines = self.gap_deadlines.lock().await;
                                            deadlines.remove(&flow_id);
                                        }
                                        progressed_any = true;

                                        if pending_packets.len() >= IDEAL_BATCH_SIZE {
                                            let _ = batch_writer.write(&mut pending_packets).await;
                                            pending_packets.clear();
                                        }

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

                                let now = Instant::now();
                                let mut arm_timer = false;
                                let mut advance_expected = false;
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
                                            deadlines.insert(flow_id, now + self.gap_timeout);
                                            arm_timer = true;
                                        }
                                    }
                                }

                                if arm_timer {
                                    let notify = self.queue_not_empty.clone();
                                    let delay = self.gap_timeout;
                                    tokio::spawn(async move {
                                        tokio::time::sleep(delay).await;
                                        notify.notify_one();
                                    });
                                }

                                if advance_expected {
                                    {
                                        let mut exp = self.expected_seq_map.lock().await;
                                        let entry = exp.entry(flow_id).or_insert(top_seq);
                                        if SequencedPacket::seq_less(*entry, top_seq) || *entry == top_seq {
                                            *entry = top_seq;
                                        }
                                    }
                                    warn!(
                                        flow_id = ?flow_id,
                                        expected = expected,
                                        next_in_queue = top_seq,
                                        "Gap timer expired; advancing expected sequence."
                                    );
                                    progressed_any = true;
                                    continue 'per_flow;
                                }

                                break 'per_flow;
                            }
                        }

                        if !progressed_any {
                            // no in-order progress possible right now
                            break;
                        }
                    }

                    // flushes any accumulated packets
                    if !pending_packets.is_empty() {
                        let _ = batch_writer.write(&mut pending_packets).await;
                        pending_packets.clear();
                    }
                }
                // periodic flush
                _ = tokio::time::sleep(batch_timeout), if !pending_packets.is_empty() => {
                    let _ = batch_writer.write(&mut pending_packets).await;
                    pending_packets.clear();
                }
            }
        }
    }
}
