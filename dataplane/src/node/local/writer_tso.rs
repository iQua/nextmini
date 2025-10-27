use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use tokio::time::Duration;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
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
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
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

                        // only when it is a TCP packet, it is reordered when needed and stored in the queue
                        let sequenced_packet = SequencedPacket { seq: packet.seq_num(), packet };

                        // adds packet to queue
                        {
                            let mut queue_map = self.queue_map.lock().await;
                            let heap = queue_map.entry(flow_id).or_insert_with(BinaryHeap::new);
                            heap.push(sequenced_packet);
                        }

                        // ensures the flow is tracked when we add a TCP data packet
                        {
                            let mut active_flows = self.active_flows.lock().await;
                            active_flows.insert(flow_id);
                        }

                        // notifies the consumer that the queue has accumulated packets beyond a threshold, so packets are
                        // guaranteed to be consumed in a relatively ordered manner
                        if heap.len() >= self.config.reorder_tolerance {
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
                        let flow_id = {
                            let active_flows = self.active_flows.lock().await;
                            if let Some(&flow_id) = active_flows.iter().next() {
                                flow_id
                            } else {
                                break;
                            }
                        };

                        let sequenced_packet = {
                            let mut queue_map = self.queue_map.lock().await;
                            if let Some(heap) = queue_map.get_mut(&flow_id) {
                                heap.pop()
                            } else {
                                None
                            }
                        };

                        if let Some(sp) = sequenced_packet {
                            let packet = sp.packet;
                            pending_packets.push(packet);

                            // proactively flushes in the hot path to ensure progress under load
                            if pending_packets.len() >= IDEAL_BATCH_SIZE {
                                let _ = batch_writer.write(&mut pending_packets).await;
                                pending_packets.clear();
                            }

                            // checks if the queue is now empty after popping
                            let is_empty = {
                                let queue_map = self.queue_map.lock().await;
                                if let Some(heap) = queue_map.get(&flow_id) {
                                    heap.is_empty()
                                } else {
                                    true
                                }
                            };

                            if is_empty {
                                let mut active_flows = self.active_flows.lock().await;
                                active_flows.remove(&flow_id);
                            }
                        } else {
                            let mut active_flows = self.active_flows.lock().await;
                            active_flows.remove(&flow_id);
                        }
                    }

                    // sends packets
                    let _ = batch_writer.write(&mut pending_packets).await;
                    pending_packets.clear();
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
