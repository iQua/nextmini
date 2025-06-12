use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::{Feature, LocalConfig};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tracing::{error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder};

/// Message types for LocalInterface, which manages the LocalReader and LocalWriter actors.
#[derive(Clone)]
pub enum ShutdownMessage {
    Shutdown, // shuts down LocalInterface gracefully, stopping all LocalReader and LocalWriter actors
}

pub enum LocalInterfaceMessage {
    WritePacket(Packet), // the processor sends a packet to the application via the local interface
}

/// Handle for Processors to interact with LocalInterface
#[derive(Clone)]
pub struct LocalInterfaceHandle {
    shutdown_sender: broadcast::Sender<ShutdownMessage>,
    write_senders: Vec<mpsc::Sender<LocalInterfaceMessage>>,
}

impl LocalInterfaceHandle {
    pub fn new(config: LocalConfig, processor: ProcessorHandle) -> Self {
        // creates local TUN devices. On Linux, this creates multiple queues for parallel processing,
        // each queue corresponding to its own device. On non-Linux platforms, it creates one device only.
        let tun_devices = Self::create_tun_devices(config.clone());

        // a broadcast channel for sending the shutdown signal to both local interface readers and writers
        let (shutdown_sender, _) = broadcast::channel(config.channel_capacity);

        let mut write_senders = Vec::with_capacity(tun_devices.len());

        for dev in tun_devices.iter() {
            // for each LocalWriter, creates its MPSC channel
            let (write_sender, write_receiver) = mpsc::channel(config.channel_capacity);
            write_senders.push(write_sender);

            let mut reader = LocalReader {
                shutdown_receiver: shutdown_sender.subscribe(),
                processor: processor.clone(),
                device: dev.clone(),
            };

            let mut writer = LocalWriter::new(
                config.clone(),
                dev.clone(),
                shutdown_sender.subscribe(),
                write_receiver,
            );

            tokio::spawn(async move {
                reader.run().await;
            });

            tokio::spawn(async move {
                writer.run().await;
            });
        }

        Self {
            shutdown_sender,
            write_senders,
        }
    }

    pub fn write_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash() % self.write_senders.len();
        let sender = &self.write_senders[idx];

        if let Err(e) = sender.try_send(LocalInterfaceMessage::WritePacket(packet)) {
            error!(
                "Error sending a packet to the local interface writer: {}. Dropped.",
                e
            );
        }
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.shutdown_sender.send(ShutdownMessage::Shutdown) {
            error!("Error shutting down all the actors: {}", e);
        };
    }

    /// Converts a netmask tuple to prefix length. Used in 'LocalInterfaceHandle::create_tun_device()'.
    fn mask_to_prefix(mask: (u8, u8, u8, u8)) -> u8 {
        let mask_u32 = u32::from_be_bytes([mask.0, mask.1, mask.2, mask.3]);
        mask_u32.count_ones() as u8
    }

    /// Creates local TUN devices for communicating with the application.
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
        #[cfg(target_os = "linux")]
        {
            let num_queues = config.num_tun_queues;

            let if_name = config.tun_interface_name.clone();
            let ipv4_addr = config.local_address;
            let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

            let dev = DeviceBuilder::new()
                .name(&if_name)
                .ipv4(
                    Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                    ipv4_prefix,
                    None,
                )
                .mtu(config.mtu as u16)
                .multi_queue(true)
                .build_async()
                .expect("Failed to create tun device");

            let mut queues = Vec::with_capacity(num_queues);

            // creates multiple TUN queues with error handling
            info!("Creating {num_queues} TUN queues.");
            for _ in 0..num_queues - 1 {
                match dev.try_clone() {
                    Ok(cloned_dev) => {
                        queues.push(Arc::new(cloned_dev));
                    }
                    Err(e) => {
                        // if we are unable to create all the queues, use what we have
                        warn!(
                            "Could not create all TUN queues ({}), continuing with {} queues",
                            e,
                            queues.len()
                        );
                        break;
                    }
                }
            }

            queues.push(Arc::new(dev));

            // ensures that we have at least one queue
            if queues.is_empty() {
                panic!("Failed to create even a single TUN queue. Terminating.");
            }

            queues
        }

        #[cfg(not(target_os = "linux"))]
        {
            let ipv4_addr = config.local_address;
            let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

            let dev = DeviceBuilder::new()
                .ipv4(
                    Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                    ipv4_prefix,
                    None,
                )
                .mtu(config.mtu as u16)
                .build_async()
                .expect("Failed to create tun device");

            // creates a single TUN queue on non-Linux platforms without multi-queue support
            info!("Creating one TUN queue on non-Linux platforms without multi-queue support.");
            let queues = vec![Arc::new(dev)];

            queues
        }
    }
}

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
}

impl LocalReader {
    async fn run(&mut self) {
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                            info!("LocalReader received shutdown signal, stopping...");
                            break;
                    }
                }
                // reads from the local TUN device
                result = self.device.recv(&mut buf) => {
                    let n = match result {
                        Ok(n) => n,
                        Err(e) => {
                            error!(
                                "Failed to read from TUN device: {:?}: interface may be down. Retrying...",
                                e
                            );
                            continue;
                        }
                    };

                    // skips empty packets
                    if n == 0 {
                        warn!("LocalReader received an empty packet.");
                        continue;
                    }

                    let packet = Packet::new(n, buf);

                    // checks if packet creation was successful (non-zero flow_id indicates valid packet)
                    if packet.flow_id == 0 {
                        continue;
                    }

                    // sends to the processor for routing and forwarding
                    self.processor.process_packet(packet);
                }
            }
        }
    }
}

enum LocalWriter {
    Sequential(SequentialLocalWriter),
    Concurrent(ConcurrentLocalWriterProducer),
}

impl LocalWriter {
    fn new(
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
            Feature::Concurrent => LocalWriter::Concurrent(ConcurrentLocalWriterProducer::new(
                config,
                device,
                shutdown_receiver,
                packet_receiver,
            )),
        }
    }

    async fn run(&mut self) {
        match self {
            LocalWriter::Sequential(writer) => writer.run().await,
            LocalWriter::Concurrent(writer_producer) => writer_producer.run().await,
        }
    }
}

pub trait LocalWriterExt {
    async fn run(&mut self);
}

/// Writes one packet to a TUN device.
struct SequentialLocalWriter {
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
}

impl LocalWriterExt for SequentialLocalWriter {
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

struct ConcurrentLocalWriterProducer {
    config: LocalConfig,
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    device: Arc<AsyncDevice>,
    queue_map: Arc<Mutex<HashMap<FlowId, BinaryHeap<SequencedPacket>>>>,
    active_flows: Arc<Mutex<HashSet<FlowId>>>,
    queue_not_empty: Arc<Notify>,
}

impl LocalWriterExt for ConcurrentLocalWriterProducer {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;
                        let ihl = (packet.buf[0] & 0x0F) as usize;
                        let ip_header_len = ihl * 4;
                        let tcp_offset = ip_header_len;

                        // extracts the sequence number from the TCP header
                        let seq_num = u32::from_be_bytes([
                            packet.buf[tcp_offset + 4],
                            packet.buf[tcp_offset + 5],
                            packet.buf[tcp_offset + 6],
                            packet.buf[tcp_offset + 7],
                        ]);

                        // sends the packet out to the TUN device if it is not a TCP packet, or if it is SYN, FIN, RST, or ACK
                        // if it is a TCP packet, it is sequenced and stored in the queue

                        // checks if the packet is TCP by examining the protocol field in the IP header (offset 9)
                        let is_tcp = packet.buf[9] == 6; // 6 is the protocol number for TCP

                        // extracts TCP flags from the TCP header (offset +13 contains the flags byte)
                        let tcp_flags = packet.buf[tcp_offset + 13];

                        // checks specific TCP flags
                        let is_syn = is_tcp && (tcp_flags & 0x02) != 0; // SYN flag is bit 1
                        let is_fin = is_tcp && (tcp_flags & 0x01) != 0; // FIN flag is bit 0
                        let is_rst = is_tcp && (tcp_flags & 0x04) != 0; // RST flag is bit 2
                        let is_ack = is_tcp && (tcp_flags & 0x10) != 0; // ACK flag is bit 4

                        // Send immediately for non-TCP or control packets
                        if !is_tcp || is_syn || is_fin || is_rst || is_ack {
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(e) = self.device.send(buf).await {
                                error!("Failed to send packet to TUN device: {:?}", e);
                            }
                            continue;
                        }

                        // if it is a TCP packet without these flags, it is sequenced and stored in the queue
                        let sequenced_packet = SequencedPacket { seq: seq_num, packet };
                        {
                            let mut queue_map = self.queue_map.lock().await;
                            let heap = queue_map.entry(flow_id).or_insert_with(BinaryHeap::new);
                            let was_empty = heap.is_empty();
                            heap.push(sequenced_packet);

                            if was_empty {
                                let mut active_flows = self.active_flows.lock().await;
                                active_flows.insert(flow_id);
                            }

                            // notifies the consumer that the queue has accumulated packets beyond a threshold, so packets are
                            // guaranteed to be consumed in a relatively ordered manner
                            if heap.len() > self.config.reorder_tolerance {
                                self.queue_not_empty.notify_one();
                            }
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
                            let buf = &packet.buf[0..packet.packet_size];

                            let _ = self.device.send(buf).await;

                            let queue_map = self.queue_map.lock().await;
                            if let Some(heap) = queue_map.get(&flow_id) {
                                if heap.is_empty() {
                                    let mut active_flows = self.active_flows.lock().await;
                                    active_flows.remove(&flow_id);
                                }
                            }
                        } else {
                            let mut active_flows = self.active_flows.lock().await;
                            active_flows.remove(&flow_id);
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
