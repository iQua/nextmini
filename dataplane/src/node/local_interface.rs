use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tracing::{error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder};

#[cfg(target_os = "linux")]
use tun_rs::{GROTable, IDEAL_BATCH_SIZE, VIRTIO_NET_HDR_LEN};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::{Feature, LocalConfig};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt};

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

            #[cfg(target_os = "linux")]
            {
                let mut reader = LocalReader {
                    shutdown_receiver: shutdown_sender.subscribe(),
                    device: dev.clone(),
                    processor: processor.clone(),
                    original_buffer: vec![0; VIRTIO_NET_HDR_LEN + 65535],
                    packet_buffers: vec![vec![0u8; 1500]; IDEAL_BATCH_SIZE],
                    packet_sizes: vec![0; IDEAL_BATCH_SIZE],
                };

                tokio::spawn(async move {
                    reader.run().await;
                });
            }
            #[cfg(not(target_os = "linux"))]
            {
                let mut reader = LocalReader {
                    shutdown_receiver: shutdown_sender.subscribe(),
                    processor: processor.clone(),
                    device: dev.clone(),
                };

                tokio::spawn(async move {
                    reader.run().await;
                });
            }

            let mut writer = LocalWriter::new(
                config.clone(),
                dev.clone(),
                shutdown_sender.subscribe(),
                write_receiver,
            );

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
        let idx = packet.flow_id.hash(self.write_senders.len());
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
                .multi_queue(true) // enables multi-queue support
                .offload(true) // enables TSO support
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
#[cfg(target_os = "linux")]
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
    // for TSO support on Linux
    original_buffer: Vec<u8>,
    packet_buffers: Vec<Vec<u8>>,
    packet_sizes: Vec<usize>,
}

#[cfg(not(target_os = "linux"))]
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
}

impl LocalReader {
    async fn run(&mut self) {
        #[cfg(target_os = "linux")]
        {
            loop {
                tokio::select! {
                    msg = self.shutdown_receiver.recv() => {
                        if let Ok(ShutdownMessage::Shutdown) = msg {
                                info!("LocalReader received shutdown signal, stopping...");
                                break;
                        }
                    }
                    // receives packets with TSO support
                    result = self.device.recv_multiple(
                        &mut self.original_buffer,
                        &mut self.packet_buffers,
                        &mut self.packet_sizes,
                        0
                    ) => {
                        let num_packets = match result {
                            Ok(num) => num,
                            Err(e) => {
                                error!("Failed to read from TUN device: {:?}", e);
                                continue;
                            }
                        };

                        // processes each packet
                        for i in 0..num_packets {
                            let packet_size = self.packet_sizes[i];
                            // skips empty packets
                            if packet_size == 0 {
                                warn!("LocalReader received an empty packet."); //keeps the original design
                                continue;
                            }

                            // uses slice to avoid copying
                            let packet = Packet::from_slice(packet_size, &self.packet_buffers[i]);

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
        #[cfg(not(target_os = "linux"))]
        {
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

                        let packet = Packet::new(n, buf.to_vec());

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

/// Writes one packet to a TUN device.
#[cfg(target_os = "linux")]
struct SequentialLocalWriter {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    gro_table: GROTable,
    packet_buffers: Vec<Vec<u8>>,
}
#[cfg(not(target_os = "linux"))]
struct SequentialLocalWriter {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
}

impl SequentialLocalWriter {
    #[cfg(target_os = "linux")]
    fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        packet_receiver: mpsc::Receiver<LocalInterfaceMessage>,
    ) -> Self {
        Self {
            device,
            shutdown_receiver,
            packet_receiver,
            gro_table: GROTable::default(),
            packet_buffers: Vec::new(),
        }
    }

    #[cfg(not(target_os = "linux"))]
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

    #[cfg(target_os = "linux")]
    async fn run(&mut self) {
        let mut pending_packets: Vec<Packet> = Vec::new();
        let batch_timeout = tokio::time::Duration::from_micros(100);

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                        if !pending_packets.is_empty() {
                            let _ = self.send_batch(&mut pending_packets).await;
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

                        // sends packets if the buffer reaches IDEAL_BATCH_SIZE
                        if pending_packets.len() >= IDEAL_BATCH_SIZE {
                            let _ = self.send_batch(&mut pending_packets).await;
                        }
                    }
                }
                // sends to the TUN device anyway every once in a while (100 milliseconds)
                _ = tokio::time::sleep(batch_timeout), if !pending_packets.is_empty() => {
                    let _ = self.send_batch(&mut pending_packets).await;
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn send_batch(&mut self, packets: &mut Vec<Packet>) -> Result<(), std::io::Error> {
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

    #[cfg(not(target_os = "linux"))]
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

    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.packet_receiver.recv() => {
                    if let Some(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let flow_id = packet.flow_id;

                        // sends the packet out to the TUN device if it is not a TCP packet, or if it is SYN, FIN,
                        // RST, or ACK
                        if !packet.is_tcp_data() {
                            let buf = &packet.buf[0..packet.packet_size];
                            if let Err(e) = self.device.send(buf).await {
                                error!("Failed to send packet to TUN device: {:?}", e);
                            }

                            continue;
                        }

                        // if it is a TCP packet, it is sequenced and stored in the queue
                        let sequenced_packet = SequencedPacket { seq: packet.seq_num(), packet };
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
