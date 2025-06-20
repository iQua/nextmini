use std::sync::Arc;

use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, warn};

use nextmini_messages::TokenBucketSpec;

use crate::node::FlowId;
use crate::node::config::LocalConfig;
use crate::node::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::token_bucket::TokenBucket;

/// The scheduling discipline.
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum SchedulingDiscipline {
    #[default]
    Fifo,
    Wrr,
}

/// The types of messages sent to the scheduler.
pub enum SchedulerReaderMessage {
    InboundPacket(Packet),
    SetFlowWeight(FlowId, usize),
}

/// The rate limit is to be sent by the processor, and in the unit of bytes per second.
pub enum SchedulerWriterMessage {
    RateLimit(TokenBucketSpec),
    OutboundPackets(Vec<Packet>),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone)]
pub struct SchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerReaderMessage>,
    writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        // creates the mpsc channel for sending packets to the scheduler
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);

        // creates the unbounded mpsc channel for sending a rate limit, in bytes (per second), to the scheduler
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();

        let scheduler = match config.scheduler_type {
            SchedulingDiscipline::Fifo => {
                Fifo::new(config, net_interface, reader_receiver, writer_receiver)
            }
            _ => {
                panic!("This scheduling discipline has not yet been implemented.");
            }
        };

        scheduler.run();

        Self {
            reader_sender,
            writer_sender,
        }
    }

    /// Sends a packet to the scheduler.
    pub fn send(&self, packet: Packet) {
        if let Err(e) = self
            .reader_sender
            .try_send(SchedulerReaderMessage::InboundPacket(packet))
        {
            error!(
                "SchedulerHandle: Error sending a packet to the scheduler: {}.",
                e
            );
        }
    }

    /// Limits the rate of sending packets the outbound network connection, in bytes/second.
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::RateLimit(spec))
        {
            error!(
                "SchedulerHandle: Error sending a rate limit to the scheduler: {}.",
                e
            );
        }
    }
}

/// FIFO is a scheduling discipline that schedules packets in a first-in-first-out manner.
pub struct Fifo {
    config: LocalConfig,
}

impl Fifo {
    pub fn new(
        config: LocalConfig,
        net_interface: NetworkInterfaceHandle,
        reader_receiver: mpsc::Receiver<SchedulerReaderMessage>,
        writer_receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    ) -> Self {
        let capacity = config.queue_capacity;
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match config.scheduler_drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        let scheduler_queue = Arc::new(ArrayQueue::new(capacity));
        let queue_not_empty = Arc::new(Notify::new());

        let mut reader = FifoReader {
            queue: scheduler_queue.clone(),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            queue_not_empty: queue_not_empty.clone(),
            capacity,
            receiver: reader_receiver,
        };

        let mut writer = FifoWriter {
            queue: scheduler_queue,
            net_interface,
            queue_not_empty,
            receiver: writer_receiver,
            token_bucket: None,
        };

        tokio::task::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::task::spawn(async move {
            let _ = writer.run().await;
        });

        Self { config }
    }

    pub fn run(&self) {
        // This method is intentionally left empty as the actual run logic is handled in the
        // FifoReader and FifoWriter tasks spawned above.
        debug!(
            "A {:?} scheduler has just been started.",
            self.config.scheduler_type
        );
    }
}

struct FifoReader {
    pub queue: Arc<ArrayQueue<Packet>>,
    /// the number of packets dropped so far
    pub packets_dropped: usize,
    /// a closure that determines whether an inbound packet should be dropped or not
    pub drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    /// signals when the queue has packets to be consumed
    pub queue_not_empty: Arc<Notify>,
    /// maximum queue capacity
    pub capacity: usize,
    /// the receiver for an mpsc channel, for other actors to send packets to this reader
    pub receiver: mpsc::Receiver<SchedulerReaderMessage>,
}

impl FifoReader {
    async fn run(&mut self) {
        // producer task: receives packets and enqueues them
        loop {
            if let Some(message) = self.receiver.recv().await {
                match message {
                    SchedulerReaderMessage::InboundPacket(packet) => {
                        self.enqueue(packet);
                    }
                }

                while let Ok(message) = self.receiver.try_recv() {
                    match message {
                        SchedulerReaderMessage::InboundPacket(packet) => {
                            self.enqueue(packet);
                        }
                    }
                }
            }
        }
    }

    fn enqueue(&mut self, packet: Packet) {
        // drops the packet based on the drop strategy
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, self.queue.len(), self.queue.len());

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;

            warn!(
                "FIFO: Scheduler dropped a packet for flow {} (size: {}). Queue length: {}/{}, packets dropped: {}",
                packet.flow_id,
                packet.packet_size,
                self.queue.len(),
                self.capacity,
                self.packets_dropped
            );

            return;
        }

        let is_tcp_data = packet.is_tcp_data();

        if self.queue.push(packet).is_err() {
            self.packets_dropped += 1;

            warn!("FIFO: Scheduler dropped a packet as the queue is full.");
        } else {
            // notifies the writer task if it is not a TCP packet, or if it is SYN, FIN, RST, or ACK
            // if it is a TCP packet, it is stored in the queue for a while before being consumed by the writer task
            if is_tcp_data {
                if self.queue.len() > 2 {
                    // if the queue length is over a threshold, it notifies the consumer task that a packet has arrived
                    // and the queue becomes 'non-empty' now
                    self.queue_not_empty.notify_one();
                }
            } else {
                self.queue_not_empty.notify_one();
            }
        }
    }
}

struct FifoWriter {
    pub queue: Arc<ArrayQueue<Packet>>,
    /// the network interface handle
    pub net_interface: NetworkInterfaceHandle,
    /// signals when the queue has packets to be consumed
    pub queue_not_empty: Arc<Notify>,
    /// the receiver for an unbounded mpsc channel, for other actors to send packets to this writer
    pub receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    /// the token bucket traffic shaper
    token_bucket: Option<TokenBucket>,
}

// Consumer task: pops packets from the queue and sends them out
impl FifoWriter {
    async fn run(&mut self) {
        let mut batch = Vec::new();

        loop {
            // receives and imposes rate limits from the controller interface, if available
            while let Ok(message) = self.receiver.try_recv() {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                }
            }

            // waits for notification if the queue is empty
            if self.queue.is_empty() {
                self.queue_not_empty.notified().await;
            }

            // drains packets from the queue efficiently
            while let Some(packet) = self.queue.pop() {
                batch.push(packet);
            }

            // sends the batch if we have packets
            if !batch.is_empty() {
                self.send_packets(&mut batch).await;
            }
        }
    }
    async fn send_packets(&mut self, batch: &mut Vec<Packet>) {
        let packets = std::mem::take(batch);
        let packet_count = packets.len();

        // if needed, calculates total bytes before sending the packets out
        if let Some(ref mut token_bucket) = self.token_bucket {
            token_bucket.send(&mut self.net_interface, packets).await;
        } else {
            if let Err(e) = self.net_interface.send(packets).await {
                error!(
                    "FifoWriter: Error sending batch of {} packets: {}",
                    packet_count, e
                );

                return;
            }
        }
    }
}

pub struct Wrr {
    config: LocalConfig,
}

impl Wrr {
    pub fn new(
        config: LocalConfig,
        net_interface: NetworkInterfaceHandle,
        reader_receiver: mpsc::Receiver<SchedulerReaderMessage>,
        writer_receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    ) -> Self {
        let capacity = config.queue_capacity;
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match config.scheduler_drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        let scheduler_queues = Arc::new(Vec::new());
        let flow_weights = Arc::new(Vec::new());

        // To be implemented:
        // Map the flow id to a class id

        let flow_to_class = Arc::new(|flow_id| flow_id);
        let queue_not_empty = Arc::new(Notify::new());

        let mut reader = WrrReader {
            queues: scheduler_queues.clone(),
            flow_weights: flow_weights.clone(),
            flow_to_class: flow_to_class.clone(),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            queue_not_empty: queue_not_empty.clone(),
            capacity,
            receiver: reader_receiver,
        };

        let mut writer = WrrWriter {
            queues: scheduler_queues.clone(),
            flow_weights: flow_weights.clone(),
            net_interface,
            queue_not_empty: queue_not_empty.clone(),
            receiver: writer_receiver,
            token_bucket: None,
        };

        tokio::task::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::task::spawn(async move {
            let _ = writer.run().await;
        });

        Self { config }
    }

    pub fn run(&self) {
        // This method is intentionally left empty as the actual run logic is handled in the
        // WrrReader and WrrWriter tasks spawned above.
        debug!(
            "A {:?} scheduler has just been started.",
            self.config.scheduler_type
        );
    }
}

pub struct WrrReader {
    pub queues: Vec<Arc<ArrayQueue<Packet>>>,
    /// the flow weights
    pub flow_weights: Vec<usize>,
    /// the function to map a flow id to a class id
    pub flow_to_class: Arc<dyn Fn(FlowId) -> usize + Send + Sync>,
    /// the number of packets dropped so far
    pub packets_dropped: usize,
    /// a closure that determines whether an inbound packet should be dropped or not
    pub drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    /// the queue not empty signal for each flow
    pub queue_not_empty: Arc<Notify>,
    /// the capacity of the queue
    pub capacity: usize,
    /// the receiver for an mpsc channel, for other actors to send packets to this reader
    pub receiver: mpsc::Receiver<SchedulerReaderMessage>,
    /// the sender for an mpsc channel, for other actors to send packets to this reader
    pub writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
}

impl WrrReader {
    async fn run(&mut self) {}
    /// Push a packet into the correct queue based on the flow id
    fn enqueue(&mut self, packet: Packet) {
        // To be implemented
        // Create new queue for new flow
        // Maybe need RwLock for the queues, since queues are created at runtime

        let class_id = (self.flow_to_class)(packet.flow_id);
        let queue = self.queues[class_id].clone();

        // drops the packet based on the drop strategy
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, queue.len(), self.capacity);

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;

            warn!(
                "WRR: Scheduler dropped a packet for flow {} (size: {}). Queue length: {}/{}, packets dropped: {}",
                packet.flow_id,
                packet.packet_size,
                queue.len(),
                self.capacity,
                self.packets_dropped
            );

            return;
        }

        let is_tcp_data = packet.is_tcp_data();

        if queue.push(packet).is_err() {
            self.packets_dropped += 1;

            warn!("FIFO: Scheduler dropped a packet as the queue is full.");
        } else {
            // notifies the writer task if it is not a TCP packet, or if it is SYN, FIN, RST, or ACK
            // if it is a TCP packet, it is stored in the queue for a while before being consumed by the writer task
            if is_tcp_data {
                if queue.len() > 2 {
                    // if the queue length is over a threshold, it notifies the consumer task that a packet has arrived
                    // and the queue becomes 'non-empty' now
                    self.queue_not_empty.notify_one();
                }
            } else {
                self.queue_not_empty.notify_one();
            }
        }
    }
    async fn schedule_packet(&mut self) {
        loop {
            let mut packet_count = 0;
            let mut current_queue = 0;

            self.queue_not_empty.notified().await;
            let mut batch = Vec::new();
            loop {
                if packet_count < self.flow_weights[current_queue] {
                    if let Some(packet) = self.queues[current_queue].pop() {
                        batch.push(packet);
                        packet_count += 1;
                    }
                } else {
                    // Send the batch to the writer
                    self.writer_sender
                        .send(SchedulerWriterMessage::OutboundPackets(batch));

                    // Move to the next queue
                    current_queue = (current_queue + 1) % self.queues.len();
                    packet_count = 0;
                    batch = Vec::new();
                }
            }
        }
    }
    fn set_flow_weight(&mut self, flow_id: FlowId, weight: usize) {
        (self.flow_to_class)(flow_id);
        self.flow_weights.push(weight);
    }
}

pub struct WrrWriter {
    /// the network interface handle
    pub net_interface: NetworkInterfaceHandle,
    /// the receiver for an unbounded mpsc channel, for other actors to send packets to this writer
    pub receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    /// the token bucket traffic shaper
    token_bucket: Option<TokenBucket>,
}

impl WrrWriter {
    async fn run(&mut self) {
        loop {
            while let Ok(message) = self.receiver.try_recv() {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    SchedulerWriterMessage::OutboundPackets(mut batch) => {
                        self.send_packets(&mut batch).await;
                    }
                }
            }

            if let Some(message) = self.receiver.recv().await {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    SchedulerWriterMessage::OutboundPackets(mut batch) => {
                        self.send_packets(&mut batch).await;
                    }
                }
            }
        }
    }
    async fn send_packets(&mut self, batch: &mut Vec<Packet>) {
        let packets = std::mem::take(batch);
        let packet_count = packets.len();

        // if needed, calculates total bytes before sending the packets out
        if let Some(ref mut token_bucket) = self.token_bucket {
            token_bucket.send(&mut self.net_interface, packets).await;
        } else {
            if let Err(e) = self.net_interface.send(packets).await {
                error!(
                    "FifoWriter: Error sending batch of {} packets: {}",
                    packet_count, e
                );

                return;
            }
        }
    }
}
