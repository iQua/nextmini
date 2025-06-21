use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

pub enum SchedulerMessage {
    RateLimit(TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
}

pub enum SchedulerPacket {
    InboundPacket(Packet),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone)]
pub enum SchedulerHandle {
    Fifo(FifoSchedulerHandle),
    Wrr(WrrSchedulerHandle),
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        match config.scheduler_type {
            SchedulingDiscipline::Fifo => {
                SchedulerHandle::Fifo(FifoSchedulerHandle::new(config, net_interface))
            }
            SchedulingDiscipline::Wrr => {
                SchedulerHandle::Wrr(WrrSchedulerHandle::new(config, net_interface))
            }
        }
    }
    pub fn send(&self, packet: Packet) {
        match self {
            SchedulerHandle::Fifo(scheduler) => scheduler.send(packet),
            SchedulerHandle::Wrr(scheduler) => scheduler.send(packet),
        }
    }
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        match self {
            SchedulerHandle::Fifo(scheduler) => scheduler.limit_rate(spec),
            SchedulerHandle::Wrr(scheduler) => scheduler.limit_rate(spec),
        }
    }
    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        match self {
            SchedulerHandle::Wrr(scheduler) => scheduler.set_flow_weight(flow_id, weight),
            SchedulerHandle::Fifo(scheduler) => scheduler.set_flow_weight(flow_id, weight),
        }
    }
}

#[derive(Clone)]
pub struct FifoSchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerPacket>,
    writer_sender: mpsc::UnboundedSender<SchedulerMessage>,
}

impl FifoSchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        // creates the mpsc channel for sending packets to the scheduler
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);

        // creates the unbounded mpsc channel for sending a rate limit, in bytes (per second), to the scheduler
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();

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

        Self {
            reader_sender,
            writer_sender,
        }
    }

    /// Sends a packet to the scheduler.
    pub fn send(&self, packet: Packet) {
        if let Err(e) = self
            .reader_sender
            .try_send(SchedulerPacket::InboundPacket(packet))
        {
            error!(
                "SchedulerHandle: Error sending a packet to the scheduler: {}.",
                e
            );
        }
    }

    /// Limits the rate of sending packets the outbound network connection, in bytes/second.
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        if let Err(e) = self.writer_sender.send(SchedulerMessage::RateLimit(spec)) {
            error!(
                "SchedulerHandle: Error sending a rate limit to the scheduler: {}.",
                e
            );
        }
    }

    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        // Intentionally left empty
        // FIFO scheduler does not support flow weights
        debug!(
            "FIFO scheduler does not support setting flow weights {} to {}.",
            flow_id, weight
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
    pub receiver: mpsc::Receiver<SchedulerPacket>,
}

impl FifoReader {
    async fn run(&mut self) {
        // producer task: receives packets and enqueues them
        loop {
            if let Some(message) = self.receiver.recv().await {
                match message {
                    SchedulerPacket::InboundPacket(packet) => {
                        self.enqueue(packet);
                    }
                }

                while let Ok(message) = self.receiver.try_recv() {
                    match message {
                        SchedulerPacket::InboundPacket(packet) => {
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
    pub receiver: mpsc::UnboundedReceiver<SchedulerMessage>,
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
                    SchedulerMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    _ => {}
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

#[derive(Clone)]
pub struct WrrSchedulerHandle {
    packet_sender: mpsc::Sender<SchedulerPacket>,
    message_sender: mpsc::UnboundedSender<SchedulerMessage>,
}

impl WrrSchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
        let (message_sender, message_receiver) = mpsc::unbounded_channel();

        let mut scheduler = Wrr::new(config, net_interface, packet_receiver, message_receiver);

        tokio::task::spawn(async move {
            scheduler.run().await;
        });

        Self {
            packet_sender,
            message_sender,
        }
    }

    /// Sends a packet to the scheduler.
    pub fn send(&self, packet: Packet) {
        if let Err(e) = self
            .packet_sender
            .try_send(SchedulerPacket::InboundPacket(packet))
        {
            error!(
                "WrrSchedulerHandle: Error sending a packet to the scheduler: {}.",
                e
            );
        }
    }

    /// Limits the rate of sending packets the outbound network connection, in bytes/second.
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        if let Err(e) = self.message_sender.send(SchedulerMessage::RateLimit(spec)) {
            error!(
                "WrrSchedulerHandle: Error sending a rate limit to the scheduler: {}.",
                e
            );
        }
    }

    /// Sets the flow weights for WRR scheduling.
    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .message_sender
            .send(SchedulerMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "WrrSchedulerHandle: Error sending flow weights to the scheduler: {}.",
                e
            );
        }
    }
}

pub struct Wrr {
    net_interface: NetworkInterfaceHandle,
    packet_receiver: mpsc::Receiver<SchedulerPacket>,
    message_receiver: mpsc::UnboundedReceiver<SchedulerMessage>,

    /// a closure that maps a flow_id to a class_id
    flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// weights of classes, which are consecutive and start from 0
    weights: Vec<usize>,

    /// the number of packets dropped
    packets_dropped: usize,

    /// the number of packets waiting to be sent
    packets_waiting: usize,

    /// the number of bytes in each class queue
    total_bytes: usize,

    /// maximum queue capacity
    capacity: usize,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<ArrayQueue<Packet>>,

    /// the token bucket traffic shaper
    token_bucket: Option<TokenBucket>,
}

impl Wrr {
    pub fn new(
        config: LocalConfig,
        net_interface: NetworkInterfaceHandle,
        packet_receiver: mpsc::Receiver<SchedulerPacket>,
        message_receiver: mpsc::UnboundedReceiver<SchedulerMessage>,
    ) -> Self {
        let capacity_unit = CapacityUnit::Packets;
        let capacity = config.queue_capacity;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match config.scheduler_drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        // Create a closure that maps a flow_id to a class_id
        let flow_classes = {
            let map: Arc<Mutex<HashMap<usize, usize>>> = Arc::new(Mutex::new(HashMap::new()));
            let next_id: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));

            Arc::new(move |flow_id: usize| -> usize {
                let mut map_guard = map.lock().unwrap();

                // If we've seen this flow_id before, return its assigned class_id
                if let Some(&class_id) = map_guard.get(&flow_id) {
                    return class_id;
                }

                // If it's a new flow_id, assign it the next consecutive class_id
                let mut next_id_guard = next_id.lock().unwrap();
                *next_id_guard += 1;

                map_guard.insert(flow_id, *next_id_guard);

                *next_id_guard
            })
        };

        let mut queues = Vec::new();
        let mut weights = Vec::new();

        Self {
            net_interface,
            packet_receiver,
            message_receiver,
            flow_classes: flow_classes.clone(),
            drop_strategy: packet_drop,
            weights,
            packets_dropped: 0,
            packets_waiting: 0,
            total_bytes: 0,
            capacity,
            queues,
            token_bucket: None,
        }
    }

    pub async fn run(&mut self) {}

    async fn enqueue(&mut self, packet: Packet) {
        let total_queue_length: usize = self.queues.iter().map(|q| q.len()).sum();

        // drops the packet based on the drop strategy
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.packet_size,
            self.total_bytes,
            total_queue_length,
        );

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;

            warn!(
                "WRR: Scheduler dropped a packet for flow {} (size: {}). Total queue length: {}/{}, packets dropped: {}",
                packet.flow_id,
                packet.packet_size,
                total_queue_length,
                self.capacity,
                self.packets_dropped
            );

            return;
        }

        // The case that this packet will not be dropped
        let class_id = (self.flow_classes)(packet.flow_id as usize);
        let packet_size = packet.packet_size;
        let is_tcp_data = packet.is_tcp_data();

        if self.queues[class_id].push(packet).is_err() {
            self.packets_dropped += 1;

            warn!("FIFO: Scheduler dropped a packet as the queue is full.");
        } else {
            // notifies the writer task if it is not a TCP packet, or if it is SYN, FIN, RST, or ACK
            // if it is a TCP packet, it is stored in the queue for a while before being consumed by the writer task
            if is_tcp_data {
                if self.queues[class_id].len() > 2 {
                    // if the queue length is over a threshold, it notifies the consumer task that a packet has arrived
                    // and the queue becomes 'non-empty' now
                    self.schedule_packets().await;
                    self.total_bytes += packet_size;
                    self.packets_waiting += 1;
                }
            } else {
                self.schedule_packets().await;
                self.total_bytes += packet_size;
                self.packets_waiting += 1;
            }
        }
    }

    async fn schedule_packets(&mut self) {
        let mut current_queue = 0;
        loop {
            if self.packets_waiting == 0 {
                return;
            }

            let mut batch = Vec::new();

            // Get packets from the current queue according to the weight
            loop {
                if self.queues[current_queue].is_empty()
                    || batch.len() >= self.weights[current_queue]
                {
                    break;
                }

                batch.push(self.queues[current_queue].pop().unwrap());
            }

            // Send the batch and move to next queue
            self.packets_waiting -= batch.len();
            self.send_packets(&mut batch).await;
            current_queue = (current_queue + 1) % self.queues.len();
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
                    "WrrScheduler: Error sending batch of {} packets: {}",
                    packet_count, e
                );

                return;
            }
        }
    }
}
