use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, warn};

use crate::node::FlowId;
use nextmini_messages::TokenBucketSpec;

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
    Fifo,
    #[default]
    Wrr,
}

/// The types of messages sent to the scheduler.
pub enum SchedulerReaderMessage {
    InboundPacket(Packet),
}

/// The rate limit is to be sent by the processor, and in the unit of bytes per second.
pub enum SchedulerWriterMessage {
    RateLimit(TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerReaderMessage>,
    writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();

        let scheduler = Scheduler::new(config, net_interface, reader_receiver, writer_receiver);
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

    /// TO BE IMPLEMENTED : Pending changes according to flow spec
    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "SchedulerHandle: Error sending a flow weight to the scheduler: {}.",
                e
            );
        }
    }
}

pub struct Scheduler {
    config: LocalConfig,
}

impl Scheduler {
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

        let queue_strategy: Arc<dyn SchedulerQueue + Send + Sync> = match config.scheduler_type {
            SchedulingDiscipline::Fifo => Arc::new(FifoQueue::new(capacity)),
            SchedulingDiscipline::Wrr => Arc::new(WrrQueue::new(capacity)),
        };

        let queues_not_empty = Arc::new(Notify::new());

        let mut reader = SchedulerReader {
            queue_strategy: queue_strategy.clone(),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            queues_not_empty: queues_not_empty.clone(),
            capacity,
            receiver: reader_receiver,
            scheduler_type: config.scheduler_type,
        };

        let mut writer = SchedulerWriter {
            queue_strategy,
            net_interface,
            queues_not_empty,
            receiver: writer_receiver,
            token_bucket: None,
            flow_weights: HashMap::new(),
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

/// Producer side of scheduler
struct SchedulerReader {
    queue_strategy: Arc<dyn SchedulerQueue + Send + Sync>,
    packets_dropped: usize,
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    queues_not_empty: Arc<Notify>,
    capacity: usize,
    receiver: mpsc::Receiver<SchedulerReaderMessage>,
    scheduler_type: SchedulingDiscipline,
}

impl SchedulerReader {
    async fn run(&mut self) {
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
        let flow_id = packet.flow_id;
        let queue_len = self.queue_strategy.queue_len(flow_id);

        // drops the packet based on the drop strategy
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, queue_len, queue_len);

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;

            warn!(
                "{:?}: Scheduler dropped a packet for flow {} (size: {}). Queue length: {}/{}, packets dropped: {}",
                self.scheduler_type,
                packet.flow_id,
                packet.packet_size,
                queue_len,
                self.capacity,
                self.packets_dropped
            );
            return;
        }

        let is_tcp_data = packet.is_tcp_data();

        if self.queue_strategy.enqueue(packet).is_err() {
            self.packets_dropped += 1;

            warn!(
                "{:?}: Scheduler dropped a packet as the queue is full.",
                self.scheduler_type
            );
        } else {
            // notifies the writer task if it is not a TCP packet, or if it is SYN, FIN, RST, or ACK
            // if it is a TCP packet, it is stored in the queue for a while before being consumed by the writer task
            if is_tcp_data {
                if queue_len > 2 {
                    // if the queue length is over a threshold, it notifies the consumer task that a packet has arrived
                    // and the queue becomes 'non-empty' now
                    self.queues_not_empty.notify_one();
                }
            } else {
                self.queues_not_empty.notify_one();
            }
        }
    }
}

/// Consumer side of scheduler
struct SchedulerWriter {
    queue_strategy: Arc<dyn SchedulerQueue + Send + Sync>, // Shared strategy instance
    flow_weights: HashMap<FlowId, usize>,
    queues_not_empty: Arc<Notify>,
    net_interface: NetworkInterfaceHandle,
    receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    token_bucket: Option<TokenBucket>,
}

impl SchedulerWriter {
    async fn run(&mut self) {
        loop {
            while let Ok(message) = self.receiver.try_recv() {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    // TO BE IMPLEMENTED : Change to flow specs
                    SchedulerWriterMessage::SetFlowWeight(flow_id, weight) => {
                        self.flow_weights.insert(flow_id, weight);
                    }
                }
            }

            // Wait for notification if queues are empty
            if self.queue_strategy.is_empty() {
                self.queues_not_empty.notified().await;
            }

            // Collect packets from queues
            let mut batch = Vec::new();
            self.queue_strategy
                .collect_packets(&mut batch, &self.flow_weights);

            // Send packets if we have any
            if !batch.is_empty() {
                self.send_packets(&mut batch).await;
            }
        }
    }

    async fn send_packets(&mut self, batch: &mut Vec<Packet>) {
        let packets = std::mem::take(batch);
        let packet_count = packets.len();

        if let Some(ref mut token_bucket) = self.token_bucket {
            token_bucket.send(&mut self.net_interface, packets).await;
        } else {
            if let Err(e) = self.net_interface.send(packets).await {
                error!(
                    "SchedulerWriter: Error sending batch of {} packets: {}",
                    packet_count, e
                );
                return;
            }
        }
    }
}

trait SchedulerQueue: Send + Sync {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet>;
    fn collect_packets(&self, batch: &mut Vec<Packet>, flow_weights: &HashMap<FlowId, usize>);
    fn is_empty(&self) -> bool;
    fn queue_len(&self, flow_id: FlowId) -> usize;
}

/// FIFO queue strategy - no inner Arc needed since Arc<QueueStrategy> provides sharing
struct FifoQueue {
    queue: ArrayQueue<Packet>, // Direct ownership, shared via Arc<FifoStrategy>
}

impl FifoQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
        }
    }
}

impl SchedulerQueue for FifoQueue {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet> {
        self.queue.push(packet)
    }

    fn collect_packets(&self, batch: &mut Vec<Packet>, _flow_weights: &HashMap<FlowId, usize>) {
        while let Some(packet) = self.queue.pop() {
            batch.push(packet);
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn queue_len(&self, _flow_id: FlowId) -> usize {
        self.queue.len()
    }
}

/// WRR queue strategy - no inner Arc needed since Arc<QueueStrategy> provides sharing
struct WrrQueue {
    flow_queues: RwLock<HashMap<FlowId, ArrayQueue<Packet>>>, // Direct ownership, shared via Arc<WrrStrategy>
    capacity: usize,
}

impl WrrQueue {
    fn new(capacity: usize) -> Self {
        Self {
            flow_queues: RwLock::new(HashMap::new()),
            capacity,
        }
    }
}

impl SchedulerQueue for WrrQueue {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet> {
        let flow_id = packet.flow_id;
        let mut flow_queues = self.flow_queues.write().unwrap();
        let flow_queue = flow_queues
            .entry(flow_id)
            .or_insert_with(|| ArrayQueue::new(self.capacity));

        flow_queue.push(packet)
    }

    fn collect_packets(&self, batch: &mut Vec<Packet>, flow_weights: &HashMap<FlowId, usize>) {
        while !self.is_empty() {
            let flow_queues = self.flow_queues.read().unwrap();
            let flow_ids: Vec<FlowId> = flow_queues.keys().cloned().collect();
            drop(flow_queues);

            for flow_id in &flow_ids {
                let weight = flow_weights.get(flow_id).unwrap_or(&1);

                for _ in 0..*weight {
                    let flow_queues = self.flow_queues.read().unwrap();
                    if let Some(flow_queue) = flow_queues.get(flow_id) {
                        if let Some(packet) = flow_queue.pop() {
                            batch.push(packet);
                        }
                    }
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        let flow_queues = self.flow_queues.read().unwrap();
        flow_queues.values().all(|queue| queue.is_empty())
    }

    fn queue_len(&self, flow_id: FlowId) -> usize {
        let flow_queues = self.flow_queues.read().unwrap();
        flow_queues.get(&flow_id).map_or(0, |queue| queue.len())
    }
}
