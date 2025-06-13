use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::mpsc;

use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use serde::Deserialize;
use tracing::{debug, error, warn};

use crate::node::config::LocalConfig;
use crate::node::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;

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
pub enum SchedulerMessage {
    InboundPacket(Packet),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::Sender<SchedulerMessage>,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        // creates the mpsc channel for sending packets to the scheduler
        let (sender, receiver) = mpsc::channel(config.channel_capacity);

        let scheduler = match config.scheduler_type {
            SchedulingDiscipline::Fifo => Fifo::new(config, net_interface, receiver),
            _ => {
                panic!("This scheduling discipline has not yet been implemented.");
            }
        };

        scheduler.run();

        Self { sender }
    }

    // Sends a packet to the scheduler.
    pub fn send(&self, packet: Packet) {
        if let Err(e) = self
            .sender
            .try_send(SchedulerMessage::InboundPacket(packet))
        {
            error!(
                "SchedulerHandle: Error sending a packet to the scheduler: {}.",
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
        receiver: mpsc::Receiver<SchedulerMessage>,
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
            receiver,
            queue_not_empty: queue_not_empty.clone(),
            capacity,
            batch_size: config.batch_size,
        };

        let mut writer = FifoWriter {
            queue: scheduler_queue,
            net_interface,
            queue_not_empty,
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
    /// the receiver for an mpsc channel, for other actors to send packets to this reader
    pub receiver: mpsc::Receiver<SchedulerMessage>,
    /// signals when the queue has packets to be consumed
    pub queue_not_empty: Arc<Notify>,
    /// maximum queue capacity
    pub capacity: usize,
    /// batch size for processing packets
    pub batch_size: usize,
}

impl FifoReader {
    async fn run(&mut self) {
        // producer task: receives packets and enqueues them
        loop {
            // processes any immediately available packets first (non-blocking)
            let processed_packets = self.process_available_packets().await;

            // if we have already processed a full batch, yield control briefly
            if processed_packets >= self.batch_size {
                tokio::task::yield_now().await;
                continue;
            }

            // waits for a new inbound packet
            if let Some(message) = self.receiver.recv().await {
                match message {
                    SchedulerMessage::InboundPacket(packet) => {
                        self.enqueue(packet);
                    }
                }
            }
        }
    }

    // Helper method to process all immediately available packets
    async fn process_available_packets(&mut self) -> usize {
        let mut processed = 0;

        while processed < self.batch_size {
            match self.receiver.try_recv() {
                Ok(SchedulerMessage::InboundPacket(packet)) => {
                    self.enqueue(packet);
                    processed += 1;
                }
                Err(_) => break, // No more packets available
            }
        }

        processed
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
                if self.queue.len() > 50 {
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
}

// Consumer task: pops packets from the queue and sends them out
impl FifoWriter {
    async fn run(&mut self) {
        let mut batch = Vec::new();

        loop {
            // Wait for notification if queue is empty
            if self.queue.is_empty() {
                self.queue_not_empty.notified().await;
            }

            // Drain packets from queue efficiently
            while let Some(packet) = self.queue.pop() {
                batch.push(packet);
            }

            // Send batch if we have packets
            if !batch.is_empty() {
                self.send_packets(&mut batch).await;
            }
        }
    }

    async fn send_packets(&mut self, batch: &mut Vec<Packet>) {
        let packets = std::mem::take(batch);

        if let Err(e) = self.net_interface.send_packets(packets).await {
            error!(
                "FifoWriter: Error sending batch of {} packets: {}",
                batch.capacity(),
                e
            );
        }
    }
}
