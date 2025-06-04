use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use serde::Deserialize;
use tracing::{debug, warn};

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
    pub async fn send(&mut self, packet: Packet) -> Result<(), SendError<SchedulerMessage>> {
        self.sender
            .send(SchedulerMessage::InboundPacket(packet))
            .await?;

        Ok(())
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

        let queue_not_empty = Arc::new(Notify::new());

        let mut reader = FifoReader {
            queue: Arc::new(ArrayQueue::new(capacity)),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            receiver,
            queue_not_empty: queue_not_empty.clone(),
            capacity,
        };

        let mut writer = FifoWriter {
            queue: Arc::new(ArrayQueue::new(capacity)),
            net_interface,
            queue_not_empty,
        };

        tokio::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::spawn(async move {
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
    /// a mpsc receiver for other actors to send packets to this scheduler
    pub receiver: mpsc::Receiver<SchedulerMessage>,
    /// Notify for signaling when queue has items (for consumer)
    pub queue_not_empty: Arc<Notify>,
    /// Maximum queue capacity
    pub capacity: usize,
}

impl FifoReader {
    async fn run(&mut self) {
        // producer task: receives packets and enqueues them
        while let Some(message) = self.receiver.recv().await {
            match message {
                SchedulerMessage::InboundPacket(packet) => {
                    self.enqueue(packet);
                    // Notify consumer that queue is not empty
                    self.queue_not_empty.notify_one();
                }
            }
        }
    }

    fn enqueue(&mut self, packet: Packet) {
        // drops the packet if the buffer is full
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, self.queue.len(), self.capacity);

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;

            warn!(
                "FIFO: Scheduler dropped packet for flow {} (size: {}) - queue length: {}/{}, drops: {}",
                packet.flow_id,
                packet.packet_size,
                self.queue.len(),
                self.capacity,
                self.packets_dropped
            );

            return;
        }

        // Try to push to queue, if full then drop
        if self.queue.push(packet).is_err() {
            self.packets_dropped += 1;
        } else {
            // Notify consumer that the queue becomes 'not empty' now
            self.queue_not_empty.notify_one();
        }
    }
}

struct FifoWriter {
    pub queue: Arc<ArrayQueue<Packet>>,
    /// the network interface handle
    pub net_interface: NetworkInterfaceHandle,
    /// Notify for signaling when queue has items (for consumer)
    pub queue_not_empty: Arc<Notify>,
}

impl FifoWriter {
    // Consumer task: pops packets from the queue and sends them out
    async fn run(&mut self) {
        loop {
            // Try to dequeue a packet
            match self.queue.pop() {
                Some(packet) => {
                    // sends the packet
                    let _ = self.net_interface.send(packet).await;
                }
                None => {
                    // the scheduler's queue is empty, waits for notification
                    self.queue_not_empty.notified().await;
                }
            }
        }
    }
}
