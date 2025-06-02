use std::collections::VecDeque;

use async_trait::async_trait;
use clap::ValueEnum;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::warn;

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
    Enqueue(Packet),
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

        let mut scheduler = match config.scheduler_type {
            SchedulingDiscipline::Fifo => Fifo::new(config, net_interface, receiver),
            SchedulingDiscipline::Wrr => {
                panic!("Wrr scheduling discipline not implemented");
            }
        };

        tokio::spawn(async move {
            scheduler.run().await;
        });

        Self { sender }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.sender
            .send(SchedulerMessage::Enqueue(packet))
            .await
            .unwrap();
    }
}

/// Defines the interface for all scheduling disciplines.
#[async_trait]
pub trait Scheduler {
    async fn run(&mut self);
    fn enqueue(&mut self, packet: Packet);
}

/// FIFO is a scheduling discipline that schedules packets in a first-in-first-out manner.
pub struct Fifo {
    queue: VecDeque<Packet>,
    /// the number of packets dropped so far
    packets_dropped: usize,
    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    net_interface: NetworkInterfaceHandle,
    /// a mpsc receiver for other actors to send packets to this scheduler
    receiver: mpsc::Receiver<SchedulerMessage>,
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

        Fifo {
            queue: VecDeque::with_capacity(capacity),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            net_interface,
            receiver,
        }
    }
}

#[async_trait]
impl Scheduler for Fifo {
    async fn run(&mut self) {
        while let Some(packet) = self.queue.pop_front() {
            tokio::select! {
                // sends a packet from the queue to the network interface
                _ = self.net_interface.send(packet) => {}
                // receives a packet from the processors
                Some(message) = self.receiver.recv() => {
                    // a packet arrives from the processors
                    match message {
                        SchedulerMessage::Enqueue(packet) => {
                            self.enqueue(packet);
                        }
                    }
                }
            }
        }
    }

    fn enqueue(&mut self, packet: Packet) {
        // drops the packet if the buffer is full
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, self.queue.len(), self.queue.len());

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            warn!(
                "FIFO: Scheduler dropped packet for flow {} (size: {}) - queue length: {}/{}, drops: {}",
                packet.flow_id,
                packet.packet_size,
                self.queue.len(),
                self.queue.capacity(),
                self.packets_dropped
            );
            return;
        }

        self.queue.push_back(packet);
    }
}
