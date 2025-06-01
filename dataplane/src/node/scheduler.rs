use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, RwLock, mpsc};
use tracing::{error, warn};

use crate::node::config::LocalConfig;
use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::metrics::Collector;
use crate::node::packet::Packet;
use crate::node::protocols_io::ProtocolWriter;
use crate::node::utils::RateLimiter;
use crate::node::{FlowId, NodeId};

/// The scheduling discipline.
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum SchedulingDiscipline {
    #[default]
    Fifo,
    Wrr,
}

/// The types of messages sent to the scheduler actors.
pub enum SchedulerMessage {
    Enqueue(Packet),
}

// a handle is for one scheduler actor, a scheduler actor is for one node's protocol writer
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::Sender<SchedulerMessage>,
}

impl SchedulerHandle {
    pub fn new(
        config: LocalConfig,
        protocol_writer: ProtocolWriter,
        controller_interface_handle: ControllerInterfaceHandle,
    ) -> Self {
        // Initialize the metrics collector
        let mut metrics_collector = Collector::new(controller_interface_handle);
        let metrics_tx = metrics_collector.get_metrics_tx();

        // Initialize the scheduler actor
        let (sender, receiver) = mpsc::channel(mpsc_channel_size);
        let mut scheduler = match scheduler_type {
            SchedulingDiscipline::Fifo => Fifo::new(
                receiver,
                config.queue_capacity,
                config.drop_strategy,
                protocol_writer,
                metrics_tx,
                config.local_id,
            ),
            SchedulingDiscipline::Wrr => {
                panic!("Wrr scheduling discipline not implemented");
            }
        };

        // spawn all tasks
        tokio::spawn(async move {
            metrics_collector.run().await; // Spawn the metrics collector
            scheduler.send_to_protocol_writer(); // Inside this method, the subscriber to the enqueue notification is spawned
            scheduler.run().await
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
/// A scheduler has two main tasks :
/// 1. Enqueue packets to the queue -> Send packets to the protocol writer
/// 2. Collect metrics at regular intervals -> Send metrics to the controller via collector struct
pub trait Scheduler {
    async fn run(&mut self);
    fn enqueue(&mut self, packet: Packet);
    fn send_to_protocol_writer(&mut self);
}

/// FIFO is a scheduling discipline that schedules packets in a first-in-first-out manner.
pub struct Fifo {
    // Receiver side of the mpsc channel (sent from scheduler handle)
    receiver: mpsc::Receiver<SchedulerMessage>,

    // The local node_id which is sent inside the metrics
    local_id: NodeId,

    // Other data used by scheduler
    queue: Arc<ArrayQueue<Packet>>,
    packets_dropped: usize,
    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    packet_arrived: Arc<Notify>,
    protocol_writer: ProtocolWriter, // a handle to the protocol writer
    shutdown: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Fifo {
    const BATCH_SIZE: usize = 32;

    pub fn new(
        receiver: mpsc::Receiver<SchedulerMessage>,
        capacity: usize,
        drop_strategy: DropStrategy,
        protocol_writer: ProtocolWriter,
        rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
        local_id: NodeId,
    ) -> Self {
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        Fifo {
            receiver,
            local_id,
            queue: Arc::new(ArrayQueue::new(capacity)),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            packet_arrived: Arc::new(Notify::new()),
            protocol_writer,
            shutdown: Arc::new(AtomicBool::new(false)),
            task_handle: None,
        }
    }
}

impl Scheduler for Fifo {
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                SchedulerMessage::Enqueue(packet) => {
                    // Enqueue the packet
                    self.enqueue(packet);
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

        if self.queue.push(packet).is_err() {
            self.packets_dropped += 1;
            error!(
                "FIFO: Failed to enqueue packet, queue may be smaller than drop strategy accounts for or concurrent issue. Total drops: {}",
                self.packets_dropped
            );
            return;
        }

        self.packet_arrived.notify_one();
    }

    fn send_to_protocol_writer(&mut self) {
        // Shutdown any existing task first
        if let Some(handle) = self.task_handle.take() {
            self.shutdown.store(true, Ordering::Relaxed);
            self.packet_arrived.notify_one();
            handle.abort();
        }

        // Reset shutdown flag for new task
        self.shutdown.store(false, Ordering::Relaxed);

        let queue = self.queue.clone();
        let packet_arrived = self.packet_arrived.clone();
        let rate_limiter = self.rate_limiter.clone();
        let mut writer = self.protocol_writer.clone();
        let shutdown_flag = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut tokens: usize = 0; // accumulated total bytes sent
            let mut counter: usize = 0; // accumulated number of packets sent
            loop {
                packet_arrived.notified().await;

                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                while let Some(packet) = queue.pop() {
                    // Send raw packet data directly without protocol header
                    tokens += packet.packet_size;
                    counter += 1;

                    writer.send(&packet.buf[0..packet.packet_size]).await; // Scheduler -> NetworkInterfaceHandle
                }
            }
        });

        self.task_handle = Some(handle);
    }
}

impl Drop for Fifo {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.packet_arrived.notify_one();

        // Abort the task if it exists
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
