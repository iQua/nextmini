use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Notify, RwLock};

use crossbeam_queue::ArrayQueue;

use tracing::{error, warn};

use crate::dataplane::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::dataplane::packet::Packet;
use crate::dataplane::protocols_io::ProtocolWriter;
use crate::dataplane::utils::RateLimiter;

/// The scheduling discipline.
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SchedulingDiscipline {
    Fifo,
    Wrr,
}

/// Defines the interface for all scheduling disciplines.
pub trait Scheduler {
    fn enqueue(&mut self, packet: Packet);
    fn run(&mut self);
}

/// FIFO is a scheduling discipline that schedules packets in a first-in-first-out manner.
pub struct Fifo {
    queue: Arc<ArrayQueue<Packet>>,
    packets_dropped: usize,
    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    packet_arrived: Arc<Notify>,
    writer: ProtocolWriter,
    rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    shutdown: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Fifo {
    const BATCH_SIZE: usize = 32;

    pub fn new(
        capacity: usize,
        drop_strategy: DropStrategy,
        writer: ProtocolWriter,
        rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    ) -> Fifo {
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        Fifo {
            queue: Arc::new(ArrayQueue::new(capacity)),
            packets_dropped: 0,
            drop_strategy: packet_drop,
            packet_arrived: Arc::new(Notify::new()),
            writer,
            rate_limiter,
            shutdown: Arc::new(AtomicBool::new(false)),
            task_handle: None,
        }
    }
}

impl Scheduler for Fifo {
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

    fn run(&mut self) {
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
        let mut writer = self.writer.reproduce();
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
                    writer.send(&packet.buf[0..packet.packet_size]).await;
                    tokens += packet.packet_size;
                    counter += 1;

                    // Apply rate limiting at batch boundaries or when queue is empty
                    if counter >= Self::BATCH_SIZE || queue.is_empty() {
                        if let Some(limiter) = rate_limiter.read().await.as_ref() {
                            if tokens > 0 {
                                limiter.consume((tokens as f64) * 8.0).await;
                            }
                        }

                        // Reset counters for next batch
                        counter = 0;
                        tokens = 0;
                    }
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
