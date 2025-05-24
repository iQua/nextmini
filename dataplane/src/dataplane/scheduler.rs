use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tokio::sync::{Notify, RwLock};

use crossbeam_queue::ArrayQueue;

use tracing::{error, debug};


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
        }
    }
}

impl Scheduler for Fifo {
    fn enqueue(&mut self, packet: Packet) {
        let enqueue_start = if cfg!(debug_assertions) { Some(Instant::now()) } else { None };
        
        // drops the packet if the buffer is full
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.packet_size, self.queue.len(), self.queue.len());

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            if cfg!(debug_assertions) {
                debug!("[PERF] FIFO scheduler dropped packet flow_id {} (queue len: {})", 
                       packet.flow_id, self.queue.len());
            }
            return;
        }

        if self.queue.push(packet).is_err() {
            error!("Fifo: CRITICAL - Failed to enqueue packet, queue may be smaller than drop strategy accounts for or concurrent issue.");
            return;
        }

        if cfg!(debug_assertions) {
            if let Some(start) = enqueue_start {
                let enqueue_duration = start.elapsed();
                if enqueue_duration.as_micros() > 20 {
                    debug!("[PERF] FIFO enqueue took {}μs (queue len: {})", 
                           enqueue_duration.as_micros(), self.queue.len());
                }
            }
        }

        self.packet_arrived.notify_one();
    }

    fn run(&mut self) {
        let queue = self.queue.clone();
        let packet_arrived = self.packet_arrived.clone();
        let rate_limiter = self.rate_limiter.clone();
        let mut writer = self.writer.reproduce();
        let shutdown_flag = self.shutdown.clone();

        tokio::spawn(async move {
            if cfg!(debug_assertions) {
                debug!("[PERF] FIFO scheduler worker started");
            }
            
            let mut tokens: usize = 0; // accumulated total bytes sent
            let mut counter: usize = 0; // accumulated number of packets sent
            loop {
                let wait_start = if cfg!(debug_assertions) { Some(Instant::now()) } else { None };
                packet_arrived.notified().await;

                if cfg!(debug_assertions) {
                    if let Some(start) = wait_start {
                        let wait_duration = start.elapsed();
                        if wait_duration.as_millis() > 10 {
                            debug!("[PERF] FIFO scheduler waited {}ms for packet notification", 
                                   wait_duration.as_millis());
                        }
                    }
                }

                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                let mut batch_packets = 0;
                let batch_start = if cfg!(debug_assertions) { Some(Instant::now()) } else { None };

                while let Some(packet) = queue.pop() {
                    let send_start = if cfg!(debug_assertions) { Some(Instant::now()) } else { None };
                    
                    // Send raw packet data directly without protocol header
                    writer.send(&packet.buf[0..packet.packet_size]).await;
                    tokens += packet.packet_size;
                    batch_packets += 1;

                    if cfg!(debug_assertions) {
                        if let Some(start) = send_start {
                            let send_duration = start.elapsed();
                            if send_duration.as_micros() > 100 {
                                debug!("[PERF] FIFO writer.send() took {}μs for {} bytes, flow_id {}", 
                                       send_duration.as_micros(), packet.packet_size, packet.flow_id);
                            }
                        }
                    }

                    if counter == Self::BATCH_SIZE {
                        let rate_limit_start = if cfg!(debug_assertions) { Some(Instant::now()) } else { None };
                        
                        if let Some(limiter) = rate_limiter.read().await.as_ref() {
                            limiter.consume((tokens as f64) * 8.0).await;
                        }

                        if cfg!(debug_assertions) {
                            if let Some(start) = rate_limit_start {
                                let rate_limit_duration = start.elapsed();
                                if rate_limit_duration.as_micros() > 50 {
                                    debug!("[PERF] Rate limiting took {}μs for {} bytes", 
                                           rate_limit_duration.as_micros(), tokens);
                                }
                            }
                        }

                        counter = 0;
                        tokens = 0;
                    }

                    counter += 1;
                }

                if cfg!(debug_assertions) {
                    if batch_packets > 0 {
                        if let Some(start) = batch_start {
                            let batch_duration = start.elapsed();
                            if batch_duration.as_micros() > 200 {
                                debug!("[PERF] FIFO batch processing {} packets took {}μs", 
                                       batch_packets, batch_duration.as_micros());
                            }
                        }
                    }
                }
            }
        });
    }
}

impl Drop for Fifo {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.packet_arrived.notify_one();
    }
}
