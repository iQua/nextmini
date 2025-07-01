use std::sync::Arc;

use tokio::sync::{Notify, mpsc};
use tracing::warn;

use nextmini_messages::SchedulingDiscipline;

use crate::node::packet::Packet;
use crate::node::scheduler::drop::PacketDrop;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::scheduler::SchedulerReaderMessage;

/// Producer side of scheduler
pub struct SchedulerReader {
    queue: Arc<dyn SchedulerQueue + Send + Sync>,
    packets_dropped: usize,
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    queues_not_empty: Arc<Notify>,
    capacity: usize,
    receiver: mpsc::Receiver<SchedulerReaderMessage>,
    scheduler_type: SchedulingDiscipline,
}

impl SchedulerReader {
    pub fn new(
        queue: Arc<dyn SchedulerQueue + Send + Sync>,
        drop_strategy: Box<dyn PacketDrop + Send + Sync>,
        queues_not_empty: Arc<Notify>,
        capacity: usize,
        receiver: mpsc::Receiver<SchedulerReaderMessage>,
        scheduler_type: SchedulingDiscipline,
    ) -> Self {
        Self {
            queue,
            packets_dropped: 0,
            drop_strategy,
            queues_not_empty,
            capacity,
            receiver,
            scheduler_type,
        }
    }

    pub async fn run(&mut self) {
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
        let queue_len = self.queue.queue_len(flow_id);

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

        if self.queue.enqueue(packet).is_err() {
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
