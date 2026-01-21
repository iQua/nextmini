use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::error;

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::sched::SchedulerWriterMessage;
use crate::node::scheduler::token_bucket::TokenBucket;

/// The consumer in the scheduler.
pub struct SchedulerWriter {
    queue: Arc<dyn SchedulerQueue + Send + Sync>,
    queues_not_empty: Arc<Notify>,
    net_interface: NetworkInterfaceHandle,
    receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    token_bucket: Option<TokenBucket>,
    capacity_semaphore: Option<Arc<Semaphore>>,
}

impl SchedulerWriter {
    pub fn new(
        queue: Arc<dyn SchedulerQueue + Send + Sync>,
        net_interface: NetworkInterfaceHandle,
        queues_not_empty: Arc<Notify>,
        capacity_semaphore: Option<Arc<Semaphore>>,
        receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    ) -> Self {
        Self {
            queue,
            queues_not_empty,
            net_interface,
            receiver,
            token_bucket: None,
            capacity_semaphore,
        }
    }

    pub async fn run(&mut self) {
        loop {
            while let Ok(message) = self.receiver.try_recv() {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    SchedulerWriterMessage::SetFlowWeight(flow_id, weight) => {
                        self.queue.set_flow_weight(flow_id, weight);
                    }
                }
            }

            // waits for notification if queues are empty
            while self.queue.is_empty() {
                self.queues_not_empty.notified().await;
            }

            // Collect and send packets from scheduler queues
            let mut batch = Vec::new();
            self.queue.collect_packets(&mut batch);
            let drained = batch.len();
            if drained > 0
                && let Some(semaphore) = &self.capacity_semaphore
            {
                // Backpressure tracks queue occupancy, not network I/O completion.
                // Once packets are dequeued, free their slots immediately.
                semaphore.add_permits(drained);
            }
            self.send_packets(&mut batch).await;

            // After each round of queue processing, yield to the producer task
            tokio::task::yield_now().await;
        }
    }

    async fn send_packets(&mut self, batch: &mut Vec<Packet>) {
        let packets = std::mem::take(batch);
        let packet_count = packets.len();

        if let Some(ref mut token_bucket) = self.token_bucket {
            token_bucket.send(&mut self.net_interface, packets).await;
        } else if let Err(e) = self.net_interface.send(packets).await {
            error!(
                "SchedulerWriter: Error sending batch of {} packets: {}",
                packet_count, e
            );
        }
    }
}
