use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::error;

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::sched::SchedulerWriterMessage;
use crate::node::scheduler::token_bucket::TokenBucket;

const MAX_PAYLOAD_BATCH_PACKETS: usize = 32;

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
            let mut probe_packets = Vec::new();
            while let Ok(message) = self.receiver.try_recv() {
                match message {
                    SchedulerWriterMessage::RateLimit(spec) => {
                        self.token_bucket = Some(TokenBucket::new(spec));
                    }
                    SchedulerWriterMessage::SetFlowWeight(flow_id, weight) => {
                        self.queue.set_flow_weight(flow_id, weight);
                    }
                    SchedulerWriterMessage::LinkProbePackets(packets) => {
                        probe_packets.extend(packets);
                    }
                }
            }
            if !probe_packets.is_empty()
                && let Err(e) = self.net_interface.send(probe_packets).await
            {
                error!("SchedulerWriter: Error sending probe packets: {}", e);
            }

            // waits for notification if queues are empty,
            // but also wake on control messages (e.g. probe packets)
            while self.queue.is_empty() {
                tokio::select! {
                    _ = self.queues_not_empty.notified() => {},
                    msg = self.receiver.recv() => {
                        match msg {
                            Some(SchedulerWriterMessage::LinkProbePackets(packets)) => {
                                if let Err(e) = self.net_interface.send(packets).await {
                                    error!("SchedulerWriter: Error sending probe packets: {}", e);
                                }
                            }
                            Some(SchedulerWriterMessage::RateLimit(spec)) => {
                                self.token_bucket = Some(TokenBucket::new(spec));
                            }
                            Some(SchedulerWriterMessage::SetFlowWeight(flow_id, weight)) => {
                                self.queue.set_flow_weight(flow_id, weight);
                            }
                            None => {}
                        }
                    }
                }
            }

            // Collect and send packets from scheduler queues
            let mut batch = Vec::new();
            self.queue
                .collect_packets(&mut batch, MAX_PAYLOAD_BATCH_PACKETS);
            let drained = batch.len();
            self.send_packets(&mut batch).await;
            if drained > 0
                && let Some(semaphore) = &self.capacity_semaphore
            {
                // Egress ownership should reflect actual send progress. Only free
                // queue slots after the batch has been handed off to the network.
                semaphore.add_permits(drained);
            }

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
