use ahash::AHashMap;
use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::error;

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::sched::SchedulerWriterMessage;
use crate::node::scheduler::token_bucket::TokenBucket;

#[derive(Default)]
struct FecCancelState {
    cancel_before_by_session: AHashMap<u64, u64>,
}

impl FecCancelState {
    fn update(&mut self, session_id: u64, cancel_before_block_id: u64) {
        let entry = self
            .cancel_before_by_session
            .entry(session_id)
            .or_insert(cancel_before_block_id);
        if cancel_before_block_id > *entry {
            *entry = cancel_before_block_id;
        }
    }

    fn retains(&self, packet: &Packet) -> bool {
        let Some((session_id, block_id)) = packet.lossless_fec_session_and_block() else {
            return true;
        };
        self.cancel_before_by_session
            .get(&session_id)
            .is_none_or(|cancel_before_block_id| block_id >= *cancel_before_block_id)
    }

    fn filter_batch(&self, batch: &mut Vec<Packet>) {
        batch.retain(|packet| self.retains(packet));
    }
}

/// The consumer in the scheduler.
pub struct SchedulerWriter {
    queue: Arc<dyn SchedulerQueue + Send + Sync>,
    queues_not_empty: Arc<Notify>,
    net_interface: NetworkInterfaceHandle,
    receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    token_bucket: Option<TokenBucket>,
    capacity_semaphore: Option<Arc<Semaphore>>,
    fec_cancel_state: FecCancelState,
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
            fec_cancel_state: FecCancelState::default(),
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
                    SchedulerWriterMessage::SetFecCancelBefore {
                        session_id,
                        cancel_before_block_id,
                    } => {
                        self.fec_cancel_state
                            .update(session_id, cancel_before_block_id);
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
            self.fec_cancel_state.filter_batch(&mut batch);
            if !batch.is_empty() {
                self.send_packets(&mut batch).await;
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use nextmini_messages::lossless_session;

    use super::*;

    #[test]
    fn fec_cancel_state_tracks_monotonic_watermarks() {
        let mut state = FecCancelState::default();

        state.update(7, 5);
        state.update(7, 3);
        state.update(7, 9);

        assert_eq!(state.cancel_before_by_session.get(&7).copied(), Some(9));
    }

    #[test]
    fn fec_cancel_state_filters_only_stale_matching_session_packets() {
        let mut state = FecCancelState::default();
        state.update(42, 4);

        let mut batch = vec![
            Packet::build_ipv4_tcp_packet(
                Ipv4Addr::new(10, 0, 0, 1),
                4000,
                Ipv4Addr::new(10, 0, 0, 2),
                5000,
                &lossless_session::encode_fec_data(42, 3, 1, 0, b"old"),
            ),
            Packet::build_ipv4_tcp_packet(
                Ipv4Addr::new(10, 0, 0, 1),
                4000,
                Ipv4Addr::new(10, 0, 0, 2),
                5000,
                &lossless_session::encode_fec_data(42, 4, 1, 0, b"keep"),
            ),
            Packet::build_ipv4_tcp_packet(
                Ipv4Addr::new(10, 0, 0, 1),
                4000,
                Ipv4Addr::new(10, 0, 0, 2),
                5000,
                &lossless_session::encode_fec_data(99, 1, 1, 0, b"other-session"),
            ),
            Packet::build_ipv4_tcp_packet(
                Ipv4Addr::new(10, 0, 0, 1),
                4000,
                Ipv4Addr::new(10, 0, 0, 2),
                5000,
                &lossless_session::encode_data(42, 1, b"not-fec"),
            ),
        ];

        state.filter_batch(&mut batch);

        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].lossless_fec_session_and_block(), Some((42, 4)));
        assert_eq!(batch[1].lossless_fec_session_and_block(), Some((99, 1)));
        assert_eq!(batch[2].lossless_fec_session_and_block(), None);
    }
}
