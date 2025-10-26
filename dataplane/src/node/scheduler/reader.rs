use std::sync::Arc;

use tokio::sync::{Notify, mpsc};
use tracing::warn;

use nextmini_messages::SchedulingDiscipline;

use crate::node::packet::Packet;
use crate::node::scheduler::drop::PacketDrop;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::sched::SchedulerReaderMessage;

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
            // notifies the writer task if it is not a TCP data packet (e.g., if it is SYN, FIN, RST, or pure ACK)
            // if it is a TCP data packet, it is stored in the queue for a while before being consumed by the
            // writer task
            if !is_tcp_data {
                self.queues_not_empty.notify_one();
            } else {
                let updated_queue_len = queue_len + 1;

                // if the queue length exceeds over a threshold or when we just transitioned from an empty
                // queue, notify the consumer task that a packet has arrived and the queue becomes 'non-empty' now
                if queue_len == 0 || updated_queue_len > 2 {
                    self.queues_not_empty.notify_one();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, timeout};

    use crate::node::FlowId;

    #[derive(Default)]
    struct MockQueue {
        enqueued_flows: Mutex<Vec<FlowId>>,
        queue_len_value: AtomicUsize,
        enqueue_should_fail: AtomicBool,
    }

    impl MockQueue {
        fn new() -> Self {
            Self::default()
        }

        fn set_queue_len(&self, len: usize) {
            self.queue_len_value.store(len, Ordering::SeqCst);
        }

        fn fail_next_enqueue(&self) {
            self.enqueue_should_fail.store(true, Ordering::SeqCst);
        }

        fn enqueued_flows(&self) -> Vec<FlowId> {
            self.enqueued_flows.lock().unwrap().clone()
        }
    }

    impl SchedulerQueue for MockQueue {
        fn enqueue(&self, packet: Packet) -> Result<(), Packet> {
            if self.enqueue_should_fail.swap(false, Ordering::SeqCst) {
                return Err(packet);
            }

            self.queue_len_value.fetch_add(1, Ordering::SeqCst);
            self.enqueued_flows.lock().unwrap().push(packet.flow_id);
            Ok(())
        }

        fn collect_packets(&self, _batch: &mut Vec<Packet>) {
            self.queue_len_value.store(0, Ordering::SeqCst);
        }

        fn is_empty(&self) -> bool {
            self.queue_len_value.load(Ordering::SeqCst) == 0
        }

        fn queue_len(&self, _flow_id: FlowId) -> usize {
            self.queue_len_value.load(Ordering::SeqCst)
        }

        fn set_flow_weight(&self, _flow_id: FlowId, _weight: usize) {}
    }

    #[derive(Clone)]
    struct RecordingDrop {
        response: bool,
        calls: Arc<Mutex<Vec<(usize, usize, usize)>>>,
    }

    impl RecordingDrop {
        fn new(response: bool) -> (Self, Arc<Mutex<Vec<(usize, usize, usize)>>>) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    response,
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl PacketDrop for RecordingDrop {
        fn should_drop(
            &mut self,
            packet_size: usize,
            byte_size: usize,
            queue_length: usize,
        ) -> bool {
            self.calls
                .lock()
                .unwrap()
                .push((packet_size, byte_size, queue_length));
            self.response
        }
    }

    fn make_tcp_packet(flow_id: FlowId, flags: u8) -> Packet {
        let packet_size = 40;
        let mut buf = vec![0; packet_size];

        // IPv4 header with TCP protocol marker.
        buf[0] = 0x45;
        buf[2] = 0;
        buf[3] = packet_size as u8;
        buf[9] = 6; // TCP

        // TCP header.
        let tcp_offset = 20;
        buf[tcp_offset + 12] = 0x50;
        buf[tcp_offset + 13] = flags;

        Packet {
            flow_id,
            packet_size: packet_size as usize,
            buf,
        }
    }

    fn make_non_tcp_packet(flow_id: FlowId) -> Packet {
        let packet_size = 40;
        let mut buf = vec![0; packet_size];

        buf[0] = 0x45;
        buf[9] = 17; // UDP
        let tcp_offset = 20;
        buf[tcp_offset + 12] = 0x50;
        buf[tcp_offset + 13] = 0;

        Packet {
            flow_id,
            packet_size: packet_size as usize,
            buf,
        }
    }

    fn build_reader(
        queue: Arc<MockQueue>,
        dropper: Box<dyn PacketDrop + Send + Sync>,
        notify: Arc<Notify>,
    ) -> SchedulerReader {
        let (_tx, rx) = mpsc::channel(8);
        let queue_trait: Arc<dyn SchedulerQueue + Send + Sync> = queue.clone();
        SchedulerReader::new(
            queue_trait,
            dropper,
            notify,
            16,
            rx,
            SchedulingDiscipline::Fifo,
        )
    }

    #[tokio::test]
    async fn enqueue_drops_packet_when_drop_strategy_requests() {
        let queue = Arc::new(MockQueue::new());
        queue.set_queue_len(3);

        let notify = Arc::new(Notify::new());
        let (dropper, calls) = RecordingDrop::new(true);
        let mut reader = build_reader(queue.clone(), Box::new(dropper), notify.clone());

        reader.enqueue(make_tcp_packet(1, 0x00));

        assert_eq!(reader.packets_dropped, 1);
        assert!(queue.enqueued_flows().is_empty());

        assert!(
            timeout(Duration::from_millis(10), notify.notified())
                .await
                .is_err(),
            "Drop case should not trigger queue notification"
        );

        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], (40, 3, 3));
    }

    #[tokio::test]
    async fn enqueue_drops_when_queue_rejects_packet() {
        let queue = Arc::new(MockQueue::new());
        queue.set_queue_len(2);
        queue.fail_next_enqueue();

        let notify = Arc::new(Notify::new());
        let (dropper, _) = RecordingDrop::new(false);
        let mut reader = build_reader(queue.clone(), Box::new(dropper), notify.clone());

        reader.enqueue(make_tcp_packet(2, 0x00));

        assert_eq!(reader.packets_dropped, 1);
        assert!(queue.enqueued_flows().is_empty());
        assert!(
            timeout(Duration::from_millis(10), notify.notified())
                .await
                .is_err(),
            "Queue rejection should not notify the consumer"
        );
    }

    #[tokio::test]
    async fn enqueue_non_tcp_packet_notifies_immediately() {
        let queue = Arc::new(MockQueue::new());
        queue.set_queue_len(0);

        let notify = Arc::new(Notify::new());
        let (dropper, _) = RecordingDrop::new(false);
        let mut reader = build_reader(queue.clone(), Box::new(dropper), notify.clone());

        reader.enqueue(make_non_tcp_packet(3));

        assert_eq!(queue.enqueued_flows(), vec![3]);
        notify.notified().await;
    }

    #[tokio::test]
    async fn enqueue_tcp_data_below_threshold_does_not_notify() {
        let queue = Arc::new(MockQueue::new());
        queue.set_queue_len(2);

        let notify = Arc::new(Notify::new());
        let (dropper, _) = RecordingDrop::new(false);
        let mut reader = build_reader(queue.clone(), Box::new(dropper), notify.clone());

        reader.enqueue(make_tcp_packet(4, 0x00));

        assert_eq!(queue.enqueued_flows(), vec![4]);
        assert!(
            timeout(Duration::from_millis(10), notify.notified())
                .await
                .is_err(),
            "TCP data below threshold should not wake the consumer"
        );
    }

    #[tokio::test]
    async fn enqueue_tcp_data_above_threshold_notifies_consumer() {
        let queue = Arc::new(MockQueue::new());
        queue.set_queue_len(3);

        let notify = Arc::new(Notify::new());
        let (dropper, _) = RecordingDrop::new(false);
        let mut reader = build_reader(queue.clone(), Box::new(dropper), notify.clone());

        reader.enqueue(make_tcp_packet(5, 0x00));

        assert_eq!(queue.enqueued_flows(), vec![5]);
        notify.notified().await;
    }
}
