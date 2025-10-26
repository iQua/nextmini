use crossbeam_queue::ArrayQueue;
use tracing::warn;

use crate::node::FlowId;
use crate::node::packet::Packet;
use crate::node::scheduler::queue::SchedulerQueue;

/// FIFO queue strategy: no inner Arc<> is needed since Arc<QueueStrategy> allows sharing
pub struct FifoQueue {
    queue: ArrayQueue<Packet>,
}

impl FifoQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
        }
    }
}

impl SchedulerQueue for FifoQueue {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet> {
        self.queue.push(packet)
    }

    fn collect_packets(&self, batch: &mut Vec<Packet>) {
        while let Some(packet) = self.queue.pop() {
            batch.push(packet);
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn queue_len(&self, _flow_id: FlowId) -> usize {
        self.queue.len()
    }

    fn set_flow_weight(&self, _flow_id: FlowId, _weight: usize) {
        warn!("Trying to set flow weight for FIFO queue.");
        // Do nothing for FIFO queue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_packet(flow_id: FlowId, size: usize) -> Packet {
        Packet {
            flow_id,
            packet_size: size,
            buf: vec![0; size.max(1)],
        }
    }

    #[test]
    fn enqueue_and_collect_follows_fifo_order() {
        let queue = FifoQueue::new(4);

        let packets = [
            make_packet(1, 64),
            make_packet(2, 128),
            make_packet(3, 256),
        ];

        for packet in packets {
            queue.enqueue(packet).unwrap();
        }

        let mut batch = Vec::new();
        queue.collect_packets(&mut batch);

        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].flow_id, 1);
        assert_eq!(batch[1].flow_id, 2);
        assert_eq!(batch[2].flow_id, 3);
        assert!(queue.is_empty());
    }

    #[test]
    fn enqueue_returns_err_when_full() {
        let queue = FifoQueue::new(1);

        queue.enqueue(make_packet(1, 64)).unwrap();

        let overflow = make_packet(1, 128);
        let Err(returned) = queue.enqueue(overflow) else {
            panic!("Expected queue to reject packet when full");
        };

        assert_eq!(returned.packet_size, 128);
        assert!(!queue.is_empty());
    }

    #[test]
    fn queue_length_and_empty_state_reflect_contents() {
        let queue = FifoQueue::new(3);

        assert!(queue.is_empty());
        assert_eq!(queue.queue_len(42), 0);

        queue.enqueue(make_packet(7, 64)).unwrap();
        queue.enqueue(make_packet(8, 64)).unwrap();

        assert!(!queue.is_empty());
        assert_eq!(queue.queue_len(123), 2);

        let mut batch = Vec::new();
        queue.collect_packets(&mut batch);

        assert!(queue.is_empty());
        assert_eq!(batch.len(), 2);
    }
}
