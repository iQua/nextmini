use crossbeam_queue::ArrayQueue;

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
        // Do nothing for FIFO queue
    }
}
