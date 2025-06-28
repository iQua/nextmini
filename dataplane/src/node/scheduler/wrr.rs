use std::collections::HashMap;
use std::sync::RwLock;

use crossbeam_queue::ArrayQueue;

use crate::node::FlowId;
use crate::node::packet::Packet;
use crate::node::scheduler::queue::SchedulerQueue;

pub struct WrrQueue {
    flow_queues: RwLock<HashMap<FlowId, ArrayQueue<Packet>>>,
    flow_weights: RwLock<HashMap<FlowId, usize>>,
    capacity: usize,
}

impl WrrQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            flow_queues: RwLock::new(HashMap::new()),
            flow_weights: RwLock::new(HashMap::new()),
            capacity,
        }
    }
}

impl SchedulerQueue for WrrQueue {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet> {
        let flow_id = packet.flow_id;
        let mut flow_queues = self.flow_queues.write().unwrap();
        let flow_queue = flow_queues
            .entry(flow_id)
            .or_insert_with(|| ArrayQueue::new(self.capacity));

        flow_queue.push(packet)
    }

    fn collect_packets(&self, batch: &mut Vec<Packet>) {
        let flow_queues = self.flow_queues.read().unwrap();
        let flow_weights = self.flow_weights.read().unwrap();

        let mut min_rounds: Option<usize> = None;
        let mut flow_ids: Vec<FlowId> = Vec::new();

        for (flow_id, flow_queue) in flow_queues.iter() {
            if !flow_queue.is_empty() {
                // remembers flow IDs with non-empty queues
                flow_ids.push(*flow_id);

                // Calculates the minimum number of rounds allowed. For example, if flow 1 with weight 2
                // has 5 packets in its queue, flow 2 with weight 1 has 3 packets, 4 packets should be
                // scheduled for sending from flow 1, and 2 from flow 2.
                let weight = *flow_weights.get(flow_id).unwrap_or(&1);
                let rounds = flow_queue.len() / weight;

                min_rounds = match min_rounds {
                    Some(current_min_rounds) => Some(current_min_rounds.min(rounds)),
                    None => Some(rounds),
                };
            }
        }

        // all queues are currently empty (should not happen as this task only runs with non-empty queues)
        assert!(
            min_rounds.is_some(),
            "Scheduler queues are empty when the consumer task runs."
        );

        drop(flow_queues);
        drop(flow_weights);

        // The weighted round-robin scheduling discipline simply schedules W packets in each round of processing,
        // where W is the integer weight of a flow.
        for _ in 0..min_rounds.unwrap() {
            for flow_id in &flow_ids {
                let flow_weights = self.flow_weights.read().unwrap();
                let flow_queues = self.flow_queues.read().unwrap();
                let weight = flow_weights.get(flow_id).unwrap_or(&1);

                if let Some(flow_queue) = flow_queues.get(flow_id) {
                    for _ in 0..*weight {
                        if let Some(packet) = flow_queue.pop() {
                            batch.push(packet);
                        }
                    }
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        let flow_queues = self.flow_queues.read().unwrap();
        flow_queues.values().all(|queue| queue.is_empty())
    }

    fn queue_len(&self, flow_id: FlowId) -> usize {
        let flow_queues = self.flow_queues.read().unwrap();
        flow_queues.get(&flow_id).map_or(0, |queue| queue.len())
    }

    fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        let mut flow_weights = self.flow_weights.write().unwrap();
        flow_weights.insert(flow_id, weight);
    }
}
