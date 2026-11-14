use crate::node::FlowId;
use crate::node::packet::Packet;

pub trait SchedulerQueue: Send + Sync {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet>;
    fn collect_packets(&self, batch: &mut Vec<Packet>, max_packets: usize);
    fn is_empty(&self) -> bool;
    fn queue_len(&self, flow_id: FlowId) -> usize;
    fn set_flow_weight(&self, flow_id: FlowId, weight: usize);
}
