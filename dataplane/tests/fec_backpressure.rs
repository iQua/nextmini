use nextmini::node::FlowId;
use nextmini::node::packet::Packet;
use nextmini::node::scheduler::queue::SchedulerQueue;
use nextmini::node::scheduler::wrr::WrrQueue;

fn make_packet(flow_id: FlowId, size: usize) -> Packet {
    let mut packet = Packet::from_vec(vec![0; size.max(1)]);
    packet.flow_id = flow_id;
    packet.packet_size = size;
    packet
}

#[test]
fn non_fec_flow_not_starved() {
    let queue = WrrQueue::new(128);

    let fec_flow: FlowId = 10;
    let non_fec_flow: FlowId = 20;

    // Simulate a heavy FEC stream and a lighter non-FEC stream sharing the same scheduler.
    queue.set_flow_weight(fec_flow, 8);
    queue.set_flow_weight(non_fec_flow, 2);

    for _ in 0..64 {
        queue
            .enqueue(make_packet(fec_flow, 1200))
            .expect("fec packet should enqueue");
    }
    queue
        .enqueue(make_packet(non_fec_flow, 256))
        .expect("non-fec packet should enqueue");

    let mut batch = Vec::new();
    queue.collect_packets(&mut batch);

    assert!(
        !batch.is_empty(),
        "scheduler must emit packets when queues are non-empty"
    );
    assert!(
        batch.iter().any(|packet| packet.flow_id == non_fec_flow),
        "non-FEC traffic must be serviced even under FEC load"
    );
    assert!(
        batch.iter().any(|packet| packet.flow_id == fec_flow),
        "FEC traffic should continue flowing while preserving non-FEC service"
    );
}
