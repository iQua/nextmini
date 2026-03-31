#[derive(Debug, Clone)]
pub enum DbEvent {
    RoutesChanged,
    FlowInserted {
        flow_id: i32,
    },
    GroupRoutesSync {
        group_id: i32,
        prior_member_node_id: Option<u32>,
    },
    ProbeRequested {
        id: i32,
        from_node_id: i32,
        to_node_id: i32,
        probe_bytes: i32,
    },
}
