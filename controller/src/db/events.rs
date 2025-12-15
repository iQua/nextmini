#[derive(Debug, Clone)]
pub enum DbEvent {
    RoutesChanged,
    FlowInserted { flow_id: i32 },
    GroupRoutesSync { group_id: i32 },
}
