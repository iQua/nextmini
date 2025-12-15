/// Defines database models.
use sqlx::FromRow;

#[derive(FromRow, Debug)]
pub struct Node {
    pub id: i32,
    pub private_network_name: Option<String>,
    pub private_network_addr: String,
    pub public_network_addr: String,
}

#[derive(Clone, Debug)]
pub struct Route {
    pub route_id: usize,
    pub src_node_id: u32,
    pub dst_node_id: u32,
    pub edges: Vec<(u32, u32)>,
}

#[derive(Clone, FromRow, Debug)]
pub struct DbRoute {
    pub route_id: i32,
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub edges: serde_json::Value,
}

#[derive(Clone, FromRow, Debug)]
pub struct DbFlow {
    pub id: i32,
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub flow_len_type: String,
    pub flow_len_bytes: Option<i64>,
    pub flow_len_duration: Option<f64>,
    pub flow_rate: Option<i32>,
    pub flow_weight: Option<i32>,
    #[allow(dead_code)]
    pub is_finished: bool,
}

#[allow(dead_code)]
#[derive(FromRow, Debug)]
pub struct Group {
    pub id: i32,
    pub label: String,
    pub src_node_id: i32,
    pub group_ip: String,
}

#[allow(dead_code)]
#[derive(FromRow, Debug)]
pub struct GroupMember {
    pub group_id: i32,
    pub node_id: i32,
}

#[allow(dead_code)]
#[derive(Clone, FromRow, Debug)]
pub struct DbGroupRoute {
    pub group_id: i32,
    pub src_node_id: i32,
    pub edges: serde_json::Value,
}
