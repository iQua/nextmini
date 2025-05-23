/// Defines database models.
use sqlx::FromRow;

#[derive(FromRow, Debug)]
pub struct Node {
    pub id: i32,
    pub private_network_name: Option<String>,
    pub private_network_addr: String,
    pub public_network_addr: String,
    pub virtual_network_addr: String,
    pub connections: Vec<i32>,
}

#[derive(Clone, FromRow, Debug)]
pub struct Route {
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub route_id: i32,
    pub route: Vec<i32>,
}

#[allow(dead_code)]
#[derive(FromRow, Debug)]
pub struct Metrics {
    pub id: i32,
    pub src_id: Option<i32>,
    pub dst_id: Option<i32>,
    pub route_id: Option<i32>,
    pub prev_hop_id: Option<i32>,
    pub hop_id: Option<i32>,
    pub flow_id: Vec<u8>,
    // pub stream_id: Option<String>,
    pub time_read: chrono::DateTime<chrono::Utc>,
    pub bps: i32,
}
