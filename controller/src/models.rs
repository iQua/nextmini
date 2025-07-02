/// Defines database models.
use sqlx::FromRow;

#[derive(FromRow, Debug)]
pub struct Node {
    pub id: i32,
    pub private_network_name: Option<String>,
    pub private_network_addr: String,
    pub public_network_addr: String,
}

#[derive(Clone, FromRow, Debug)]
pub struct Route {
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub route_id: i32,
    pub route: Vec<i32>,
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
