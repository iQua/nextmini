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
