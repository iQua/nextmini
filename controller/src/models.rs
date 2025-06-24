/// Defines database models.
use sqlx::FromRow;
use std::net::Ipv4Addr;

#[derive(FromRow, Debug)]
pub struct Node {
    pub id: i32,
    pub private_network_name: Option<String>,
    pub private_network_addr: Ipv4Addr,
    pub public_network_addr: Ipv4Addr,
}

#[derive(Clone, FromRow, Debug)]
pub struct Route {
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub route_id: i32,
    pub route: Vec<i32>,
}
