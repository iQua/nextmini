/// Defines database models.
use sqlx::FromRow;

#[derive(FromRow, Debug)]
pub struct Node {
    pub id: i32,
    pub private_network_name: Option<String>,
    pub private_network_addr: String,
    pub public_network_addr: String,
    // tun interface information
    pub virtual_network_addr: String, // tun virtual ip (10.0.0.1)
    // smoltcp interface information
    pub smoltcp_virtual_addr: String, // smoltcp virtual ip (192.168.0.1)
}

#[derive(Clone, FromRow, Debug)]
pub struct Route {
    pub src_node_id: i32,
    pub dst_node_id: i32,
    pub route_id: i32,
    pub route: Vec<i32>,
}
