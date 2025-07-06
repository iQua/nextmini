use nextmini_messages::{RoutingTableEntry, TokenBucketSpec};
use tokio::net::TcpStream;

use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::splice::tcp_max::TcpMaxClient;
use crate::node::{FlowId, NodeId};

/// the packet processing logic for both "normal" and "max" operating modes.

pub trait PacketProcessor: Send + Sync {
    fn process_packet(&self, packet: Packet);
    fn update_routing_table(&self, routes: Vec<RoutingTableEntry>);
    fn add_node(&self, node_id: NodeId, scheduler: SchedulerHandle);
    fn add_node_address(&self, node_id: NodeId, remote_addr: String);
    fn connect_tcp_max_client(&self, tcp_max_client: TcpMaxClient);
    fn connect_local_interface(&self, local_interface: LocalInterfaceHandle);
    fn connect_user_space_sender(&self, flow_id: FlowId, sender: UserSpaceSender);
    fn disconnect_user_space_sender(&self, flow_id: FlowId);
    fn connect_server(&self, server: UserSpaceServerHandle);
    fn limit_rate(&self, node_id: NodeId, spec: TokenBucketSpec);
    fn set_flow_weight(&self, flow_id: FlowId, weight: usize);
    fn splice_connection(&self, flow_id: FlowId, stream: TcpStream);
}
