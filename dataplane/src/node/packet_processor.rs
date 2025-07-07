use nextmini_messages::{RoutingTableEntry, TokenBucketSpec};
use tokio::net::TcpStream;

use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::splice::tcp_max::TcpMaxClient;
use crate::node::{FlowId, NodeId};
use crate::node::processor::ProcessorHandle;
use crate::node::splice::connector::ConnectorHandle;

/// the packet processing logic for both "normal" and "max" operating modes.

#[derive(Clone)]
pub enum PacketProcessorHandle {
    Normal(ProcessorHandle),
    Max(ConnectorHandle),
}

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

impl PacketProcessor for PacketProcessorHandle {
    fn process_packet(&self, packet: Packet) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.process_packet(packet),
            PacketProcessorHandle::Max(handle) => handle.process_packet(packet),
        }
    }

    fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.update_routing_table(routes),
            PacketProcessorHandle::Max(handle) => handle.update_routing_table(routes),
        }
    }

    fn add_node(&self, node_id: NodeId, scheduler: SchedulerHandle) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.add_node(node_id, scheduler),
            PacketProcessorHandle::Max(handle) => handle.add_node(node_id, scheduler),
        }
    }

    fn add_node_address(&self, node_id: NodeId, remote_addr: String) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.add_node_address(node_id, remote_addr),
            PacketProcessorHandle::Max(handle) => handle.add_node_address(node_id, remote_addr),
        }
    }

    fn connect_tcp_max_client(&self, tcp_max_client: TcpMaxClient) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.connect_tcp_max_client(tcp_max_client),
            PacketProcessorHandle::Max(handle) => handle.connect_tcp_max_client(tcp_max_client),
        }
    }

    fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.connect_local_interface(local_interface),
            PacketProcessorHandle::Max(handle) => handle.connect_local_interface(local_interface),
        }
    }

    fn connect_user_space_sender(&self, flow_id: FlowId, sender: UserSpaceSender) {
        match self {
            PacketProcessorHandle::Normal(handle) => {
                handle.connect_user_space_sender(flow_id, sender)
            }
            PacketProcessorHandle::Max(handle) => handle.connect_user_space_sender(flow_id, sender),
        }
    }

    fn disconnect_user_space_sender(&self, flow_id: FlowId) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.disconnect_user_space_sender(flow_id),
            PacketProcessorHandle::Max(handle) => handle.disconnect_user_space_sender(flow_id),
        }
    }

    fn connect_server(&self, server: UserSpaceServerHandle) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.connect_server(server),
            PacketProcessorHandle::Max(handle) => handle.connect_server(server),
        }
    }

    fn limit_rate(&self, node_id: NodeId, spec: TokenBucketSpec) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.limit_rate(node_id, spec),
            PacketProcessorHandle::Max(handle) => handle.limit_rate(node_id, spec),
        }
    }

    fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.set_flow_weight(flow_id, weight),
            PacketProcessorHandle::Max(handle) => handle.set_flow_weight(flow_id, weight),
        }
    }

    fn splice_connection(&self, flow_id: FlowId, stream: TcpStream) {
        match self {
            PacketProcessorHandle::Normal(handle) => handle.splice_connection(flow_id, stream),
            PacketProcessorHandle::Max(handle) => handle.splice_connection(flow_id, stream),
        }
    }
}
