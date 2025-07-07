use std::fmt::{Display, Formatter};
use std::sync::Arc;

use ahash::AHashMap;
use flume;
use tokio;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info, warn};

use nextmini_messages::{OperatingMode, RoutingTableEntry, TokenBucketSpec};

use crate::node::config::{Feature, LocalConfig};
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorPacket;
use crate::node::route::RoutingTable;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub enum ConnectorMessage {
    ConnectTcpMaxClient(TcpMaxClient),
    AddNodeAddress(NodeId, String),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
}

pub struct Connector {
    packet_receiver: mpsc::Receiver<ProcessorPacket>,
    message_receiver: mpsc::Receiver<ConnectorMessage>,
    config: LocalConfig,
    tcp_max_client: Option<TcpMaxClient>,
    routing_table: RoutingTable,
    node_addresses: AHashMap<NodeId, String>,
    schedulers: AHashMap<FlowId, SchedulerHandle>,
}

impl Connector {
    pub fn new(
        packet_receiver: mpsc::Receiver<ProcessorPacket>,
        message_receiver: mpsc::Receiver<ConnectorMessage>,
        config: LocalConfig,
    ) -> Self {
        Self {
            packet_receiver,
            message_receiver,
            config,
            tcp_max_client: None,
            routing_table: RoutingTable::new(config.node_id),
            node_addresses: AHashMap::new(),
            schedulers: AHashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.message_receiver.recv() => {
                    self.handle_message(msg).await;
                }
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(packet) => {
                            self.process_packet(packet).await;
                        }
                        ProcessorPacket::InboundMaxRequest(flow_id, stream) => {
                            self.handle_inbound_request(flow_id, stream).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ConnectorMessage) {
        match msg {
            ConnectorMessage::ConnectTcpMaxClient(tcp_max_client) => {
                self.tcp_max_client = Some(tcp_max_client);
            }
            ConnectorMessage::AddNodeAddress(node_id, address) => {
                self.node_addresses.insert(node_id, address);
            }
            ConnectorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
        }
    }

    async fn process_packet(&mut self, packet: Packet) {
        let flow_id = packet.flow_id;

        // Send the packet directly if tcp max connection is established
        if let Some(scheduler) = self.schedulers.get(&flow_id) {
            scheduler.send(packet);
            return;
        }

        // We are on the src node and the tcp connection is not spliced yet
        let next_hop_id = self.routing_table.select_route_for_flow(flow_id).unwrap();
        let remote_addr = self.node_addresses[&next_hop_id].clone();

        let scheduler = self
            .tcp_max_client
            .as_ref()
            .unwrap()
            .connect_as_src_node(packet.flow_id, &remote_addr, next_hop_id)
            .await;

        // sends the packet
        scheduler.send(packet);

        // inserts the scheduler into the hashmap
        self.schedulers.insert(flow_id, scheduler);
    }

    async fn handle_inbound_request(&mut self, flow_id: FlowId, mut inbound_stream: TcpStream) {
        let route_id = self.routing_table.select_route_for_flow(flow_id).unwrap();
        let next_hop_id = self.routing_table.get_next_hop_by_route(route_id).unwrap();

        // handles the case where we are at the dst node.
        if next_hop_id == self.routing_table.local_id {
            let scheduler = self
                .tcp_max_client
                .as_ref()
                .unwrap()
                .connect_as_dst_node(inbound_stream, next_hop_id)
                .await;

            // inserts reversed flow id.
            self.schedulers
                .insert(SchedulerKey::Flow(flow_id.reverse()), scheduler);

            return;
        }

        // handles the case where we are at a relay node.
        let next_hop_addr = self.node_addresses.get(&next_hop_id).cloned().unwrap();

        let mut outbound_stream = self
            .tcp_max_client
            .as_ref()
            .unwrap()
            .connect_as_relay(flow_id, &next_hop_addr)
            .await;

        // TODO: We might don't need to spawn the task;
        // spawns a new task to handle the connection splicing.
        tokio::spawn(async move {
            match zero_copy_bidirectional(&mut inbound_stream, &mut outbound_stream).await {
                Ok((upstream_bytes, downstream_bytes)) => {
                    info!(
                        "Spliced connection for flow {} to {} (upstream: {} bytes, downstream: {} bytes).",
                        flow_id, next_hop_addr, upstream_bytes, downstream_bytes
                    );
                }
                Err(e) => {
                    error!("Error during splicing for flow {}: {}.", flow_id, e);
                }
            }
        });
    }
}
