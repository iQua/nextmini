use ahash::AHashMap;
use tokio;
#[cfg(not(target_os = "linux"))]
use tokio::io::copy_bidirectional;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
#[cfg(target_os = "linux")]
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info};

use nextmini_messages::{GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, RoutingTableEntry};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorPacket;
use crate::node::route::RoutingTable;
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub enum ConnectorMessage {
    AddNodeAddress(NodeId, String),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    UpdateGroupDirectory(Vec<GroupDirectoryEntry>),
    UpdateGroupRoutes {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    ConnectTcpMaxClient(Box<TcpMaxClient>),
    InboundMaxRequest(FlowId, TcpStream),
    SetFlowStatsReporter(Box<FlowStatsReporterHandle>),
}

pub struct Connector {
    /// receives packets from the processor handle at the src node in max mode
    packet_receiver: mpsc::Receiver<ProcessorPacket>,

    /// receives messages from the processor handle
    message_receiver: mpsc::Receiver<ConnectorMessage>,

    /// the TCP max client
    tcp_max_client: Option<TcpMaxClient>,

    /// the routing table
    routing_table: RoutingTable,

    /// the remote node addresses
    node_addresses: AHashMap<NodeId, String>,

    /// a hashmap for schedulers per flow per next hop in max mode
    schedulers: AHashMap<FlowId, AHashMap<NodeId, SchedulerHandle>>,

    /// optional flow stats reporter for route reporting
    flowstats_reporter: Option<FlowStatsReporterHandle>,
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
            tcp_max_client: None,
            routing_table: RoutingTable::new(config),
            node_addresses: AHashMap::new(),
            schedulers: AHashMap::new(),
            flowstats_reporter: None,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for a first packet or a message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        // starts a batch with the first packet
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            self.process_packet(first_packet).await;

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet).await;
                            }
                        }
                    }
                }
                Some(msg) = self.message_receiver.recv() => {
                    self.handle_message(msg).await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ConnectorMessage) {
        match msg {
            ConnectorMessage::ConnectTcpMaxClient(tcp_max_client) => {
                self.tcp_max_client = Some(*tcp_max_client);
            }
            ConnectorMessage::AddNodeAddress(node_id, address) => {
                self.node_addresses.insert(node_id, address);
            }
            ConnectorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ConnectorMessage::UpdateGroupDirectory(groups) => {
                self.routing_table.install_group_directory(groups);
            }
            ConnectorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                self.routing_table
                    .install_group_routes(group_id, src_node_id, routes);
            }
            ConnectorMessage::InboundMaxRequest(flow_id, stream) => {
                self.handle_inbound_request(flow_id, stream).await;
            }
            ConnectorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
        }
    }

    /// Processes packets at the source node.
    async fn process_packet(&mut self, packet: Packet) {
        let flow_id = packet.flow_id;

        let next_hops = match self
            .routing_table
            .get_next_hops_by_flow(flow_id, self.flowstats_reporter.as_ref())
        {
            Ok(hops) => hops,
            Err(e) => {
                error!("Error getting the next hops: {}", e);
                return;
            }
        };

        if next_hops.is_empty() {
            error!("No next hops available for flow {}.", flow_id);
            return;
        }

        let last_index = next_hops.len() - 1;
        let mut primary_packet = Some(packet);

        for (idx, next_hop_id) in next_hops.into_iter().enumerate() {
            if next_hop_id == self.routing_table.local_id {
                // Connector handles remote forwarding only; local delivery stays with the processor.
                continue;
            }

            let outbound_packet = if idx == last_index {
                primary_packet
                    .take()
                    .expect("packet already dispatched to last hop")
            } else {
                primary_packet
                    .as_ref()
                    .expect("packet missing during multicast fan-out")
                    .clone()
            };

            if let Some(existing) = self
                .schedulers
                .get(&flow_id)
                .and_then(|map| map.get(&next_hop_id))
            {
                existing.send(outbound_packet);
                continue;
            }

            let remote_addr = match self.node_addresses.get(&next_hop_id) {
                Some(addr) => addr.clone(),
                None => {
                    error!("No remote address found for node id: {}", next_hop_id);
                    continue;
                }
            };

            let tcp_max_client = match self.tcp_max_client.as_ref() {
                Some(client) => client,
                None => {
                    error!(
                        "TCP Max client not yet connected; dropping packet for {}.",
                        flow_id
                    );
                    continue;
                }
            };

            let stream = tcp_max_client.connect(flow_id, &remote_addr).await;

            let scheduler = tcp_max_client
                .initialize_scheduler(stream, next_hop_id)
                .await;

            scheduler.send(outbound_packet);

            self.schedulers
                .entry(flow_id)
                .or_insert_with(AHashMap::default)
                .insert(next_hop_id, scheduler);
        }
    }

    /// Handles an inbound request as the destination node or as a relay node.
    async fn handle_inbound_request(&mut self, flow_id: FlowId, mut inbound_stream: TcpStream) {
        let next_hop_id = match self
            .routing_table
            .get_next_hop_by_flow(flow_id, self.flowstats_reporter.as_ref())
        {
            Ok(next_hop_id) => next_hop_id,
            Err(e) => {
                error!("Error getting the next hop: {}", e);
                return;
            }
        };

        let tcp_max_client = self.tcp_max_client.as_ref().unwrap();

        // handles flows where this node is the final destination
        if next_hop_id == self.routing_table.local_id {
            // creates a new scheduler for response packets
            let scheduler = tcp_max_client
                .initialize_scheduler(inbound_stream, next_hop_id)
                .await;

            // reverses the flow ID and stores it in a hashmap from flow IDs to schedulers. This is for sending
            // response packets from the destination node to the source node
            let (src_node_id, _) = self.routing_table.extract_node_ids_from_flow(flow_id);
            self.schedulers
                .entry(flow_id.reverse())
                .or_insert_with(AHashMap::default)
                .insert(src_node_id, scheduler);
        } else {
            // handles flows that need to be forwarded to the next hop as a relay node

            // obtains the next hop address from the hashmap of node addresses (only stored for dataplane nodes)
            let next_hop_addr = match self.node_addresses.get(&next_hop_id) {
                Some(addr) => addr.clone(),
                // redirects to the external server if the next hop is not a registered node (for sending external traffic)
                None => {
                    let external_server_addr =
                        format!("{}:{}", flow_id.dst_ip(), flow_id.dst_port());

                    info!(
                        "No remote address found for node id: {}, redirecting to external server: {}",
                        next_hop_id, external_server_addr
                    );

                    external_server_addr
                }
            };

            // connect to next hop. For external servers, skip sending max header.
            let mut outbound_stream = if self.node_addresses.contains_key(&next_hop_id) {
                // obtains the [0x06] + flow_id stream to the next hop
                tcp_max_client.connect(flow_id, &next_hop_addr).await
            } else {
                // for external servers, skip sending max header and flow id
                tcp_max_client.connect_without_header(&next_hop_addr).await
            };

            // spawns a task to splice data between inbound and outbound streams (zero-copy on Linux)
            tokio::spawn(async move {
                match splice_bidirectional(&mut inbound_stream, &mut outbound_stream).await {
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
}

#[cfg(target_os = "linux")]
async fn splice_bidirectional(
    inbound_stream: &mut TcpStream,
    outbound_stream: &mut TcpStream,
) -> std::io::Result<(u64, u64)> {
    zero_copy_bidirectional(inbound_stream, outbound_stream).await
}

#[cfg(not(target_os = "linux"))]
async fn splice_bidirectional(
    inbound_stream: &mut TcpStream,
    outbound_stream: &mut TcpStream,
) -> std::io::Result<(u64, u64)> {
    copy_bidirectional(inbound_stream, outbound_stream).await
}
