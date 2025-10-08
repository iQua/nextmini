use ahash::AHashMap;
use tokio;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info};

use nextmini_messages::RoutingTableEntry;

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorPacket;
use crate::node::route::RoutingTable;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub enum ConnectorMessage {
    AddNodeAddress(NodeId, String),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    ConnectTcpMaxClient(TcpMaxClient),
    InboundMaxRequest(FlowId, TcpStream),
    SetFlowStatsReporter(FlowStatsReporterHandle),
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

    /// a hashmap for the schedulers in max mode
    schedulers: AHashMap<FlowId, SchedulerHandle>,

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
                self.tcp_max_client = Some(tcp_max_client);
            }
            ConnectorMessage::AddNodeAddress(node_id, address) => {
                self.node_addresses.insert(node_id, address);
            }
            ConnectorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ConnectorMessage::InboundMaxRequest(flow_id, stream) => {
                self.handle_inbound_request(flow_id, stream).await;
            }
            ConnectorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(flowstats_reporter);
            }
        }
    }

    /// Processes packets at the source node.
    async fn process_packet(&mut self, packet: Packet) {
        let flow_id = packet.flow_id;

        // sends directly when the TCP max connection is already established, or initiates a new connection
        if let Some(scheduler) = self.schedulers.get(&flow_id) {
            // if the scheduler is already initialized at the source node, sends the packet to the next hop directly
            scheduler.send(packet);
        } else {
            // if the scheduler is not initialized, initiates a new connection for the first packet of the flow

            // obtains the next hop id from the routing table
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

            // obtains the remote address for the next hop node
            let remote_addr = match self.node_addresses.get(&next_hop_id) {
                Some(addr) => addr.clone(),
                None => {
                    error!("No remote address found for node id: {}", next_hop_id);
                    return;
                }
            };

            let tcp_max_client = self.tcp_max_client.as_ref().unwrap();

            // establishes a TCP max connection to the next-hop node
            let stream = tcp_max_client.connect(packet.flow_id, &remote_addr).await;

            // initializes a scheduler which is then stored and used for all subsequent packets in that flow
            let scheduler = tcp_max_client
                .initialize_scheduler(stream, next_hop_id)
                .await;

            // sends the first packet of the flow with the new scheduler
            scheduler.send(packet);

            // stores the flow id to the scheduler into hashmap for sending subsequent packets
            self.schedulers.insert(flow_id, scheduler);
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
            self.schedulers.insert(flow_id.reverse(), scheduler);
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

            // spawns a task to perform zero-copy bidirectional splicing between inbound and outbound streams
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
}
