/// Defines message enums for controller-dataplane communication.
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum DataplaneToController {
    StartUp {
        private_network_name: String,
        private_network_addr: String,
        public_network_addr: String,
        node_id: Option<usize>,
    },
    Metrics {
        metrics: Vec<Metric>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metric {
    pub flow_id: [u8; 16],
    pub local_node_id: usize,
    pub remote_node_id: usize,
    pub bytes: usize,
    pub time_read: chrono::DateTime<chrono::Utc>,
}

/// The transport protocol used to transfer data between nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Tcp,
    Quic,
}

/// Configuration for a single flow
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct FlowConfig {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub remote_addr: [u8; 4],
    pub client_port: u16,
    pub flow_rate: Option<u64>,
    pub flow_size: Option<u64>,
    pub duration: Option<u64>,
    pub start_time: Option<u64>,
}
/// The specification of a token bucket traffic shaper.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TokenBucketSpec {
    pub rate: usize,
    pub bucket_size: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type")]
pub enum ControllerToDataplane {
    StartUp {
        node_id: usize,
        addr: [u8; 4],
        net_mask: [u8; 4],
        smoltcp_addr: [u8; 4],
        smoltcp_net_mask: [u8; 4],
        smoltcp_client_port_base: u16,
        smoltcp_server_port: u16,
        flow_configs: Vec<FlowConfig>,
        incoming_flows_count: usize,
        protocol: Protocol,
    },
    AddNode {
        remote_node_id: usize,
        remote_addr: String,
    },
    InstallRoutes {
        routes: Vec<RoutingTableEntry>,
    },
    SetLinkRate {
        node_id: usize,
        spec: TokenBucketSpec,
    },
}

/// Routing table entry: route_id → next_hop, with source and destination node IDs
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RoutingTableEntry {
    pub route_id: usize,
    pub next_hop: usize,
    pub src_node_id: usize,
    pub dst_node_id: usize,
}
