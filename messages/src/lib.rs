/// Defines message enums for controller-dataplane communication.
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
    pub flow_id: Vec<i32>,
    // pub stream_id: Option<String>,
    pub bps: usize,
    pub src_node_id: Option<usize>,
    pub time_read: chrono::DateTime<chrono::Utc>,
}

/// The transport protocol used to transfer data between nodes
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Quic,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type")]
pub enum ControllerToDataplane {
    StartUp {
        node_id: usize,
        addr: [u8; 4],
        net_mask: [u8; 4],
        session_id: [u8; 4],
        num_interfaces: usize,
        protocol: Protocol,
    },
    AddNode {
        protocol: Protocol,
        node_id: usize,
        addr: String,
    },
    InstallFlow {
        flows: Vec<Flow>,
    },
    SetLinkRate {
        node_id: usize,
        rate: usize,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Flow {
    pub flow_id: Vec<u8>,
    pub routes: Vec<RouteInfo>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RouteInfo {
    pub id: usize,
    pub next_hop: usize,
    // pub streams: Vec<String>,
}
