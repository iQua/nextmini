/// Defines message enums for controller-dataplane communication.
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

mod ip_ser;

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

/// The traffic specification for a user-space TCP flow.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Flow {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub flow_spec: FlowSpec,
}

/// The specification of a user-space TCP flow.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct FlowSpec {
    pub flow_len: FlowLen,
    #[serde(default)]
    pub flow_rate: Option<usize>,
    #[serde(default)]
    pub flow_weight: Option<usize>,
}

/// The length of a user-space TCP flow, specified either by the number of bytes or by the duration of the flow.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FlowLen {
    Bytes(usize),
    Duration(f64),
}

impl FlowLen {
    pub fn exceeded(&self, sent_size: u64, start_time: std::time::Instant) -> bool {
        match *self {
            FlowLen::Bytes(size) => sent_size >= size as u64,
            FlowLen::Duration(duration) => start_time.elapsed().as_secs_f64() >= duration,
        }
    }
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
        #[serde(with = "ip_ser")]
        net_mask: Ipv4Addr,
        #[serde(with = "ip_ser")]
        virtual_base_addr: Ipv4Addr,
        #[serde(with = "ip_ser")]
        user_space_base_addr: Ipv4Addr,
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
    AddFlows {
        flows: Vec<Flow>,
    },
    SetFlowWeight {
        src_ip: u32,
        dst_ip: u32,
        src_port: u16,
        dst_port: u16,
        weight: usize,
    },
}

/// Routing table entry: route_id → next_hop, with source and destination node IDs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RoutingTableEntry {
    pub route_id: usize,
    pub next_hop: usize,
    pub src_node_id: usize,
    pub dst_node_id: usize,
}
