/// Defines message enums for controller-dataplane communication.
use std::fmt;
use std::net::Ipv4Addr;

use clap::ValueEnum;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

mod ip_ser;

/// Used to indicate that an integer value is invalid.
pub const INVALID: usize = usize::MAX;

/// Identifier for a multicast group allocated by the controller.
pub type GroupId = usize;

/// Routing table entry describing multicast fan-out from a node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GroupRoutingTableEntry {
    /// Route identifier equals the multicast group id for now.
    pub route_id: usize,
    pub next_hops: Vec<usize>,
    pub src_node_id: usize,
    pub group_id: GroupId,
}

/// Types of messages used to communicate from the dataplane to the controller.
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
    FlowFinished {
        flows: Vec<FlowFinishedInfo>,
    },
    UserFlowStart {
        flows: Vec<UserFlowStart>,
    },
    AppFlowStart {
        appflows: Vec<AppFlow>,
    },
    RouteAssigned {
        assignments: Vec<RouteAssignment>,
    },
    CreateGroup {
        group_id: GroupId,
        label: String,
    },
    JoinGroup {
        group_id: GroupId,
    },
    LeaveGroup {
        group_id: GroupId,
    },
}

/// The new app flow message reported to controller from a src node to dest node.
#[derive(Serialize, Deserialize, Debug)]
pub struct AppFlow {
    pub flow_id: [u8; 16],
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub start_time: i64,
}

/// The information about a finished flow.
#[derive(Serialize, Deserialize, Debug)]
pub struct FlowFinishedInfo {
    pub flow_id: [u8; 16],
    pub controller_id: Option<i32>,
    pub start_time: i64,
    pub finish_time: i64,
}

/// The information about the start of a user-space flow.
#[derive(Serialize, Deserialize, Debug)]
pub struct UserFlowStart {
    pub controller_id: i32,
    pub flow_id: [u8; 16],
    pub start_time: i64,
}

/// The information about a route assignment.
#[derive(Serialize, Deserialize, Debug)]
pub struct RouteAssignment {
    pub flow_id: [u8; 16],
    pub route_id: usize,
    pub time: i64,
}

/// Performance metrics for a particular flow on a link from a local node to remote node.
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
    Udp,
    Quic,
}

/// The scheduling discipline for all schedulers.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum SchedulingDiscipline {
    #[default]
    Fifo,
    Wrr,
}

/// The operating mode of a dataplane node.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum OperatingMode {
    #[default]
    Normal,
    Max,
}

/// How a dataplane route forwards traffic.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RouteForwardingMode {
    #[default]
    Unicast,
    Multicast,
}

impl RouteForwardingMode {
    pub fn is_multicast(self) -> bool {
        matches!(self, RouteForwardingMode::Multicast)
    }
}

fn default_route_forwarding_mode() -> RouteForwardingMode {
    RouteForwardingMode::Unicast
}

fn deserialize_forward_mode<'de, D>(deserializer: D) -> Result<RouteForwardingMode, D::Error>
where
    D: Deserializer<'de>,
{
    struct RouteForwardingModeVisitor;

    impl<'de> Visitor<'de> for RouteForwardingModeVisitor {
        type Value = RouteForwardingMode;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("a route forwarding mode (\"unicast\", \"multicast\", or a boolean)")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match value.to_ascii_lowercase().as_str() {
                "unicast" => Ok(RouteForwardingMode::Unicast),
                "multicast" => Ok(RouteForwardingMode::Multicast),
                other => Err(E::unknown_variant(other, &["unicast", "multicast"])),
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(if v {
                RouteForwardingMode::Multicast
            } else {
                RouteForwardingMode::Unicast
            })
        }
    }

    deserializer.deserialize_any(RouteForwardingModeVisitor)
}

/// The node specification for a dataplane node.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct NodeSpec {
    pub node_id: usize,
    #[serde(default)]
    pub operating_mode: OperatingMode,
}

/// The traffic specification for a user-space TCP flow.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Flow {
    pub controller_id: Option<i32>,
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

/// Types of messages from the controller to the dataplane.
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
        #[serde(with = "ip_ser")]
        external_base_addr: Ipv4Addr,
        #[serde(with = "ip_ser")]
        multicast_pool_base: Ipv4Addr,
        #[serde(with = "ip_ser")]
        multicast_pool_mask: Ipv4Addr,
        max_server_port: u16,
        protocol: Protocol,
        scheduler_type: SchedulingDiscipline,
        node_spec: NodeSpec,
    },
    AddNode {
        remote_node_id: usize,
        remote_addr: String,
    },
    AddNodeAddress {
        remote_node_id: usize,
        remote_max_server_addr: String,
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
    GroupCreated {
        group_id: GroupId,
        src_node_id: usize,
        label: String,
        success: bool,
        error_msg: Option<String>,
    },
    InstallGroupRoutes {
        group_id: GroupId,
        src_node_id: usize,
        routes: Vec<GroupRoutingTableEntry>,
    },
}

/// Routing table entry: route_id → next_hop, with source and destination node IDs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RoutingTableEntry {
    pub route_id: usize,
    pub next_hops: Vec<usize>,
    pub src_node_id: usize,
    pub dst_node_id: usize,
    #[serde(
        default = "default_route_forwarding_mode",
        deserialize_with = "deserialize_forward_mode"
    )]
    pub forward_mode: RouteForwardingMode,
}
