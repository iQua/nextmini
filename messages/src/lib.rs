/// Defines message enums for controller-dataplane communication.
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;

use clap::ValueEnum;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

mod ip_ser;
pub mod lossless_session;
pub use lossless_session::{
    FecCapabilities, FecManifest, FecScheme, FecStatus, LOSSLESS_SESSION_BASE_VERSION,
    LOSSLESS_SESSION_FEC_VERSION, LosslessSessionFecData,
};

/// Used to indicate that an integer value is invalid.
pub const INVALID: usize = usize::MAX;

/// Identifier for a multicast group allocated by the controller.
pub type GroupId = usize;

/// High-bit namespace separator for multicast route IDs inside dataplane tables.
///
/// Multicast route IDs produced by the controller must stay below this value.
pub const MULTICAST_ROUTE_FLAG: usize = 1 << 30;

/// Number of route-id slots reserved per multicast group for deterministic tree mapping.
///
/// Route IDs are computed as: `group_id * MULTITREE_STRIDE + tree_id`.
pub const MULTITREE_STRIDE: usize = 1 << 16;

/// Directory entry mapping a multicast group id to its allocated IP.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GroupDirectoryEntry {
    pub group_id: GroupId,
    #[serde(with = "ip_ser")]
    pub group_ip: Ipv4Addr,
}

/// Routing table entry describing multicast fan-out from a node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GroupRoutingTableEntry {
    /// Controller-defined deterministic multicast route identifier.
    pub route_id: usize,
    pub next_hops: Vec<usize>,
    pub src_node_id: usize,
    pub group_id: GroupId,
}

/// One multicast tree definition for a group route update.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GroupRouteTree {
    pub tree_id: usize,
    #[serde(default)]
    pub weight: Option<f64>,
    /// Directed edges (from_node_id, to_node_id) describing this tree.
    pub edges: Vec<(u32, u32)>,
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
    /// Indicates that a dataplane node has finished wiring its local topology.
    NodeTopologyReady {
        node_id: usize,
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
        label: String,
    },
    JoinGroup {
        group_id: GroupId,
    },
    LeaveGroup {
        group_id: GroupId,
    },
    /// Sets multicast DAG edges for a group. Intended for external optimizers (e.g. LP) that want
    /// to directly control the multicast tree without rewriting unicast routes.
    ///
    /// The controller is expected to validate that the sender is the group's source node.
    SetGroupRoutes {
        group_id: GroupId,
        /// Directed edges (from_node_id, to_node_id) describing the multicast DAG.
        edges: Vec<(u32, u32)>,
    },
    /// Sets multiple multicast trees for a group.
    SetGroupRoutesMulti {
        group_id: GroupId,
        trees: Vec<GroupRouteTree>,
    },
    /// Periodic lossless session stats from dataplane (feature-gated at source).
    LosslessStats {
        stats: LosslessStats,
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

/// Lossless session metrics (optional)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LosslessStats {
    pub session_id: u64,
    pub node_id: usize,
    /// "sender" | "receiver"
    pub role: String,
    pub bytes: u64,
    pub chunks: u64,
    pub resends: u64,
    pub repairs: u64,
    pub fec_used: u64,
    pub ts_ms: i64,
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

/// Transport selection for controller-managed flows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "snake_case")]
pub enum FlowTransport {
    #[default]
    Tcp,
    LosslessUnicast,
}

/// The traffic specification for a user-space TCP flow.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Flow {
    pub controller_id: Option<i32>,
    pub src_node_id: usize,
    pub dst_node_id: usize,
    #[serde(default)]
    pub route_id: Option<usize>,
    pub flow_spec: FlowSpec,
}

/// The specification of a user-space TCP flow.
#[derive(Serialize, PartialEq, Debug, Clone, Copy)]
pub struct FlowSpec {
    pub flow_len: FlowLen,
    #[serde(default)]
    pub flow_rate: Option<usize>, // bytes per second
    #[serde(default)]
    pub flow_weight: Option<usize>,
    #[serde(default)]
    pub transport: FlowTransport,
}

impl<'de> Deserialize<'de> for FlowSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FlowSpecSerde {
            flow_len: FlowLen,
            #[serde(default)]
            flow_rate: Option<usize>,
            #[serde(default)]
            flow_weight: Option<usize>,
            #[serde(default)]
            transport: FlowTransport,
        }

        let helper = FlowSpecSerde::deserialize(deserializer)?;
        let spec = FlowSpec {
            flow_len: helper.flow_len,
            flow_rate: helper.flow_rate,
            flow_weight: helper.flow_weight,
            transport: helper.transport,
        };
        spec.validate().map_err(de::Error::custom)?;
        Ok(spec)
    }
}

#[derive(Debug)]
pub enum FlowSpecValidationError {
    DurationMissingRate,
}

impl fmt::Display for FlowSpecValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowSpecValidationError::DurationMissingRate => {
                write!(f, "duration-based flows require flow_rate (bytes/sec)")
            }
        }
    }
}

impl std::error::Error for FlowSpecValidationError {}

impl FlowSpec {
    pub fn validate(&self) -> Result<(), FlowSpecValidationError> {
        match self.flow_len {
            FlowLen::Duration(_)
                if self.flow_rate.is_none()
                    && matches!(self.transport, FlowTransport::LosslessUnicast) =>
            {
                Err(FlowSpecValidationError::DurationMissingRate)
            }
            _ => Ok(()),
        }
    }
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

impl Hash for FlowLen {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            FlowLen::Bytes(size) => {
                0u8.hash(state);
                size.hash(state);
            }
            FlowLen::Duration(duration) => {
                1u8.hash(state);
                duration.to_bits().hash(state);
            }
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
    /// Signals that the controller has seen every expected dataplane node.
    TopologyReady,
    GroupCreated {
        group_id: GroupId,
        #[serde(with = "ip_ser")]
        group_ip: Ipv4Addr,
        src_node_id: usize,
    },
    InstallGroupDirectory {
        groups: Vec<GroupDirectoryEntry>,
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
