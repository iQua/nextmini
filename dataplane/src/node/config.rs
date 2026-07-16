use std::net::Ipv4Addr;
use std::time::Duration;

use tokio_tungstenite::tungstenite::{Error, Message};

use clap_serde_derive::ClapSerde;
use clap_serde_derive::clap;
use clap_serde_derive::clap::Parser;
use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
use serde::Deserialize;
use serde::de::Deserializer;
use tracing::{error, info, warn};

use nextmini_messages::lossless_session::FecFeedbackMode;
use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, FlowTransport, INVALID, OperatingMode,
    Protocol, SchedulingDiscipline, TokenBucketSpec,
};

use crate::node::scheduler::drop::DropStrategy;
use crate::node::{FlowId, FlowIdExt, NodeId, NodeIdExt};

/// The choice of congestion control algorithm in QUIC. Only BBR and CUBIC are supported by s2n-quic.
#[derive(Clone, Default, Debug, PartialEq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum CongestionControl {
    #[default]
    Bbr,
    Cubic,
}

/// The processing mode for processing packets
#[derive(Clone, Default, Debug, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Feature {
    /// The sequential feature guarantees that no packets are reordered throughout the entire path,
    /// by processing packets consistently using one of the packet processors.
    #[default]
    Sequential,
    /// The concurrent feature allows packets to be processed in parallel by multiple packet processors,
    /// therefore packets may be reordered. They are put back in order before being sent to the TUN interface.
    /// This feature is useful for high throughput applications, when a flow can be split into multiple
    /// paths over the network.
    Concurrent,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LosslessFecScheme {
    #[default]
    RaptorQ,
    Mettle,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LosslessRuntimeSessionMode {
    #[default]
    Plain,
    Fec,
    Mettle,
    Cloudcast,
}

impl LosslessRuntimeSessionMode {
    pub const fn is_cloudcast(self) -> bool {
        matches!(self, Self::Cloudcast)
    }
}

#[derive(Parser)]
#[command(author, version, about)]
pub struct Args {
    /// The address of the controller to connect to (e.g., 128.100.100.128).
    #[arg(value_name = "CONTROLLER_ADDR")]
    pub controller_addr_override: Option<String>,

    /// The path to the configuration file.
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,

    /// All other configuration options for the dataplane node.
    #[clap(flatten)]
    pub args: <LocalConfig as ClapSerde>::Opt,
}

/// Configuration options for the dataplane, read from a configuration file or from the command-line.
#[derive(ClapSerde, Debug, Clone, Deserialize)]
pub struct LocalConfig {
    /// The server address.
    #[default("".to_string())]
    #[arg(long)]
    pub controller_addr: String,

    /// Timeout (ms) for establishing the initial WebSocket connection to the controller.
    #[default(5000)]
    #[arg(skip)]
    pub controller_connect_timeout_ms: u64,

    /// The path of the configuration file.
    /// This is used to remember which config file produced this configuration so namespace children can reuse it.
    #[default("".to_string())]
    #[serde(skip)]
    #[arg(skip)]
    pub config_path: String,

    /// The name of the private network this node might share with other nodes. This is used to identify
    /// nodes on the same network. Not specifying this field will cause the node to connect to other nodes
    /// via the public network addresses only.
    #[default("".to_string())]
    #[arg(long)]
    pub private_network_name: String,

    /// The name of the local network interface to use for communicating between nodes on the same subnet.
    #[default("eth0".to_string())]
    #[arg(long)]
    pub private_network_interface: String,

    /// The port to use for communicating between nodes on the same network.
    #[default("8080".to_string())]
    #[arg(long)]
    pub private_network_port: String,

    /// The name of the local network interface to use for communciating with nodes from a different subnet.
    #[default("eth0".to_string())]
    #[arg(long)]
    pub public_network_interface: String,

    /// The port to use for communicating with nodes from a different subnet.
    #[default("8080".to_string())]
    #[arg(long)]
    pub public_network_port: String,

    /// The port for the connection-on-demand TCP server operating in both normal and max mode to listen on
    #[default(8081)]
    #[arg(skip)]
    pub max_server_port: u16,

    #[default(0)]
    #[arg(long)]
    pub node_id: NodeId,

    /// The node ID offset added to computed node IDs for namespace mode to avoid clashes across VMs.
    #[default(0)]
    #[arg(long)]
    pub node_id_offset: usize,

    /// If true, enable IPv4 forwarding on the host, writes /proc/sys/net/ipv4/ip_forward=1.
    #[default(false)]
    #[arg(long)]
    pub auto_enable_ip_forward: bool,

    /// If true, add FORWARD rules between the namespace bridge and the outbound interface.
    #[default(false)]
    #[arg(long)]
    pub auto_add_forward_rules: bool,

    /// If true, add a MASQUERADE rule for the namespace subnet on the outbound interface.
    #[default(false)]
    #[arg(long)]
    pub auto_add_nat: bool,

    /// Enables the kernel-backed local interface (TUN). Disable when embedding the dataplane in-process.
    #[default(true)]
    #[arg(long)]
    pub enable_local_interface: bool,

    /// This is not used in metrics collector.
    /// The interval at which metrics are collected and sent to the controller.
    #[default(5)]
    #[arg(long)]
    pub metrics_collection_interval: u64,

    /// The total number of dataplane nodes deployed.
    #[default(1)]
    #[arg(long)]
    pub n_nodes: usize,

    /// The number of local TUN queues to use to send and receive packets.
    #[default(1)]
    #[arg(long)]
    pub num_tun_queues: usize,

    /// The number of packet processors to use to process packets. Recommended to use a larger number on a machine
    /// with multiple cores for greater throughput. Use the number of CPU cores if set to 0.
    #[default(0)]
    #[arg(long)]
    pub num_packet_processors: usize,

    /// The capacity for all channels between actors.
    #[default(1000)]
    #[arg(long)]
    pub channel_capacity: usize,

    /// The address of the local network interface to use for communicating between nodes on the same subnet.
    #[default("".to_string())]
    #[arg(long)]
    pub private_network_addr: String,

    /// The address of the network interface to use for communciating with nodes over the public Internet.
    #[default("".to_string())]
    #[arg(long)]
    pub public_network_addr: String,

    /// The name of the local TUN interface.
    #[default("utun".to_string())]
    #[arg(long)]
    pub tun_interface_name: String,

    /// The MTU of the tun interface. A larger value reduces CPU load when trasmit large files, but increases
    /// cost of retransmission. The maximum value is 6400.
    #[default(1400)]
    #[arg(long)]
    pub mtu: i32,

    /// Set if the node should restart when the connection to the server is lost.
    #[default(false)]
    #[arg(long)]
    pub restart_on_disconnect: bool,

    /// Maximum microseconds to hold a flow waiting for a missing TCP segment before emitting newer data.
    #[default(500)]
    #[arg(long)]
    pub delay_tolerance: u64,

    /// Maximum number of queued TCP data packets to tolerate before forcing delivery.
    /// Set to 0 to disable backlog-based advancement.
    #[default(0)]
    #[arg(long)]
    pub backlog_tolerance: u64,

    /// If true, reorder TCP packets locally based on sequence numbers before delivery.
    #[default(true)]
    #[arg(long)]
    pub enforce_tcp_order: bool,

    /// QUIC congestion control algorithm to use.
    #[default(CongestionControl::Bbr)]
    #[arg(long, value_enum)]
    pub quic_congestion_control: CongestionControl,

    /// The local network address.
    #[default(default_local_address())]
    #[arg(skip)]
    pub local_address: Ipv4Addr,

    /// The tun virtual base network address from the controller.
    #[default(default_virtual_base_addr())]
    #[arg(skip)]
    pub virtual_base_addr: Ipv4Addr,

    /// The user-space network address.
    #[default(default_user_space_address())]
    #[arg(skip)]
    pub user_space_address: Ipv4Addr,

    /// The user-space base network address.
    #[default(default_user_space_base_addr())]
    #[arg(skip)]
    pub user_space_base_addr: Ipv4Addr,

    /// The external network address.
    #[default(default_external_base_address())]
    #[arg(skip)]
    pub external_base_address: Ipv4Addr,

    /// The external base network address.
    #[default(default_external_base_addr())]
    #[arg(skip)]
    pub external_base_addr: Ipv4Addr,

    /// The local network mask.
    #[default(default_netmask())]
    #[arg(skip)]
    pub local_netmask: Ipv4Addr,

    /// The transport protocol: TCP or QUIC.
    #[default(Protocol::Tcp)]
    #[arg(long, value_enum)]
    pub protocol: Protocol,

    /// The scheduling discipline.
    #[default(SchedulingDiscipline::Fifo)]
    #[arg(long, value_enum)]
    pub scheduler_type: SchedulingDiscipline,

    /// The capacity of each scheduler queue.
    #[default(1000)]
    #[arg(long)]
    pub queue_capacity: usize,

    /// The drop strategy for the scheduler.
    #[default(DropStrategy::TailDrop)]
    #[arg(long, value_enum)]
    pub scheduler_drop_strategy: DropStrategy,

    /// The processing mode for processing packets.
    #[default(Feature::Sequential)]
    #[arg(long, value_enum)]
    pub feature: Feature,

    /// The operating mode.
    #[default(OperatingMode::Normal)]
    #[arg(skip)]
    pub operating_mode: OperatingMode,

    /// If true, dataplane packet channels apply backpressure instead of
    /// dropping when full. When false (default), channels use try_send().
    #[default(false)]
    #[arg(long)]
    pub channel_backpressure: bool,

    /// The flow config received from the controller.
    #[default(vec![Flow {
        controller_id: None,
        lossless_session_id: None,
        src_node_id: 0,
        dst_node_id: 0,
        route_id: None,
        flow_spec: FlowSpec {
            flow_len: FlowLen::Bytes(1_000_000_000),
            flow_rate: None,
            flow_weight: None,
            transport: FlowTransport::Tcp,
        },
    }])]
    #[arg(skip)]
    pub flow: Vec<Flow>,

    /// The user space client port automatically assigned by dataplane.
    #[default(45535)]
    #[arg(long)]
    pub user_space_client_port: u16,

    /// The user space server port automatically assigned by dataplane.
    #[default(8888)]
    #[arg(long)]
    pub user_space_server_port: u16,

    /// The linux bridge name for namespace isolation.
    #[default("isobr0".to_string())]
    #[arg(skip)]
    pub bridge_name: String,

    /// The IPv4 address to assign to the bridge.
    #[default("172.16.8.1".to_string())]
    #[arg(skip)]
    pub bridge_ip: String,

    /// The subnet mask length (CIDR) associated with `bridge_ip`.
    #[default(16)]
    #[arg(skip)]
    pub subnet: u8,

    /// Amount of time to wait (ms) between node creation in namespace mode.
    #[default(50)]
    #[arg(skip)]
    pub interval_between_spawn: u64,

    /// Maximum time to wait (ms) for a veth interface to establish carrier in namespace mode.
    #[default(20000)]
    #[arg(skip)]
    pub carrier_max_wait_ms: u64,

    /// Poll interval (ms) for carrier checks in namespace mode.
    #[default(100)]
    #[arg(skip)]
    pub carrier_poll_interval_ms: u64,

    /// Additional delay after spawning child before starting handshake (ms) in namespace mode.
    #[default(150)]
    #[arg(skip)]
    pub child_start_delay_ms: u64,

    /// Timeout (ms) for parent waiting on child network setup handshake in namespace mode.
    #[default(25000)]
    #[arg(skip)]
    pub handshake_timeout_ms: u64,

    /// The default configuration for the lossless runtime.
    #[default(Default::default())]
    #[arg(skip)]
    pub lossless_runtime_config: LosslessConfig,

    /// Optional namespace-backed lossless integration-test harness settings.
    #[default(Default::default())]
    #[arg(skip)]
    pub integration_test: IntegrationTestConfig,
}

impl LocalConfig {
    /// Creates a `LocalConfig` from a TOML string while applying ClapSerde defaults.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn from_toml_str(toml_str: &str) -> Result<LocalConfig, toml::de::Error> {
        let mut opt: <LocalConfig as ClapSerde>::Opt = toml::from_str(toml_str)?;
        Ok(LocalConfig::from(&mut opt))
    }

    /// Converts IP address to node ID, supporting both TUN and user space networks.
    pub fn ip_to_node_id(&self, ip: Ipv4Addr) -> NodeId {
        let ip_addr = u32::from(ip);
        let netmask = u32::from(self.local_netmask);

        let tun_base = u32::from(self.virtual_base_addr);
        let user_space_base = u32::from(self.user_space_base_addr);
        let external_base = u32::from(self.external_base_addr);

        match ip_addr & netmask {
            subnet if subnet == (tun_base & netmask) && ip_addr >= tun_base => {
                (ip_addr - tun_base) as NodeId
            }
            subnet if subnet == (user_space_base & netmask) && ip_addr >= user_space_base => {
                (ip_addr - user_space_base) as NodeId
            }
            // binds the external client/server address to the node ID
            subnet if subnet == (external_base & netmask) && ip_addr >= external_base => {
                (ip_addr - external_base) as NodeId
            }
            _ => INVALID,
        }
    }

    /// Attempts to extract node IDs, returning `None` when either IP is outside the configured subnets.
    pub fn try_extract_node_ids_from_flow(&self, flow_id: FlowId) -> Option<(NodeId, NodeId)> {
        let src_ip = flow_id.src_ip();
        let dst_ip = flow_id.dst_ip();
        let src_node_id = self.ip_to_node_id(src_ip);
        let dst_node_id = self.ip_to_node_id(dst_ip);

        if src_node_id == INVALID || dst_node_id == INVALID {
            None
        } else {
            Some((src_node_id, dst_node_id))
        }
    }

    /// Returns the effective settings for TCP reordering tolerance.
    pub fn reorder_tolerances(&self) -> (bool, Option<Duration>, usize) {
        let enforce = self.enforce_tcp_order;
        let gap_timeout = match (enforce, self.delay_tolerance) {
            (true, micros) if micros > 0 => Some(Duration::from_micros(micros)),
            _ => None,
        };
        let backlog = if enforce && self.backlog_tolerance > 0 {
            self.backlog_tolerance.min(usize::MAX as u64) as usize
        } else {
            0
        };

        (enforce, gap_timeout, backlog)
    }

    /// Parses config file from config.toml.
    fn from_file_and_args() -> (LocalConfig, Option<String>) {
        let mut args = Args::parse();

        let mut cfgs = match std::fs::read_to_string(&args.config_path) {
            Ok(content) => match toml::from_str::<<LocalConfig as ClapSerde>::Opt>(&content) {
                Ok(cfgs) => LocalConfig::from(cfgs).merge(&mut args.args),
                Err(e) => {
                    info!("Failed to parse config file (with error: {e}), using default values.");
                    LocalConfig::from(&mut args.args)
                }
            },
            Err(e) => {
                let fname = &args.config_path.clone();
                info!(
                    "Failed to read the config file '{fname}' (with error: {e}), using default values."
                );
                LocalConfig::from(&mut args.args)
            }
        };

        let controller_addr_args = args.controller_addr_override;

        // remembers which config file produced this configuration so namespace children can reuse it
        cfgs.config_path = args.config_path.clone();

        (cfgs, controller_addr_args)
    }

    /// Creates a new instance of LocalConfig.
    pub fn new() -> LocalConfig {
        let (mut cfgs, controller_addr_args) = Self::from_file_and_args();

        // overrides controller_addr from command line if provided
        if let Some(addr) = controller_addr_args {
            cfgs.controller_addr = addr;
        }

        cfgs.normalize_controller_addr();
        cfgs.populate_runtime_defaults();

        cfgs
    }

    fn normalize_controller_addr(&mut self) {
        let trimmed = self.controller_addr.trim();
        if trimmed.is_empty() {
            return;
        }

        // Allow controller_addr to be specified as either a full websocket URL (ws:// / wss://)
        // or as a host:port pair (e.g., 127.0.0.1:3000).
        if trimmed.contains("://") {
            self.controller_addr = trimmed.to_string();
        } else {
            self.controller_addr = format!("ws://{trimmed}");
        }
    }

    /// Populates runtime-derived defaults such as interface addresses when they are missing.
    pub fn populate_runtime_defaults(&mut self) {
        // sets the private ipv4 address of the network interface for the private network
        // Defined by RFC 1918, private IP addresses fall within the following ranges:
        // 10.0.0.0 - 10.255.255.255 (10.0.0.0/8)
        // 172.16.0.0 - 172.31.255.255 (172.16.0.0/12)
        // 192.168.0.0 - 192.168.255.255 (192.168.0.0/16)
        if self.private_network_addr.is_empty() {
            let itf_name = self.private_network_interface.clone();

            // retrieves a list of all private network interfaces available on the system
            let network_interfaces = NetworkInterface::show().expect(
                "Failed to get the network interfaces. This is like due to the lack of privileges.",
            );

            let mut ipv4addr = String::new();

            // iterates over the list of network interfaces to find the one matching the specified name,
            // such as 'eth0'
            for itf in network_interfaces.iter() {
                if itf.name == itf_name {
                    // for the matching interface, iterates over its associated addresses, looking for an
                    // ipv4 address
                    for addr in itf.addr.iter() {
                        if let Addr::V4(ipv4) = addr {
                            ipv4addr = ipv4.ip.to_string();
                        }
                    }
                }
            }

            // obtains the ipv4 address of the dataplane node
            self.private_network_addr = ipv4addr;

            if self.node_id == 0 && self.n_nodes == 1 {
                // Computes node_id from private_network_addr using external_base_addr for external
                // client/server deployments. Only do this when the real IP shares the same prefix
                // as external_base_addr; otherwise the subtraction underflows and produces a huge ID.
                if let Ok(real_ip) = self.private_network_addr.parse::<Ipv4Addr>() {
                    let ip = u32::from(real_ip);
                    let base = u32::from(self.external_base_addr);
                    let netmask = u32::from(self.local_netmask);

                    let same_subnet = (ip & netmask) == (base & netmask);
                    if same_subnet && ip >= base {
                        let computed_node_id = (ip - base) as NodeId;
                        if computed_node_id != 0 {
                            self.node_id = computed_node_id;
                            info!(
                                "From real IP {} using external_base_addr, node_id is: {}.",
                                self.private_network_addr, self.node_id
                            );
                        }
                    } else if !same_subnet {
                        warn!(
                            "Real IP {} does not match external_base_addr {} under netmask {}; \
                            leaving node_id unset. Set node_id explicitly if needed.",
                            real_ip, self.external_base_addr, self.local_netmask
                        );
                    } else {
                        warn!(
                            "Real IP {} is lower than external_base_addr {} under netmask {}; \
                            leaving node_id unset. Set node_id explicitly if needed.",
                            real_ip, self.external_base_addr, self.local_netmask
                        );
                    }
                } else if !self.private_network_addr.is_empty() {
                    error!(
                        "Failed to parse private_network_addr as Ipv4Addr: {}.",
                        self.private_network_addr
                    );
                }
            }
        }

        // sets the ipv4 address of the network interface for the public network
        if self.public_network_addr.is_empty() {
            let itf_name = self.public_network_interface.clone();

            // retrieves a list of all public network interfaces available on the system
            let network_interfaces = NetworkInterface::show().expect(
                "Failed to get the network interfaces. This is like due to the lack of privileges.",
            );

            let mut ipv4addr = String::new();

            // iterates over the list of network interfaces to find the one matching the specified name,
            // such as 'eth0'
            for itf in network_interfaces.iter() {
                if itf.name == itf_name {
                    // for the matching interface, iterates over its associated addresses, looking for an
                    // ipv4 address
                    for addr in itf.addr.iter() {
                        if let Addr::V4(ipv4) = addr {
                            ipv4addr = ipv4.ip.to_string();
                        }
                    }
                }
            }

            self.public_network_addr = ipv4addr;
        }

        // sets the number of packet processors to the number of threads if it is 0
        if self.num_packet_processors == 0 {
            self.num_packet_processors = num_cpus::get();
        }

        self.lossless_runtime_config.ingress_feature = self.feature.clone();
        self.lossless_runtime_config.ingress_channel_backpressure = self.channel_backpressure;
        let lossless_control_capacity = self.channel_capacity.max(1024);
        self.lossless_runtime_config.runtime_message_capacity = lossless_control_capacity;
        self.lossless_runtime_config.session_control_inbox_capacity = lossless_control_capacity;
        self.lossless_runtime_config.session_inbox_capacity = self.channel_capacity.max(1);
    }

    pub fn update(&mut self, response: Result<Message, Error>) {
        match response {
            Ok(Message::Binary(data)) => {
                let controller_response =
                    match rmp_serde::from_slice::<ControllerToDataplane>(&data) {
                        Ok(response) => response,
                        Err(e) => {
                            error!("Failed to parse a message from the controller: {}", e);
                            return;
                        }
                    };

                match controller_response {
                    ControllerToDataplane::StartUp {
                        node_id,
                        net_mask,
                        virtual_base_addr,
                        user_space_base_addr,
                        external_base_addr,
                        max_server_port,
                        protocol,
                        scheduler_type,
                        node_spec,
                    } => {
                        self.node_id = node_id;
                        self.protocol = protocol;
                        self.local_netmask = net_mask;
                        // sets three base addresses for the node
                        self.virtual_base_addr = virtual_base_addr;
                        self.user_space_base_addr = user_space_base_addr;
                        self.external_base_addr = external_base_addr;
                        // calculates the local, user space and external addresses for the node
                        self.local_address = node_id.ip_addr(virtual_base_addr, net_mask);
                        self.user_space_address = node_id.ip_addr(user_space_base_addr, net_mask);
                        // self.external_address = node_id.ip_addr(external_base_addr, net_mask);
                        self.max_server_port = max_server_port;
                        self.scheduler_type = scheduler_type;
                        self.operating_mode = node_spec.operating_mode;
                    }

                    // Adds flows message.
                    ControllerToDataplane::AddFlows { flows } => {
                        self.flow = flows;
                    }

                    _ => {
                        error!(
                            "A message with an unexpected type has been received from the controller."
                        );
                    }
                }
            }
            Ok(_) => warn!(
                "Received a message that is not a binary or a ping message. Something may be wrong."
            ),
            Err(e) => {
                error!("Error receiving the message: {}", e);
            }
        }
    }
}

/// Lossless session configuration knobs (consumed when the lossless subsystem is enabled).
#[derive(Debug, Clone, Deserialize)]
pub struct LosslessConfig {
    /// Default data block size in bytes.
    pub default_block_size: usize,

    /// Local lossless runtime mode. `cloudcast` uses plain block payloads striped across tree scopes.
    #[serde(default)]
    pub session_mode: LosslessRuntimeSessionMode,

    /// Optional token-bucket for data pacing (bytes/sec, bucket size bytes).
    pub data_bucket: Option<TokenBucketSpec>,

    /// Grace period (ms) to wait for receiver READY during session start.
    #[serde(default = "default_ready_grace_ms")]
    pub ready_grace_ms: u64,

    /// Maximum time to wait for post-`SourceDone` receiver feedback before aborting.
    #[serde(default = "default_peer_report_timeout_ms")]
    pub peer_report_timeout_ms: u64,

    /// Global kill-switch for FEC sessions. When false, all FEC session requests are rejected.
    #[serde(default = "default_fec_enabled")]
    pub fec_enabled: bool,

    /// Default `symbols_per_block` used when runtime derives sender manifests internally.
    #[serde(default = "default_fec_default_symbols_per_block")]
    pub fec_default_symbols_per_block: u32,

    /// Default FEC scheme used when runtime derives sender manifests internally.
    #[serde(default)]
    pub fec_default_scheme: LosslessFecScheme,

    /// Feedback protocol used by FEC sender sessions.
    #[serde(default)]
    pub fec_feedback_mode: FecFeedbackMode,

    /// Tree IDs used for FEC symbol striping.
    #[serde(default = "default_fec_default_tree_ids")]
    pub fec_default_tree_ids: Vec<u16>,

    /// Optional tree weights used by tree-scoped Cloudcast striping.
    #[serde(default)]
    pub fec_default_tree_weights: Vec<f64>,

    /// METTLE coded-rate numerator used to derive the stream overhead parameter.
    #[serde(default = "default_mettle_default_coded_rate_num")]
    pub mettle_default_coded_rate_num: u32,

    /// METTLE coded-rate denominator used to derive the stream overhead parameter.
    #[serde(default = "default_mettle_default_coded_rate_den")]
    pub mettle_default_coded_rate_den: u32,

    /// Number of Cloudcast stripes/partitions used to quantize tree weights.
    #[serde(default)]
    pub cloudcast_stripes: usize,

    /// Explicit Cloudcast stripe-to-tree table. Entry `i` is the tree used by stripe `i`.
    #[serde(default)]
    pub cloudcast_stripe_tree_ids: Vec<u16>,

    /// Effective processor ingress policy copied from `LocalConfig.feature`.
    /// Runtime preflight uses this to enforce sequential-only collaborative multi-tree mode.
    #[serde(skip)]
    pub ingress_feature: Feature,

    /// Effective ingress backpressure policy copied from `LocalConfig.channel_backpressure`.
    /// Runtime preflight uses this to reject FEC modes that can silently drop on full queues.
    #[serde(skip)]
    pub ingress_channel_backpressure: bool,

    /// Capacity of the runtime actor mailbox that forwards inbound lossless
    /// frames and lifecycle commands to the background session runtime.
    #[serde(skip)]
    pub runtime_message_capacity: usize,

    /// Capacity of each per-session control inbox between the runtime actor and
    /// the spawned sender/receiver task.
    #[serde(skip)]
    pub session_control_inbox_capacity: usize,

    /// Capacity of each per-session data inbox between the runtime actor and
    /// the spawned receiver task.
    #[serde(skip)]
    pub session_inbox_capacity: usize,
}

impl Default for LosslessConfig {
    fn default() -> Self {
        Self {
            default_block_size: 8500,
            session_mode: LosslessRuntimeSessionMode::Plain,
            data_bucket: None,
            ready_grace_ms: 1500,
            peer_report_timeout_ms: 15_000,
            fec_enabled: false,
            fec_default_symbols_per_block: 32,
            fec_default_scheme: LosslessFecScheme::RaptorQ,
            fec_feedback_mode: FecFeedbackMode::Rounds,
            fec_default_tree_ids: vec![0],
            fec_default_tree_weights: Vec::new(),
            mettle_default_coded_rate_num: 1,
            mettle_default_coded_rate_den: 1,
            cloudcast_stripes: 0,
            cloudcast_stripe_tree_ids: Vec::new(),
            ingress_feature: Feature::Sequential,
            ingress_channel_backpressure: true,
            runtime_message_capacity: 1024,
            session_control_inbox_capacity: 1024,
            session_inbox_capacity: 1024,
        }
    }
}

const fn default_ready_grace_ms() -> u64 {
    1500
}

const fn default_peer_report_timeout_ms() -> u64 {
    15_000
}

const fn default_fec_enabled() -> bool {
    false
}

const fn default_fec_default_symbols_per_block() -> u32 {
    32
}

fn default_fec_default_tree_ids() -> Vec<u16> {
    vec![0]
}

const fn default_mettle_default_coded_rate_num() -> u32 {
    1
}

const fn default_mettle_default_coded_rate_den() -> u32 {
    1
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
pub struct IntegrationTreeConfig {
    pub tree_id: usize,
    #[serde(deserialize_with = "deserialize_edge_pairs")]
    pub edges: Vec<(u32, u32)>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
pub struct IntegrationTestConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub case_name: String,
    #[serde(default)]
    pub group_label: String,
    #[serde(default)]
    pub source_node_id: usize,
    #[serde(default)]
    pub receiver_ids: Vec<usize>,
    #[serde(default)]
    pub artifact_dir: String,
    #[serde(default)]
    pub payload_path: String,
    #[serde(default)]
    pub synthetic_payload: bool,
    #[serde(default)]
    pub payload_size: u64,
    #[serde(default = "default_integration_group_timeout_ms")]
    pub group_timeout_ms: u64,
    #[serde(default = "default_integration_receive_timeout_ms")]
    pub receive_timeout_ms: u64,
    #[serde(default = "default_integration_poll_interval_ms")]
    #[allow(dead_code)]
    pub poll_interval_ms: u64,
    #[serde(default = "default_integration_src_port")]
    pub src_port: u16,
    #[serde(default = "default_integration_dst_port")]
    pub dst_port: u16,
    #[serde(default)]
    pub block_size: usize,
    #[serde(default)]
    pub trees: Vec<IntegrationTreeConfig>,
}

#[allow(dead_code)]
impl IntegrationTestConfig {
    pub fn is_receiver(&self, node_id: usize) -> bool {
        self.receiver_ids.contains(&node_id)
    }

    pub fn role_for_node(&self, node_id: usize) -> Option<IntegrationNodeRole> {
        if !self.enabled {
            return None;
        }

        if node_id == self.source_node_id {
            return Some(IntegrationNodeRole::Source);
        }

        if self.is_receiver(node_id) {
            return Some(IntegrationNodeRole::Receiver);
        }

        Some(IntegrationNodeRole::Router)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationNodeRole {
    Source,
    Receiver,
    Router,
}

fn default_integration_group_timeout_ms() -> u64 {
    120_000
}

fn default_integration_receive_timeout_ms() -> u64 {
    600_000
}

fn default_integration_poll_interval_ms() -> u64 {
    200
}

fn default_integration_src_port() -> u16 {
    45_000
}

fn default_integration_dst_port() -> u16 {
    46_000
}

fn deserialize_edge_pairs<'de, D>(deserializer: D) -> Result<Vec<(u32, u32)>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum EdgeRepr {
        EdgePairs(Vec<Vec<u32>>),
        NodeSequence(Vec<u32>),
    }

    let repr = EdgeRepr::deserialize(deserializer)?;
    match repr {
        EdgeRepr::EdgePairs(edge_pairs) => {
            let mut edges = Vec::with_capacity(edge_pairs.len());
            for pair in edge_pairs {
                if pair.len() != 2 {
                    return Err(serde::de::Error::custom(
                        "Each edge must have exactly two nodes.",
                    ));
                }
                edges.push((pair[0], pair[1]));
            }
            Ok(edges)
        }
        EdgeRepr::NodeSequence(nodes) => {
            if nodes.len() < 2 {
                return Err(serde::de::Error::custom(
                    "Route must have at least two nodes.",
                ));
            }
            Ok(nodes
                .windows(2)
                .map(|window| (window[0], window[1]))
                .collect())
        }
    }
}

fn default_local_address() -> Ipv4Addr {
    Ipv4Addr::new(10, 0, 0, 1)
}

fn default_virtual_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(10, 0, 0, 0)
}

fn default_user_space_address() -> Ipv4Addr {
    Ipv4Addr::new(192, 168, 0, 1)
}

fn default_user_space_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(192, 168, 0, 0)
}

// The external base address for default external client.
fn default_external_base_address() -> Ipv4Addr {
    Ipv4Addr::new(172, 16, 8, 3)
}

// The external base address for external traffic.
fn default_external_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(172, 16, 8, 3)
}

fn default_netmask() -> Ipv4Addr {
    Ipv4Addr::new(255, 255, 255, 0)
}

#[cfg(test)]
mod tests {
    use super::{
        FecFeedbackMode, IntegrationNodeRole, IntegrationTestConfig, LocalConfig, LosslessConfig,
        LosslessFecScheme, LosslessRuntimeSessionMode, deserialize_edge_pairs,
    };
    use serde::Deserialize;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    #[test]
    fn default_reorder_tolerances_match_defaults() {
        let cfg = LocalConfig::default();
        let (enforce, gap, backlog) = cfg.reorder_tolerances();
        assert!(enforce);
        assert_eq!(gap, Some(Duration::from_micros(500)));
        // Default backlog tolerance is disabled (0) when enforcement is enabled.
        assert_eq!(backlog, 0);
    }

    #[test]
    fn unordered_mode_disables_tolerances() {
        let cfg = LocalConfig {
            enforce_tcp_order: false,
            delay_tolerance: 123,
            backlog_tolerance: 99,
            ..Default::default()
        };

        let (enforce, gap, backlog) = cfg.reorder_tolerances();
        assert!(!enforce);
        assert_eq!(gap, None);
        assert_eq!(backlog, 0);
    }

    #[test]
    fn zero_tolerances_disable_components() {
        let cfg = LocalConfig {
            delay_tolerance: 0,
            backlog_tolerance: 0,
            ..Default::default()
        };

        let (_, gap, backlog) = cfg.reorder_tolerances();
        assert_eq!(gap, None);
        assert_eq!(backlog, 0);
    }

    #[test]
    fn normalize_controller_addr_prepends_ws_scheme_when_missing() {
        let mut cfg = LocalConfig {
            controller_addr: "127.0.0.1:3000".to_string(),
            ..Default::default()
        };

        cfg.normalize_controller_addr();
        assert_eq!(cfg.controller_addr, "ws://127.0.0.1:3000");
    }

    #[test]
    fn normalize_controller_addr_keeps_existing_scheme() {
        let mut cfg = LocalConfig {
            controller_addr: "wss://controller.example:3000".to_string(),
            ..Default::default()
        };

        cfg.normalize_controller_addr();
        assert_eq!(cfg.controller_addr, "wss://controller.example:3000");
    }

    #[test]
    fn ip_to_node_id_returns_invalid_when_ip_below_external_base() {
        let cfg = LocalConfig::default();
        let ip = Ipv4Addr::new(172, 16, 8, 2); // below default external_base_addr (172.16.8.3)
        assert_eq!(cfg.ip_to_node_id(ip), nextmini_messages::INVALID);
    }

    #[test]
    fn lossless_defaults_keep_fec_opt_in_and_explicit() {
        let cfg = LocalConfig::default();
        let lossless = cfg.lossless_runtime_config;

        assert_eq!(lossless.default_block_size, 8500);
        assert_eq!(lossless.session_mode, LosslessRuntimeSessionMode::Plain);
        assert!(
            !lossless.fec_enabled,
            "FEC must be explicit opt-in by default"
        );
        assert_eq!(lossless.fec_default_symbols_per_block, 32);
        assert_eq!(lossless.fec_default_scheme, LosslessFecScheme::RaptorQ);
        assert_eq!(lossless.fec_feedback_mode, FecFeedbackMode::Rounds);
        assert_eq!(lossless.fec_default_tree_ids, vec![0]);
        assert!(lossless.fec_default_tree_weights.is_empty());
        assert_eq!(lossless.mettle_default_coded_rate_num, 1);
        assert_eq!(lossless.mettle_default_coded_rate_den, 1);
        assert_eq!(lossless.ingress_feature, super::Feature::Sequential);
        assert!(
            lossless.ingress_channel_backpressure,
            "lossless defaults should assume backpressured ingress unless synced from LocalConfig"
        );
    }

    #[test]
    fn populate_runtime_defaults_syncs_lossless_ingress_policy() {
        let mut cfg = LocalConfig {
            feature: super::Feature::Concurrent,
            channel_backpressure: false,
            private_network_addr: "127.0.0.1".to_string(),
            public_network_addr: "127.0.0.1".to_string(),
            ..Default::default()
        };

        cfg.populate_runtime_defaults();

        assert_eq!(
            cfg.lossless_runtime_config.ingress_feature,
            super::Feature::Concurrent
        );
        assert!(
            !cfg.lossless_runtime_config.ingress_channel_backpressure,
            "runtime ingress backpressure policy must mirror LocalConfig.channel_backpressure"
        );
    }

    #[test]
    fn lossless_fec_defaults_are_not_canonicalized() {
        let cfg = LosslessConfig {
            fec_default_symbols_per_block: 0,
            fec_default_scheme: LosslessFecScheme::Mettle,
            fec_default_tree_ids: vec![5, 1, 5, 3],
            fec_default_tree_weights: vec![2.0, 1.0],
            mettle_default_coded_rate_num: 21,
            mettle_default_coded_rate_den: 20,
            ..Default::default()
        };

        assert_eq!(cfg.fec_default_symbols_per_block, 0);
        assert_eq!(cfg.fec_default_scheme, LosslessFecScheme::Mettle);
        assert_eq!(cfg.fec_default_tree_ids, vec![5, 1, 5, 3]);
        assert_eq!(cfg.fec_default_tree_weights, vec![2.0, 1.0]);
        assert_eq!(cfg.mettle_default_coded_rate_num, 21);
        assert_eq!(cfg.mettle_default_coded_rate_den, 20);
    }

    #[test]
    fn lossless_carousel_feedback_mode_parses_from_config() {
        #[derive(Deserialize)]
        struct Wrapper {
            lossless_runtime_config: LosslessConfig,
        }

        let parsed: Wrapper = toml::from_str(
            r#"
            [lossless_runtime_config]
            default_block_size = 8192
            fec_feedback_mode = "carousel"
            "#,
        )
        .expect("carousel feedback mode should parse");

        assert_eq!(
            parsed.lossless_runtime_config.fec_feedback_mode,
            FecFeedbackMode::Carousel
        );
    }

    #[test]
    fn lossless_cloudcast_mode_parses_from_config() {
        #[derive(Deserialize)]
        struct Wrapper {
            lossless_runtime_config: LosslessConfig,
        }

        let parsed: Wrapper = toml::from_str(
            r#"
            [lossless_runtime_config]
            default_block_size = 8192
            session_mode = "cloudcast"
            fec_default_tree_ids = [0, 3]
            fec_default_tree_weights = [2.5, 1.0]
            cloudcast_stripes = 8
            cloudcast_stripe_tree_ids = [0, 0, 0, 0, 0, 3, 3, 3]
            "#,
        )
        .expect("cloudcast lossless runtime config should parse");

        assert_eq!(
            parsed.lossless_runtime_config.session_mode,
            LosslessRuntimeSessionMode::Cloudcast
        );
        assert_eq!(
            parsed.lossless_runtime_config.fec_default_tree_weights,
            vec![2.5, 1.0]
        );
        assert_eq!(parsed.lossless_runtime_config.cloudcast_stripes, 8);
        assert_eq!(
            parsed.lossless_runtime_config.cloudcast_stripe_tree_ids,
            vec![0, 0, 0, 0, 0, 3, 3, 3]
        );
    }

    #[test]
    fn integration_test_role_infers_source_receiver_and_router() {
        let cfg = IntegrationTestConfig {
            enabled: true,
            source_node_id: 1,
            receiver_ids: vec![4, 5],
            ..Default::default()
        };

        assert_eq!(cfg.role_for_node(1), Some(IntegrationNodeRole::Source));
        assert_eq!(cfg.role_for_node(4), Some(IntegrationNodeRole::Receiver));
        assert_eq!(cfg.role_for_node(2), Some(IntegrationNodeRole::Router));
    }

    #[test]
    fn integration_tree_edges_accept_edge_pairs() {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(deserialize_with = "deserialize_edge_pairs")]
            edges: Vec<(u32, u32)>,
        }

        let parsed: Wrapper =
            toml::from_str("edges = [[1, 2], [2, 4], [2, 5]]").expect("tree edges should parse");
        assert_eq!(parsed.edges, vec![(1, 2), (2, 4), (2, 5)]);
    }
}
