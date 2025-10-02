use std::net::Ipv4Addr;

use tokio_tungstenite::tungstenite::{Error, Message};

use clap_serde_derive::ClapSerde;
use clap_serde_derive::clap;
use clap_serde_derive::clap::Parser;
use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
use serde::Deserialize;
use tracing::{error, info, warn};

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, OperatingMode, Protocol, SchedulingDiscipline,
};

use crate::node::NodeId;
use crate::node::NodeIdExt;
use crate::node::scheduler::drop::DropStrategy;

/// The choice of congestion control algorithm in QUIC. Only BBR and CUBIC are supported by s2n-quic.
#[derive(Clone, Default, Debug, PartialEq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum CongestionControl {
    #[default]
    Bbr,
    Cubic,
}

/// The processing mode for processing packets
#[derive(Clone, Default, Debug, PartialEq, Deserialize, clap::ValueEnum)]
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

#[derive(Parser)]
#[command(author, version, about)]
pub struct Args {
    /// The address of the controller to connect to (e.g., 128.100.100.128).
    pub controller_addr: Option<String>,

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

    /// Reorder tolerance for the multipath mode.
    #[default(4)]
    #[arg(long)]
    pub reorder_tolerance: usize,

    /// The flow config received from controller.
    #[default(vec![Flow {
        controller_id: None,
        src_node_id: 0,
        dst_node_id: 0,
        flow_spec: FlowSpec {
            flow_len: FlowLen::Bytes(1_000_000_000),
            flow_rate: None,
            flow_weight: None,
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
    #[default(60000)]
    #[arg(skip)]
    pub handshake_timeout_ms: u64,
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

impl LocalConfig {
    /// Converts IP address to node ID, supporting both TUN and user space networks.
    pub fn ip_to_node_id(&self, ip: Ipv4Addr) -> NodeId {
        let ip_addr = u32::from(ip);
        let netmask = u32::from(self.local_netmask);

        let tun_base = u32::from(self.virtual_base_addr);
        let user_space_base = u32::from(self.user_space_base_addr);
        let external_base = u32::from(self.external_base_addr);

        match ip_addr & netmask {
            subnet if subnet == (tun_base & netmask) => (ip_addr - tun_base) as NodeId,
            subnet if subnet == (user_space_base & netmask) => {
                (ip_addr - user_space_base) as NodeId
            }
            // binds the external client/server address to the node ID
            subnet if subnet == (external_base & netmask) => (ip_addr - external_base) as NodeId,
            _ => {
                panic!("Detected unknown IP {}.", ip);
            }
        }
    }

    /// Creates a new instance of LocalConfig.
    pub fn new() -> LocalConfig {
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

        // overrides controller_addr from command line if provided
        if let Some(addr) = args.controller_addr {
            cfgs.controller_addr = addr;
        }

        // remembers which config file produced this configuration so namespace children can reuse it
        cfgs.config_path = args.config_path.clone();

        // sets the private ipv4 address of the network interface for the private network
        // Defined by RFC 1918, private IP addresses fall within the following ranges:
        // 10.0.0.0 - 10.255.255.255 (10.0.0.0/8)
        // 172.16.0.0 - 172.31.255.255 (172.16.0.0/12)
        // 192.168.0.0 - 192.168.255.255 (192.168.0.0/16)
        if cfgs.private_network_addr.is_empty() {
            let itf_name = cfgs.private_network_interface.clone();

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
            cfgs.private_network_addr = ipv4addr;

            // computes the node_id from private_network_addr using external_base_addr
            if let Ok(real_ip) = cfgs.private_network_addr.parse::<Ipv4Addr>() {
                let ip = u32::from(real_ip);
                // external_base_addr is used to compute the node_id
                // as it has the same prefix with the private_network_addr
                let base = u32::from(cfgs.external_base_addr);
                let computed_node_id = (ip - base) as NodeId;
                if computed_node_id != 0 && cfgs.node_id == 0 {
                    cfgs.node_id = computed_node_id;
                    info!(
                        "From real IP {} using external_base_addr, node_id is: {}.",
                        cfgs.private_network_addr, cfgs.node_id
                    );
                } else if cfgs.node_id != 0 {
                    info!(
                        "Using configured node_id: {}, ignoring computed node_id: {} from IP {}.",
                        cfgs.node_id, computed_node_id, cfgs.private_network_addr
                    );
                }
            } else if !cfgs.private_network_addr.is_empty() {
                error!(
                    "Failed to parse private_network_addr as Ipv4Addr: {}.",
                    cfgs.private_network_addr
                );
            }
        }

        // sets the ipv4 address of the network interface for the public network
        if cfgs.public_network_addr.is_empty() {
            let itf_name = cfgs.public_network_interface.clone();

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

            cfgs.public_network_addr = ipv4addr;
        }

        // sets the number of packet processors to the number of threads if it is 0
        if cfgs.num_packet_processors == 0 {
            cfgs.num_packet_processors = num_cpus::get();
        }

        cfgs
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

    /// initialize the config for the namespace nodes
    #[allow(unused)]
    pub fn new_for_namespace(
        config_path: &str,
        controller_addr: &str,
        ns_addr: &str,
    ) -> LocalConfig {
        // Reads the TOML configuration file (or falls back to defaults).
        let mut cfgs = match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str::<<LocalConfig as ClapSerde>::Opt>(&content) {
                Ok(opts) => LocalConfig::from(opts),
                Err(e) => {
                    info!("Failed to parse config file (with error: {e}), using default values.");
                    LocalConfig::from(<LocalConfig as ClapSerde>::Opt::default())
                }
            },
            Err(e) => {
                info!(
                    "Failed to read the config file '{config_path}' (with error: {e}), using default values."
                );
                LocalConfig::from(<LocalConfig as ClapSerde>::Opt::default())
            }
        };

        // manually sets namespace node's ip addresses
        cfgs.private_network_addr = ns_addr.to_string();
        cfgs.public_network_addr = ns_addr.to_string();

        // calculates the node_id
        if let Ok(real_ip) = ns_addr.parse::<Ipv4Addr>() {
            let ip = u32::from(real_ip);
            let base = u32::from(cfgs.external_base_addr);
            let computed_node_id = (ip - base) as NodeId;
            cfgs.node_id = computed_node_id;
        } else {
            error!("Failed to parse ns_addr '{ns_addr}' as IPv4");
        }

        // Placeholder – caller (e.g. isoserver) should overwrite this.
        cfgs.controller_addr = controller_addr.to_string();

        if cfgs.num_packet_processors == 0 {
            cfgs.num_packet_processors = num_cpus::get();
        }

        cfgs.config_path = config_path.to_string();

        cfgs
    }
}
