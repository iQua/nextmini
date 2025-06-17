use tokio_tungstenite::tungstenite::{Error, Message};

use clap_serde_derive::ClapSerde;
use clap_serde_derive::clap;
use clap_serde_derive::clap::Parser;
use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
use serde::Deserialize;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, Protocol};

use crate::node::NodeId;
use crate::node::drop::DropStrategy;
use crate::node::scheduler::SchedulingDiscipline;

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
    pub controller_addr: String,

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
    /// The server address
    #[default("".to_string())]
    #[arg(skip)]
    pub controller_addr: String,

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

    #[default(0)]
    #[arg(long)]
    pub node_id: NodeId,

    /// This is not used in metrics collector
    /// The interval at which metrics are collected and sent to the controller
    #[default(5)]
    #[arg(long)]
    pub metrics_collection_interval: u64,

    /// The number of local TUN queues to use to send and receive packets.
    #[default(1)]
    #[arg(long)]
    pub num_tun_queues: usize,

    /// The number of packet processors to use to process packets. Recommended to use a larger number on a machine
    /// with multiple cores for greater throughput. Use the number of CPU cores if set to 0.
    #[default(0)]
    #[arg(long)]
    pub num_packet_processors: usize,

    /// The capacity for all channels between actors
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

    /// The name of the local TUN interface
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

    // The local network address
    #[default(10, 0, 0, 1)]
    #[arg(skip)]
    pub local_address: (u8, u8, u8, u8),

    // The local network mask
    #[default(255, 255, 255, 0)]
    #[arg(skip)]
    pub local_netmask: (u8, u8, u8, u8),

    // The transport protocol: TCP or QUIC
    #[default(Protocol::Tcp)]
    #[arg(long, value_enum)]
    pub protocol: Protocol,

    // The scheduling discipline
    #[default(SchedulingDiscipline::Fifo)]
    #[arg(long, value_enum)]
    pub scheduler_type: SchedulingDiscipline,

    // The capacity of each scheduler queue
    #[default(1000)]
    #[arg(long)]
    pub queue_capacity: usize,

    // The drop strategy for the scheduler
    #[default(DropStrategy::TailDrop)]
    #[arg(long, value_enum)]
    pub scheduler_drop_strategy: DropStrategy,

    // The processing mode for processing packets
    #[default(Feature::Sequential)]
    #[arg(long, value_enum)]
    pub feature: Feature,

    // Reorder tolerance for the multipath mode
    #[default(4)]
    #[arg(long)]
    pub reorder_tolerance: usize,

    // The user-space smoltcp ip address from controller
    #[default(192, 168, 0, 1)]
    #[arg(skip)]
    pub user_space_smoltcp_ip: (u8, u8, u8, u8),

    // The user-space smoltcp network mask from controller
    #[default(255, 255, 255, 0)]
    #[arg(skip)]
    pub user_space_smoltcp_netmask: (u8, u8, u8, u8),

    // The user-space smoltcp port from controller (used as client port)
    #[default(49152)]
    #[arg(skip)]
    pub smoltcp_client_port: u16,

    // Fixed server port for smoltcp server (from controller)
    #[default(8888)]
    #[arg(skip)]
    pub smoltcp_server_port: u16,
}

impl LocalConfig {
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

        cfgs.controller_addr = args.controller_addr;

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

            cfgs.private_network_addr = ipv4addr;
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
                        addr,
                        net_mask,
                        smoltcp_addr,
                        smoltcp_net_mask,
                        smoltcp_port,
                        smoltcp_server_port,
                        protocol,
                    } => {
                        self.node_id = node_id;
                        self.local_address = (addr[0], addr[1], addr[2], addr[3]);
                        self.local_netmask = (net_mask[0], net_mask[1], net_mask[2], net_mask[3]);
                        // update smoltcp ip and netmask
                        self.user_space_smoltcp_ip = (
                            smoltcp_addr[0],
                            smoltcp_addr[1],
                            smoltcp_addr[2],
                            smoltcp_addr[3],
                        );
                        self.user_space_smoltcp_netmask = (
                            smoltcp_net_mask[0],
                            smoltcp_net_mask[1],
                            smoltcp_net_mask[2],
                            smoltcp_net_mask[3],
                        );
                        self.smoltcp_client_port = smoltcp_port;
                        self.smoltcp_server_port = smoltcp_server_port;
                        self.protocol = protocol;
                        self.scheduler_type = SchedulingDiscipline::Fifo;
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
