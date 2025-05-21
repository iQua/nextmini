/// Defines configuration structs and loading logic.
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use strato_messages::{MultiPathMethod, Protocol};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Route {
    #[serde(default)]
    pub src_node_id: usize,
    #[serde(default)]
    pub dst_node_id: usize,
    pub route_id: usize,
    pub hops: Vec<usize>,
    pub streams: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DBConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub database: String,
    pub port: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PresetTopology {
    FullMesh,
    Ring,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RoutePreset {
    #[serde(default)]
    #[serde(alias = "type")]
    pub preset_topology: Option<PresetTopology>,
    #[serde(default)]
    pub n_nodes: Option<usize>,
    #[serde(default)]
    pub route_ids: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Topology {
    #[serde(default)]
    pub connect: Option<Vec<(usize, usize)>>,
    #[serde(default)]
    pub disconnect: Option<Vec<(usize, usize)>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkRate {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub bandwidth: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// The session ID as a vector of four bytes
    #[serde(default = "default_session_id")]
    pub session_id: [u8; 4],

    /// The port to listen on
    #[serde(default = "default_port")]
    pub port: u16,

    /// The base ipv4 address for the network
    #[serde(default = "default_base_ipv4_addr")]
    pub base_ipv4_addr: [u8; 4],

    /// The net mask for the network.
    /// This is used to calculate the ipv4 address for each node. Change this only if you understand what you are doing.
    #[serde(default = "default_ipv4_net_mask")]
    pub ipv4_net_mask: [u8; 4],

    /// The transport protocol: TCP, UDP, or QUIC.
    #[serde(default = "default_protocol")]
    pub protocol: Protocol,

    /// Whether the controller should automatically synchronize with the dataplane.
    /// if true, the controller will automatically synchronize routes in its database with the dataplane routing tables.
    /// if false, synchronization will take place manually by sending a sync route trigger through the Postgres database
    #[serde(default = "default_true")]
    pub auto_db_sync: bool,

    /// A list of routes. For example:
    ///     src_node_id: 0,     // The source node ID
    ///     dst_node_id: 1,     // The destination node ID
    ///     hops: [0, 2, 3, 1]  // The paths for the route
    #[serde(default)]
    pub routes: Vec<Route>,

    /// A vector of link rates.
    #[serde(default)]
    pub link_rates: Vec<LinkRate>, // A list of link rates.

    /// A dictionary of routes presets. Overriden by routes on overlap.
    #[serde(default)]
    pub routes_preset: RoutePreset,

    /// An option to manually configure the connections.
    #[serde(default)]
    pub topology: Topology,

    /// The total number of paths to use for a flow (with the Interface multi-path method).
    #[serde(default = "default_interfaces")]
    pub num_interfaces: usize,

    /// The multi-path method: Interface or Stream.
    /// The Interface mode creates multiple network interfaces, each bound to a path (same as MPTCP), on every dataplane
    /// node.
    /// The Stream mode creates one interface, and binds each path to a TCP stream.
    #[serde(default = "default_multi_path_method")]
    pub multi_path_method: MultiPathMethod,

    /// Should database be reset before starting the controller?
    /// Warning: If this is set to true, all data will be deleted when restarting the controller.
    #[serde(default)]
    pub reset_db: bool,

    /// The database configuration.
    #[serde(default = "default_db_config")]
    pub db: DBConfig,
}

// Default values if they are missing from the configuration file

/// The default session ID, as a vector of four bytes
fn default_session_id() -> [u8; 4] {
    [1, 2, 3, 4]
}

/// The default port number to listen on
fn default_port() -> u16 {
    3000
}

/// The default base ipv4 address for the network
fn default_base_ipv4_addr() -> [u8; 4] {
    [10, 0, 0, 0]
}

/// The default net mask for the network.
fn default_ipv4_net_mask() -> [u8; 4] {
    [255, 255, 255, 0]
}

/// The default transport protocol: QUIC
fn default_protocol() -> Protocol {
    Protocol::Quic
}

/// The default multi-path method: Stream
fn default_multi_path_method() -> MultiPathMethod {
    MultiPathMethod::Stream
}

fn default_true() -> bool {
    true
}

/// The default total number of paths to use for a flow.
fn default_interfaces() -> usize {
    1
}

/// The default configuration for the database
fn default_db_config() -> DBConfig {
    DBConfig {
        user: "pgusr".to_string(),
        password: "pgpwrd".to_string(),
        host: "127.0.0.1".to_string(),
        database: "strato".to_string(),
        port: "5432".to_string(),
    }
}

pub fn get_config(filename: &str) -> Config {
    if Path::new(filename).exists() {
        match fs::read_to_string(filename) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(mut config) => {
                    println!("Successfully loaded configuration from: {}", filename);

                    // Post-process routes to ensure src_node_id and dst_node_id are set
                    for route in &mut config.routes {
                        // If src_node_id is not set and hops is not empty, use first element of hops
                        if route.src_node_id == 0 && !route.hops.is_empty() {
                            route.src_node_id = route.hops[0];
                            println!(
                                "Auto-inferring src_node_id as {} from the hops array",
                                route.src_node_id
                            );
                        }

                        // If dst_node_id is not set and hops is not empty, use last element of hops
                        if route.dst_node_id == 0 && !route.hops.is_empty() {
                            route.dst_node_id = route.hops[route.hops.len() - 1];
                            println!(
                                "Auto-inferring dst_node_id as {} from the hops array",
                                route.dst_node_id
                            );
                        }
                    }
                    config
                }
                Err(e) => {
                    println!(
                        "Error parsing TOML config file: {}. Using the default configuration.",
                        e
                    );
                    Config::default()
                }
            },
            Err(e) => {
                println!(
                    "Error reading config file: {}. Using the default configuration.",
                    e
                );
                Config::default()
            }
        }
    } else {
        println!("The configuration file cannot be found. Using the default configuration.");
        Config::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            session_id: default_session_id(),
            port: default_port(),
            base_ipv4_addr: default_base_ipv4_addr(),
            ipv4_net_mask: default_ipv4_net_mask(),
            protocol: default_protocol(),
            auto_db_sync: default_true(),
            routes: Vec::new(),
            link_rates: Vec::new(),
            routes_preset: RoutePreset::default(),
            topology: Topology::default(),
            num_interfaces: default_interfaces(),
            multi_path_method: default_multi_path_method(),
            reset_db: false,
            db: default_db_config(),
        }
    }
}
