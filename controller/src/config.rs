/// Defines configuration structs and loading logic.
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use nextmini-messages::Protocol;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Route {
    #[serde(default)]
    pub route_id: usize,
    #[serde(default)]
    pub src_node_id: usize,
    #[serde(default)]
    pub dst_node_id: usize,
    pub route: Vec<usize>,
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
    ///     route_id: 0,        // The route ID (unique identifier)
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
        host: "172.16.8.2".to_string(),
        database: "nextmini".to_string(),
        port: "5432".to_string(),
    }
}

pub fn get_config(filename: &str) -> Config {
    if Path::new(filename).exists() {
        match fs::read_to_string(filename) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(mut config) => {
                    println!("Successfully loaded configuration from: {}", filename);

                    for (index, route) in config.routes.iter_mut().enumerate() {
                        // Only auto-assign route_id if it's not manually specified (default value 0)
                        if route.route_id == 0 {
                            route.route_id = index;
                            println!(
                                "Auto-assigned route_id {} to route at index {}",
                                route.route_id, index
                            );
                        } else {
                            println!(
                                "Using manually specified route_id {} for route at index {}",
                                route.route_id, index
                            );
                        }

                        // If src_node_id is not set and route is not empty, use first element of route
                        if route.src_node_id == 0 && !route.route.is_empty() {
                            route.src_node_id = route.route[0];
                            println!(
                                "Auto-inferring src_node_id as {} from the route array for route_id {}",
                                route.src_node_id, route.route_id
                            );
                        }

                        // If dst_node_id is not set and route is not empty, use last element of route
                        if route.dst_node_id == 0 && !route.route.is_empty() {
                            route.dst_node_id = route.route[route.route.len() - 1];
                            println!(
                                "Auto-inferring dst_node_id as {} from the route array for route_id {}",
                                route.dst_node_id, route.route_id
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
            reset_db: false,
            db: default_db_config(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_config_parsing() {
        let toml_content = r#"
reset_db = true
protocol = "quic"

[[routes]]
route = [1, 2, 3, 4]

[[routes]]
route = [1, 3, 2, 4]

[[routes]]
route = [1, 3, 4]
"#;

        let mut config: Config = toml::from_str(toml_content).expect("Failed to parse TOML");

        // Test basic config values
        assert_eq!(config.reset_db, true);
        assert_eq!(config.protocol, Protocol::Quic);

        // Test that we have 3 routes
        assert_eq!(config.routes.len(), 3);

        // Simulate route processing logic from get_config
        for (index, route) in config.routes.iter_mut().enumerate() {
            route.route_id = index;

            // Auto-infer src_node_id and dst_node_id like in get_config
            if route.src_node_id == 0 && !route.route.is_empty() {
                route.src_node_id = route.route[0];
            }
            if route.dst_node_id == 0 && !route.route.is_empty() {
                route.dst_node_id = route.route[route.route.len() - 1];
            }
        }

        // Verify route 0: [1, 2, 3, 4]
        assert_eq!(config.routes[0].route_id, 0);
        assert_eq!(config.routes[0].src_node_id, 1);
        assert_eq!(config.routes[0].dst_node_id, 4);
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);

        // Verify route 1: [1, 3, 2, 4]
        assert_eq!(config.routes[1].route_id, 1);
        assert_eq!(config.routes[1].src_node_id, 1);
        assert_eq!(config.routes[1].dst_node_id, 4);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);

        // Verify route 2: [1, 3, 4]
        assert_eq!(config.routes[2].route_id, 2);
        assert_eq!(config.routes[2].src_node_id, 1);
        assert_eq!(config.routes[2].dst_node_id, 4);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
    }

    #[test]
    fn test_route_processing_logic() {
        // Test the route processing logic that happens in get_config
        let config_content = r#"
reset_db = true
protocol = "quic"
base_ipv4_addr = [10, 0, 0, 0]
ipv4_net_mask = [255, 255, 255, 0]

[[routes]]
route = [1, 2, 3, 4]

[[routes]]
route = [1, 3, 2, 4]

[[routes]]
route = [1, 3, 4]
"#;

        let mut config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Simulate the route processing logic from get_config
        for (index, route) in config.routes.iter_mut().enumerate() {
            route.route_id = index;

            // Auto-infer src_node_id and dst_node_id like in get_config
            if route.src_node_id == 0 && !route.route.is_empty() {
                route.src_node_id = route.route[0];
            }
            if route.dst_node_id == 0 && !route.route.is_empty() {
                route.dst_node_id = route.route[route.route.len() - 1];
            }
        }

        // Verify basic configuration
        assert_eq!(config.reset_db, true);
        assert_eq!(config.protocol, Protocol::Quic);
        assert_eq!(config.base_ipv4_addr, [10, 0, 0, 0]);
        assert_eq!(config.ipv4_net_mask, [255, 255, 255, 0]);

        // Verify routes were processed correctly
        assert_eq!(config.routes.len(), 3);

        // Verify route processing (route_id assignment and src/dst inference)
        for (index, route) in config.routes.iter().enumerate() {
            assert_eq!(route.route_id, index);
            assert_eq!(route.src_node_id, 1); // Should be auto-inferred from first element
            assert_eq!(route.dst_node_id, 4); // Should be auto-inferred from last element
        }

        // Verify specific route paths
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
    }
}
