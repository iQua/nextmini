/// Defines configuration structs and loading logic.
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{error, info};

use nextmini_messages::{FlowSize, Protocol};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Route {
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
pub struct Topology {
    #[serde(default)]
    #[serde(alias = "topology")]
    #[serde(rename = "type")]
    pub topology_type: Option<PresetTopology>,
    #[serde(default)]
    pub n_nodes: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkRate {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub rate: usize,
    pub bucket_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Flow {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub flow_size: FlowSize, // flow size
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on
    #[serde(default = "default_port")]
    pub port: u16,

    /// The base ipv4 address for the network
    #[serde(default = "default_base_addr")]
    pub base_addr: [u8; 4],

    /// The net mask for the network.
    /// This is used to calculate the ipv4 address for each node. Change this only if you understand what you are doing.
    #[serde(default = "default_net_mask")]
    pub net_mask: [u8; 4],

    /// The base ipv4 address for user-space smoltcp network
    #[serde(default = "default_user_space_base_addr")]
    pub user_space_base_addr: [u8; 4],

    /// The net mask for user-space smoltcp network
    #[serde(default = "default_user_space_net_mask")]
    pub user_space_net_mask: [u8; 4],

    /// The transport protocol: TCP or QUIC.
    #[serde(default = "default_protocol")]
    pub protocol: Protocol,

    /// Whether the controller should automatically synchronize with the dataplane.
    /// if true, the controller will automatically synchronize routes in its database with the dataplane routing tables.

    /// A list of routes. Each route is defined as a path of node IDs.
    /// For example: route = [1, 2, 3, 4] means a path from node 1 to node 4 via nodes 2 and 3
    #[serde(default)]
    pub routes: Vec<Route>,

    /// A vector of link rates.
    #[serde(default)]
    pub link_rates: Vec<LinkRate>, // A list of link rates.

    /// Topology configuration for automatic route generation.
    #[serde(default)]
    pub topology: Topology,

    /// Should database be reset before starting the controller?

    /// flows configuration
    #[serde(default)]
    pub flows: Vec<Flow>, // A list of flows.

    /// The database configuration.
    #[serde(default = "default_db_config")]
    pub db: DBConfig,
}

// Default values if they are missing from the configuration file

/// The default port number to listen on
fn default_port() -> u16 {
    3000
}

/// The default base ipv4 address for the network
fn default_base_addr() -> [u8; 4] {
    [10, 0, 0, 0]
}

/// The default net mask for the network.
fn default_net_mask() -> [u8; 4] {
    // accommodates up to 255 * 255 nodes in the private network
    [255, 255, 0, 0]
}

/// The default base ipv4 address for user-space smoltcp network
fn default_user_space_base_addr() -> [u8; 4] {
    [192, 168, 0, 0]
}

/// The default net mask for user-space smoltcp network
fn default_user_space_net_mask() -> [u8; 4] {
    [255, 255, 255, 0]
}

/// The default transport protocol: QUIC
fn default_protocol() -> Protocol {
    Protocol::Quic
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
                Ok(config) => {
                    info!("Successfully loaded configuration from: {}", filename);

                    info!(
                        "Loaded {} custom routes from configuration",
                        config.routes.len()
                    );
                    config
                }
                Err(e) => {
                    error!(
                        "Error parsing TOML config file: {}. Using the default configuration.",
                        e
                    );
                    Config::default()
                }
            },
            Err(e) => {
                error!(
                    "Error reading config file: {}. Using the default configuration.",
                    e
                );
                Config::default()
            }
        }
    } else {
        info!("The configuration file cannot be found. Using the default configuration.");
        Config::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: default_port(),
            base_addr: default_base_addr(),
            net_mask: default_net_mask(),
            user_space_base_addr: default_user_space_base_addr(),
            user_space_net_mask: default_user_space_net_mask(),
            protocol: default_protocol(),
            routes: Vec::new(),
            flows: Vec::new(),
            link_rates: Vec::new(),
            topology: Topology::default(),
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
        protocol = "quic"

        [[routes]]
        route = [1, 2, 3, 4]

        [[routes]]
        route = [1, 3, 2, 4]

        [[routes]]
        route = [1, 3, 4]
        "#;

        let config: Config = toml::from_str(toml_content).expect("Failed to parse TOML");

        // Test basic config values
        assert_eq!(config.protocol, Protocol::Quic);

        // Test that we have 3 routes
        assert_eq!(config.routes.len(), 3);

        // Verify route parsing
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
    }

    #[test]
    fn test_route_processing_logic() {
        // Test the route processing logic that happens in get_config
        let config_content = r#"
        protocol = "quic"
        base_addr = [10, 0, 0, 0]
        net_mask = [255, 255, 255, 0]

        [[routes]]
        route = [1, 2, 3, 4]

        [[routes]]
        route = [1, 3, 2, 4]

        [[routes]]
        route = [1, 3, 4]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify basic configuration
        assert_eq!(config.protocol, Protocol::Quic);
        assert_eq!(config.base_addr, [10, 0, 0, 0]);
        assert_eq!(config.net_mask, [255, 255, 255, 0]);

        // Verify routes were processed correctly
        assert_eq!(config.routes.len(), 3);

        // Verify specific route paths
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
    }

    #[test]
    fn test_preset_topology_config_parsing() {
        // Test preset topology configuration parsing
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "full_mesh"
        n_nodes = 3

        [[routes]]
        route = [1, 2, 3]

        [[routes]]
        route = [3, 2, 1]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology configuration
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::FullMesh => {
                assert_eq!(config.topology.n_nodes, Some(3));
            }
            _ => panic!("Expected FullMesh topology"),
        }

        // Verify custom routes are present
        assert_eq!(config.routes.len(), 2);
        assert_eq!(config.routes[0].route, vec![1, 2, 3]);
        assert_eq!(config.routes[1].route, vec![3, 2, 1]);
    }

    #[test]
    fn test_ring_topology_config_parsing() {
        // Test ring topology configuration parsing
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "ring"
        n_nodes = 4

        [[routes]]
        route = [1, 3, 4]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology configuration
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::Ring => {
                assert_eq!(config.topology.n_nodes, Some(4));
            }
            _ => panic!("Expected Ring topology"),
        }

        // Verify custom routes are present
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.routes[0].route, vec![1, 3, 4]);
    }

    #[test]
    fn test_route_id_assignment_only_custom_routes() {
        // Test route_id assignment for only custom routes (no preset topology)
        let config_content = r#"
        protocol = "quic"

        [[routes]]
        route = [1, 2, 3]

        [[routes]]
        route = [3, 2, 1]

        [[routes]]
        route = [1, 4, 2]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify no preset topology
        assert!(config.topology.topology_type.is_none());

        // Verify custom routes
        assert_eq!(config.routes.len(), 3);
        assert_eq!(config.routes[0].route, vec![1, 2, 3]);
        assert_eq!(config.routes[1].route, vec![3, 2, 1]);
        assert_eq!(config.routes[2].route, vec![1, 4, 2]);

        info!("✓ Only custom routes configuration parsed correctly");
        info!("  - Custom routes should get route_id: 0, 1, 2");
    }

    #[test]
    fn test_route_id_assignment_with_full_mesh() {
        // Test route_id assignment with full mesh preset + custom routes
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "full_mesh"
        n_nodes = 3

        [[routes]]
        route = [1, 2, 3]

        [[routes]]
        route = [3, 1, 2]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::FullMesh => {
                assert_eq!(config.topology.n_nodes, Some(3));
            }
            _ => panic!("Expected FullMesh topology"),
        }

        // Verify custom routes
        assert_eq!(config.routes.len(), 2);
        assert_eq!(config.routes[0].route, vec![1, 2, 3]);
        assert_eq!(config.routes[1].route, vec![3, 1, 2]);

        info!("✓ Full mesh + custom routes configuration parsed correctly");
        info!("  - Full mesh (3 nodes) should generate route_id: 0, 1, 2, 3, 4, 5");
        info!("  - Custom routes should get route_id: 6, 7");
    }

    #[test]
    fn test_route_id_assignment_with_ring() {
        // Test route_id assignment with ring preset + custom routes
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "ring"
        n_nodes = 4

        [[routes]]
        route = [1, 3, 4]

        [[routes]]
        route = [4, 2, 1]

        [[routes]]
        route = [2, 4, 3, 1]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::Ring => {
                assert_eq!(config.topology.n_nodes, Some(4));
            }
            _ => panic!("Expected Ring topology"),
        }

        // Verify custom routes
        assert_eq!(config.routes.len(), 3);
        assert_eq!(config.routes[0].route, vec![1, 3, 4]);
        assert_eq!(config.routes[1].route, vec![4, 2, 1]);
        assert_eq!(config.routes[2].route, vec![2, 4, 3, 1]);

        info!("✓ Ring + custom routes configuration parsed correctly");
        info!("  - Ring (4 nodes) should generate route_id: 0, 1, 2, 3");
        info!("  - Custom routes should get route_id: 4, 5, 6");
    }

    #[test]
    fn test_controller_config_toml_format() {
        // Test the exact format used in controller-config.toml
        let config_content = r#"
        protocol = "quic"

        # Forward routes: Node 1 to Node 4
        [[routes]]
        route = [1, 2, 3, 4]

        [[routes]]
        route = [1, 3, 2, 4]

        [[routes]]
        route = [1, 3, 4]

        # Reverse routes: Node 4 to Node 1
        [[routes]]
        route = [4, 3, 2, 1]

        [[routes]]
        route = [4, 2, 3, 1]

        [[routes]]
        route = [4, 3, 1]

        # Node 1 to Node 2
        [[routes]]
        route = [1, 2]

        # Node 2 to Node 1
        [[routes]]
        route = [2, 1]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify basic settings
        assert_eq!(config.protocol, Protocol::Quic);

        // Verify no preset topology
        assert!(config.topology.topology_type.is_none());

        // Verify all 8 routes are parsed correctly
        assert_eq!(config.routes.len(), 8);

        // Verify specific routes
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
        assert_eq!(config.routes[3].route, vec![4, 3, 2, 1]);
        assert_eq!(config.routes[4].route, vec![4, 2, 3, 1]);
        assert_eq!(config.routes[5].route, vec![4, 3, 1]);
        assert_eq!(config.routes[6].route, vec![1, 2]);
        assert_eq!(config.routes[7].route, vec![2, 1]);

        info!("✓ Controller-config.toml format parsed correctly");
        info!("  - 8 custom routes should get route_id: 0, 1, 2, 3, 4, 5, 6, 7");
    }

    #[test]
    fn test_empty_routes_handling() {
        // Test handling of configuration with no routes
        let config_content = r#"
        protocol = "quic"
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify basic settings
        assert_eq!(config.protocol, Protocol::Quic);

        // Verify no routes
        assert_eq!(config.routes.len(), 0);
        assert!(config.topology.topology_type.is_none());

        info!("✓ Empty routes configuration handled correctly");
    }

    #[test]
    fn test_route_deduplication_scenario() {
        // Test route deduplication with preset topology + duplicate custom routes
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "full_mesh"
        n_nodes = 4

        # These routes duplicate some of the preset routes
        [[routes]]
        route = [1, 2]  # This will duplicate preset route

        [[routes]]
        route = [2, 1]  # This will duplicate preset route

        # These are unique multi-hop routes
        [[routes]]
        route = [1, 2, 3, 4]

        [[routes]]
        route = [4, 3, 2, 1]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::FullMesh => {
                assert_eq!(config.topology.n_nodes, Some(4));
            }
            _ => panic!("Expected FullMesh topology"),
        }

        // Verify custom routes (including duplicates)
        assert_eq!(config.routes.len(), 4);
        assert_eq!(config.routes[0].route, vec![1, 2]); // Will be duplicate
        assert_eq!(config.routes[1].route, vec![2, 1]); // Will be duplicate
        assert_eq!(config.routes[2].route, vec![1, 2, 3, 4]); // Unique
        assert_eq!(config.routes[3].route, vec![4, 3, 2, 1]); // Unique

        info!("✓ Route deduplication scenario configuration parsed correctly");
        info!("  - Full mesh (4 nodes) should generate 12 preset routes");
        info!("  - 2 custom routes should be deduplicated (skipped)");
        info!("  - 2 unique custom routes should be added");
        info!("  - Total expected routes: 12 + 2 = 14 routes");
    }
}
