/// Defines configuration structs and loading logic.
use std::fs;
use std::net::Ipv4Addr;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{error, info};

use nextmini_messages::{Flow, NodeSpec, Protocol, SchedulingDiscipline};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Route {
    pub route: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DAG {
    pub edges: Vec<(u32, u32)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
    FatTree,
    Torus,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FatTreeConfig {
    pub k: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct TorusConfig {
    pub dim: usize,
    pub n: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Topology {
    #[serde(default)]
    #[serde(alias = "topology")]
    #[serde(rename = "type")]
    pub topology_type: Option<PresetTopology>,

    #[serde(default)]
    pub n_nodes: Option<usize>,

    #[serde(default)]
    pub fat_tree_config: Option<FatTreeConfig>,

    #[serde(default)]
    pub torus_config: Option<TorusConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkRate {
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub rate: usize,
    pub bucket_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// The port for the persistent TCP/QUIC server operating in normal mode to listen on
    #[serde(default = "default_port")]
    pub port: u16,

    /// The base ipv4 address for the network
    #[serde(default = "default_base_addr")]
    pub base_addr: Ipv4Addr,

    /// The net mask for the network.
    /// This is used to calculate the ipv4 address for each node. Change this only if you understand what you are doing.
    #[serde(default = "default_net_mask")]
    pub net_mask: Ipv4Addr,

    /// The base ipv4 address for user-space smoltcp network
    #[serde(default = "default_user_space_base_addr")]
    pub user_space_base_addr: Ipv4Addr,

    /// The base ipv4 address for external network
    #[serde(default = "default_external_base_addr")]
    pub external_base_addr: Ipv4Addr,

    /// The port for the connection-on-demand TCP server operating in both normal and max mode to listen on
    #[serde(default = "default_max_server_port")]
    pub max_server_port: u16,

    /// The transport protocol: TCP or QUIC.
    #[serde(default = "default_protocol")]
    pub protocol: Protocol,

    /// Whether the controller should automatically synchronize with the dataplane.
    /// if true, the controller will automatically synchronize routes in its database with the dataplane routing tables.

    /// A list of routes. Each route is defined as a path of node IDs.
    /// For example: route = [1, 2, 3, 4] means a path from node 1 to node 4 via nodes 2 and 3
    #[serde(default)]
    pub routes: Vec<Route>,

    /// A vector of directed acyclic graphs.
    #[serde(default)]
    pub graphs: Vec<DAG>,

    /// A vector of link rates.
    #[serde(default)]
    pub link_rates: Vec<LinkRate>, // A list of link rates.

    /// Topology configuration for automatic route generation.
    #[serde(default)]
    pub topology: Topology,

    /// The Flows configuration
    #[serde(default)]
    pub flows: Vec<Flow>, // A list of flows.

    /// The scheduler type
    #[serde(default = "default_scheduler_type")]
    pub scheduler_type: SchedulingDiscipline,

    /// The database configuration.
    #[serde(default = "default_db_config")]
    pub db: DBConfig,

    /// The operating mode.
    #[serde(default)]
    pub nodes: Vec<NodeSpec>,
}

// Default values if they are missing from the configuration file

/// The default port for the TCP server operating in max mode to listen on
fn default_max_server_port() -> u16 {
    8081
}

/// The default port number to listen on
fn default_port() -> u16 {
    3000
}

/// The default base ipv4 address for the network
fn default_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(10, 0, 0, 0)
}

/// The default net mask for the network.
fn default_net_mask() -> Ipv4Addr {
    // accommodates up to 255 * 255 nodes in the private network
    Ipv4Addr::new(255, 255, 0, 0)
}

/// The default base ipv4 address for user-space smoltcp network
fn default_user_space_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(192, 168, 0, 0)
}

/// The default base ipv4 address for external network
fn default_external_base_addr() -> Ipv4Addr {
    Ipv4Addr::new(172, 16, 8, 3)
}

/// The default transport protocol: QUIC
fn default_protocol() -> Protocol {
    Protocol::Quic
}

/// The default scheduler type: FIFO
fn default_scheduler_type() -> SchedulingDiscipline {
    SchedulingDiscipline::Fifo
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
            external_base_addr: default_external_base_addr(),
            max_server_port: default_max_server_port(),
            protocol: default_protocol(),
            routes: Vec::new(),
            graphs: Vec::new(),
            flows: Vec::new(),
            link_rates: Vec::new(),
            topology: Topology::default(),
            scheduler_type: default_scheduler_type(),
            db: default_db_config(),
            nodes: Vec::new(),
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
        assert_eq!(config.base_addr, Ipv4Addr::new(10, 0, 0, 0));
        assert_eq!(config.net_mask, Ipv4Addr::new(255, 255, 255, 0));

        // Verify routes were processed correctly
        assert_eq!(config.routes.len(), 3);

        // Verify specific route paths
        assert_eq!(config.routes[0].route, vec![1, 2, 3, 4]);
        assert_eq!(config.routes[1].route, vec![1, 3, 2, 4]);
        assert_eq!(config.routes[2].route, vec![1, 3, 4]);
    }

    #[test]
    fn test_fat_tree_topology_config_parsing() {
        // Test preset topology configuration parsing
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "fat_tree"
        n_nodes = 3
        fat_tree_config = { k = 2 }
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology configuration
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::FatTree => {
                assert_eq!(config.topology.n_nodes, Some(3));
                assert_eq!(
                    config.topology.fat_tree_config,
                    Some(FatTreeConfig { k: 2 })
                );
            }

            _ => panic!("Expected FatTree topology"),
        }
    }

    #[test]
    fn test_torus_topology_config_parsing() {
        // Test preset topology configuration parsing
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "torus"
        n_nodes = 4
        torus_config = { dim = 2, n = 2 }
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology configuration
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::Torus => {
                assert_eq!(config.topology.n_nodes, Some(4));
                assert_eq!(
                    config.topology.torus_config,
                    Some(TorusConfig { dim: 2, n: 2 })
                );
            }
            _ => panic!("Expected Torus topology"),
        }
    }

    #[test]
    fn test_ring_topology_config_parsing() {
        // Test ring topology configuration parsing
        let config_content = r#"
        protocol = "quic"

        [topology]
        type = "torus"
        n_nodes = 4

        [[routes]]
        route = [1, 3, 4]
        "#;

        let config: Config = toml::from_str(config_content).expect("Failed to parse TOML");

        // Verify preset topology configuration
        assert!(config.topology.topology_type.is_some());
        match config.topology.topology_type.as_ref().unwrap() {
            PresetTopology::Torus => {
                assert_eq!(config.topology.n_nodes, Some(4));
            }
            _ => panic!("Expected Torus topology"),
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
    fn test_graph_parsing_and_digraph_build() {
        // Test parsing of graphs (DAGs) and building a DiGraph using petgraph
        let toml_content = r#"
        protocol = "tcp"

        [[graphs]]
        edges = [[1, 2], [2, 3], [3, 4]]
        "#;

        let config: Config = toml::from_str(toml_content).expect("Failed to parse TOML");

        // Verify edges of the first graph
        assert_eq!(config.graphs[0].edges, vec![(1, 2), (2, 3), (3, 4)]);

        // Build a DiGraph
        use petgraph::graph::DiGraph;
        let dag = DiGraph::<u32, ()>::from_edges(&config.graphs[0].edges);
        println!("dag: {:?}", dag);
        // Verify the number of nodes and edges in the DiGraphs
        // Petagraph starts indexing from 0, so the largest node index + 1 is the number of nodes
        assert_eq!(dag.node_count(), 5);
        assert_eq!(dag.edge_count(), 3);
    }
}
