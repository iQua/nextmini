/// Defines configuration structs and loading logic.
use std::fs;
use std::net::Ipv4Addr;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{error, info};

use nextmini_messages::{Flow, NodeSpec, Protocol, SchedulingDiscipline};

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
    FullMesh,
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

    #[serde(default)]
    pub edges: Option<Vec<(u32, u32)>>,
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

    /// A vector of directed acyclic graphs.
    #[serde(default)]
    pub routes: Vec<DAG>,

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
    use petgraph::Direction;
    use petgraph::graph::{DiGraph, NodeIndex};
    use petgraph::graphmap::DiGraphMap;

    #[test]
    fn test_graph_parsing_and_digraph_build() {
        // Test parsing of graphs (DAGs) and building a DiGraph using petgraph
        let toml_content = r#"
        protocol = "tcp"

        [[routes]]
        edges = [[7,2],[2, 5], [2, 3], [3, 4], [4, 5]]
        "#;

        let config: Config = toml::from_str(toml_content).expect("Failed to parse TOML");
        let edges = config.routes[0].edges.clone();

        // Verify edges of the first graph
        assert_eq!(edges, vec![(7, 2), (2, 5), (2, 3), (3, 4), (4, 5)]);

        // Build a DiGraphMap from the edges
        let route = DiGraphMap::<u32, ()>::from_edges(&edges);

        // Verify the number of nodes and edges in the DiGraphMap
        assert_eq!(route.node_count(), 5);
        assert_eq!(route.edge_count(), 5);

        // Verify the neighbors of node 2
        let neighbors = route.neighbors(2).collect::<Vec<u32>>();
        assert_eq!(neighbors, vec![5, 3]);

        // Verify dst node
        let dst_node_id = route
            .nodes()
            .find(|id| route.neighbors_directed(*id, Direction::Outgoing).count() == 0)
            .unwrap();
        assert_eq!(dst_node_id, 5);

        // Finds neighbors with DiGraph approach
        let graph = DiGraph::<u32, ()>::from_edges(&edges);
        let current_node = 2;
        let neighbors = graph
            .neighbors(current_node.into())
            .map(|idx| NodeIndex::index(idx))
            .collect::<Vec<_>>();
        assert_eq!(neighbors, vec![3, 5]);

        // IMPORTANT: petgraph will always start from 0 even if this node is not in the edges
        let nodes = graph
            .node_indices()
            .map(|idx| NodeIndex::index(idx))
            .collect::<Vec<_>>();
        assert_ne!(nodes, vec![2, 3, 4, 5, 7]);
    }
}
