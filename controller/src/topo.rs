use tracing::info;

use petgraph::graph::{NodeIndex, UnGraph};
use thiserror::Error;

use crate::config;
use crate::config::{FatTreeConfig, PresetTopology, TorusConfig};

#[derive(Error, Debug)]
pub enum TopologyError {
    #[error("Failed to read configuration file: {0}")]
    ConfigReadError(#[from] std::io::Error),

    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),

    #[error("Invalid topology configuration: {0}")]
    InvalidConfig(String),

    #[error("Unsupported torus dimension: {0}")]
    UnsupportedDimension(u32),

    #[error("Numeric overflow in calculation: {0}")]
    NumericOverflow(String),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

pub trait TopologyBuilder {
    fn build(&self) -> Result<Vec<(u32, u32)>>;
}

impl TopologyBuilder for FatTreeConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        let k = u32::try_from(self.k)
            .map_err(|_| TopologyError::NumericOverflow("FatTree k parameter overflow".into()))?;

        validate_fattree_params(k)?;
        info!("Building a FatTree topology with k = {}.", k);

        let (num_layer_switches, layer_switches_per_pod, core_switches_per_agg) =
            calculate_fattree_params(k)?;

        let edges = build_fattree_edges(
            num_layer_switches,
            layer_switches_per_pod,
            core_switches_per_agg,
        );

        Ok(edges)
    }
}

impl TopologyBuilder for TorusConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        let dimension = self.dim as u32;
        let nodes_per_dim = self.n as u32;

        validate_torus_params(dimension, nodes_per_dim)?;

        let total_nodes = calculate_total_nodes(dimension, nodes_per_dim)?;
        info!(
            "Building {}D Torus topology with {} nodes.",
            dimension, total_nodes
        );

        let edges = build_torus_edges(dimension, nodes_per_dim)?;

        Ok(edges)
    }
}

fn validate_fattree_params(k: u32) -> Result<()> {
    if k % 2 != 0 {
        return Err(TopologyError::InvalidConfig("k must be even".into()));
    }
    if k == 0 {
        return Err(TopologyError::InvalidConfig("k must be positive".into()));
    }
    Ok(())
}

fn calculate_fattree_params(k: u32) -> Result<(u32, u32, u32)> {
    let num_layer_switches = k.pow(2) / 2;
    let layer_switches_per_pod = k / 2;
    let num_core_switches = k.pow(2) / 4;
    let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

    Ok((
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    ))
}

fn build_fattree_edges(
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) -> Vec<(u32, u32)> {
    let mut edges = Vec::with_capacity((num_layer_switches * layer_switches_per_pod * 2) as usize);

    // Edge to aggregation layer connections
    build_edge_to_aggregation_connections(&mut edges, num_layer_switches, layer_switches_per_pod);

    // Aggregation to core layer connections
    build_aggregation_to_core_connections(
        &mut edges,
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    );

    edges
}

fn build_edge_to_aggregation_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
) {
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;
        edges.extend(
            (agg_start..agg_start + layer_switches_per_pod).map(|agg_id| (edge_id + 1, agg_id + 1)),
        );
    }
}

fn build_aggregation_to_core_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) {
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;
        edges.extend(
            (core_start..core_start + core_switches_per_agg)
                .map(|core_id| (agg_id + 1, core_id + 1)),
        );
    }
}

fn validate_torus_params(dimension: u32, nodes_per_dim: u32) -> Result<()> {
    if !(1..=3).contains(&dimension) {
        return Err(TopologyError::UnsupportedDimension(dimension));
    }
    if nodes_per_dim == 0 {
        return Err(TopologyError::InvalidConfig(
            "nodes_per_dim must be positive".into(),
        ));
    }
    Ok(())
}

fn calculate_total_nodes(dimension: u32, nodes_per_dim: u32) -> Result<usize> {
    usize::try_from(nodes_per_dim.pow(dimension))
        .map_err(|_| TopologyError::NumericOverflow("Total node count overflow".into()))
}

fn build_torus_edges(dimension: u32, nodes_per_dim: u32) -> Result<Vec<(u32, u32)>> {
    // Create empty undirected graph
    let mut graph = UnGraph::<(), ()>::default();

    // Add all nodes first
    let total_nodes = nodes_per_dim.pow(dimension);
    for _ in 0..total_nodes {
        graph.add_node(());
    }

    // Add edges based on dimension
    match dimension {
        1 => build_1d_torus_edges(&mut graph, nodes_per_dim),
        2 => build_2d_torus_edges(&mut graph, nodes_per_dim),
        3 => build_3d_torus_edges(&mut graph, nodes_per_dim),
        _ => return Err(TopologyError::UnsupportedDimension(dimension)),
    }

    // Extract edges from graph
    let edges: Vec<(u32, u32)> = graph
        .edge_indices()
        .map(|e| {
            let (a, b) = graph.edge_endpoints(e).unwrap();
            (a.index() as u32 + 1, b.index() as u32 + 1)
        })
        .collect();

    Ok(edges)
}

fn build_1d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        let next = (i + 1) % nodes_per_dim;
        let i_idx = NodeIndex::new(i as usize);
        let next_idx = NodeIndex::new(next as usize);

        // Add single edge - UnGraph handles bidirectional nature
        graph.add_edge(i_idx, next_idx, ());
    }
}

fn build_2d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            let current = i + j * nodes_per_dim;
            let current_idx = NodeIndex::new(current as usize);

            // X dimension connection
            let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim;
            let next_i_idx = NodeIndex::new(next_i as usize);
            graph.add_edge(current_idx, next_i_idx, ());

            // Y dimension connection
            let next_j = i + ((j + 1) % nodes_per_dim) * nodes_per_dim;
            let next_j_idx = NodeIndex::new(next_j as usize);
            graph.add_edge(current_idx, next_j_idx, ());
        }
    }
}

fn build_3d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            for k in 0..nodes_per_dim {
                let current = i + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let current_idx = NodeIndex::new(current as usize);

                // X dimension
                let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_i_idx = NodeIndex::new(next_i as usize);
                graph.add_edge(current_idx, next_i_idx, ());

                // Y dimension
                let next_j =
                    i + ((j + 1) % nodes_per_dim) * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_j_idx = NodeIndex::new(next_j as usize);
                graph.add_edge(current_idx, next_j_idx, ());

                // Z dimension
                let next_k =
                    i + j * nodes_per_dim + ((k + 1) % nodes_per_dim) * nodes_per_dim.pow(2);
                let next_k_idx = NodeIndex::new(next_k as usize);
                graph.add_edge(current_idx, next_k_idx, ());
            }
        }
    }
}

// builds preset topology edges from config
pub fn build_topology_edges_from_config(config: &config::Config) -> Option<Vec<(u32, u32)>> {
    if let Some(preset) = &config.topology.topology_type {
        match preset {
            PresetTopology::FullMesh => {
                let n = config.topology.n_nodes? as u32;
                let mut edges = Vec::new();
                for src in 1..=n {
                    for dst in (src + 1)..=n {
                        edges.push((src, dst));
                    }
                }
                Some(edges)
            }
            PresetTopology::FatTree => config.topology.fat_tree_config.as_ref()?.build().ok(),
            PresetTopology::Torus => config.topology.torus_config.as_ref()?.build().ok(),
        }
    } else {
        config.topology.edges.clone()
    }
}
