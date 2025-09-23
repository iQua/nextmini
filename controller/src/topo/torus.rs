// Initiates and builds the network topology.
use petgraph::graph::{NodeIndex, UnGraph};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::topo::topo::Result;
use crate::topo::topo::{TopologyBuilder, TopologyError};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct TorusConfig {
    pub dim: usize,
    pub n: usize,
}

impl TorusConfig {
    fn validate_params(dimension: u32, nodes_per_dim: u32) -> Result<()> {
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

    fn build_edges(dimension: u32, nodes_per_dim: u32) -> Result<Vec<(u32, u32)>> {
        // Create empty undirected graph
        let mut graph = UnGraph::<(), ()>::default();

        // Add all nodes first
        let total_nodes = nodes_per_dim.pow(dimension);
        for _ in 0..total_nodes {
            graph.add_node(());
        }

        // Add edges based on dimension
        match dimension {
            1 => Self::build_1d_torus_edges(&mut graph, nodes_per_dim),
            2 => Self::build_2d_torus_edges(&mut graph, nodes_per_dim),
            3 => Self::build_3d_torus_edges(&mut graph, nodes_per_dim),
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
                    let next_i =
                        (i + 1) % nodes_per_dim + j * nodes_per_dim + k * nodes_per_dim.pow(2);
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
}

impl TopologyBuilder for TorusConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        let dimension = self.dim as u32;
        let nodes_per_dim = self.n as u32;

        Self::validate_params(dimension, nodes_per_dim)?;

        let total_nodes = Self::calculate_total_nodes(dimension, nodes_per_dim)?;
        info!(
            "Building {}D Torus topology with {} nodes.",
            dimension, total_nodes
        );

        let edges = Self::build_edges(dimension, nodes_per_dim)?;

        Ok(edges)
    }
}
