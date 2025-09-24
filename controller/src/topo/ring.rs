use serde::{Deserialize, Serialize};
use tracing::info;

use crate::topo::topo::Result;
use crate::topo::topo::TopologyBuilder;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct RingConfig {
    pub n_nodes: usize,
}

impl TopologyBuilder for RingConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        let mut edges = Vec::new();
        for i in 1..=self.n_nodes {
            let next = if i == self.n_nodes {
                1u32
            } else {
                (i + 1) as u32
            };
            edges.push((i as u32, next));
        }

        info!(
            "Building Ring topology with {} nodes and {} edges.",
            self.n_nodes,
            edges.len()
        );
        Ok(edges)
    }
}
