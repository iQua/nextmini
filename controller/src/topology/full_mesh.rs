use serde::{Deserialize, Serialize};
use tracing::info;

use crate::topology::topo::Result;
use crate::topology::topo::TopologyBuilder;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FullMeshConfig {
    pub n_nodes: usize,
}

impl TopologyBuilder for FullMeshConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        info!("Building a full mesh topology with {} nodes.", self.n_nodes);
        let n_nodes = self.n_nodes as u32;
        let mut preset_edges = Vec::new();

        for src in 1..=n_nodes {
            for dst in (src + 1)..=n_nodes {
                preset_edges.push((src, dst));
            }
        }

        Ok(preset_edges)
    }
}
