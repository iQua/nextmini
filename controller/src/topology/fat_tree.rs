// Initiates and builds the FatTree network topology.
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::topology::topo::Result;
use crate::topology::topo::{TopologyBuilder, TopologyError};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FatTreeConfig {
    pub k: usize,
}

impl FatTreeConfig {
    fn validate_params(k: u32) -> Result<()> {
        if !k.is_multiple_of(2) {
            return Err(TopologyError::InvalidConfig("k must be even".into()));
        }

        if k == 0 {
            return Err(TopologyError::InvalidConfig("k must be positive".into()));
        }

        Ok(())
    }

    fn calculate_params(k: u32) -> Result<(u32, u32, u32)> {
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

    fn build_edges(
        num_layer_switches: u32,
        layer_switches_per_pod: u32,
        core_switches_per_agg: u32,
    ) -> Vec<(u32, u32)> {
        let mut edges =
            Vec::with_capacity((num_layer_switches * layer_switches_per_pod * 2) as usize);

        // Edge to aggregation layer connections
        Self::build_edge_to_aggregation_connections(
            &mut edges,
            num_layer_switches,
            layer_switches_per_pod,
        );

        // Aggregation to core layer connections
        Self::build_aggregation_to_core_connections(
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
                (agg_start..agg_start + layer_switches_per_pod)
                    .map(|agg_id| (edge_id + 1, agg_id + 1)),
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
}

impl TopologyBuilder for FatTreeConfig {
    fn build(&self) -> Result<Vec<(u32, u32)>> {
        let k = u32::try_from(self.k)
            .map_err(|_| TopologyError::NumericOverflow("FatTree k parameter overflow".into()))?;

        Self::validate_params(k)?;
        info!("Building a FatTree topology with k = {}.", k);

        let (num_layer_switches, layer_switches_per_pod, core_switches_per_agg) =
            Self::calculate_params(k)?;

        let edges = Self::build_edges(
            num_layer_switches,
            layer_switches_per_pod,
            core_switches_per_agg,
        );

        Ok(edges)
    }
}
