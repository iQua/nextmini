// Initiates and builds the Torus network topology.
use std::collections::HashSet;

use thiserror::Error;
use tracing::info;

use crate::config;
use crate::config::PresetTopology;

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

// Builds all topology edges from the configuration file, including both the preset topology and custom edges.
pub fn build_topology(config: &config::Config) -> Option<Vec<(u32, u32)>> {
    // for deduplication of preset topology and custom edges
    let mut edge_set: HashSet<(u32, u32)> = HashSet::new();

    // adds preset topology edges
    if let Some(preset) = &config.topology.topology_type {
        let preset_edges = match preset {
            PresetTopology::FullMesh => config.topology.full_mesh_config.as_ref()?.build().ok(),
            PresetTopology::FatTree => config.topology.fat_tree_config.as_ref()?.build().ok(),
            PresetTopology::Torus => config.topology.torus_config.as_ref()?.build().ok(),
        };

        // This could be redundant if the preset topology doesn't need to be normalized.
        if let Some(preset_edges) = preset_edges {
            for (a, b) in preset_edges {
                edge_set.insert(if a <= b { (a, b) } else { (b, a) });
            }
        }
    }

    // adds custom topology edges from the [topology] section in the configuration file
    if let Some(custom_edges) = &config.topology.edges {
        for &(a, b) in custom_edges {
            edge_set.insert(if a <= b { (a, b) } else { (b, a) });
        }
    }

    if edge_set.is_empty() {
        None
    } else {
        info!("Total number of edges in the topology: {}", edge_set.len());
        Some(edge_set.into_iter().collect())
    }
}
