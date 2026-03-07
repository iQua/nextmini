use std::fmt::{Display, Formatter};

use nextmini_messages::lossless_session::{LosslessSessionFecMode, LosslessSessionMode};

use crate::node::config::{Feature, FecTreeIdsSource, LosslessConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    RuntimeChannelClosed,
    InvalidBlockSize { value: usize },
    BlockSizeTooLarge { value: usize },
    InvalidMaxTreeLanes { value: usize },
    ZeroSymbolsPerBlock,
    InstalledRoutesTreeIdsUnsupported,
    MissingTreeIds,
    TreeIdsMustBeSortedUnique { tree_ids: Vec<u16> },
    TooManyTreeIds { configured: usize, max: usize },
    CollaborativeMultiTreeDisabled,
    MultiTreeRequiresSequentialIngress { feature: Feature },
    MultiTreeRequiresIngressBackpressure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SenderPolicy {
    pub mode: LosslessSessionMode,
}

pub(super) fn validate_block_size(block_size: usize) -> Result<u32, PreflightError> {
    if block_size == 0 {
        return Err(PreflightError::InvalidBlockSize { value: block_size });
    }

    u32::try_from(block_size).map_err(|_| PreflightError::BlockSizeTooLarge { value: block_size })
}

pub(super) fn derive_sender_policy(
    runtime_config: &LosslessConfig,
) -> Result<SenderPolicy, PreflightError> {
    if !runtime_config.fec_enabled {
        return Ok(SenderPolicy {
            mode: LosslessSessionMode::Plain,
        });
    }

    if runtime_config.fec_max_tree_lanes == 0 {
        return Err(PreflightError::InvalidMaxTreeLanes {
            value: runtime_config.fec_max_tree_lanes,
        });
    }

    let symbols_per_block = runtime_config.canonical_fec_default_symbols_per_block();
    if symbols_per_block == 0 {
        return Err(PreflightError::ZeroSymbolsPerBlock);
    }

    let tree_ids = derive_sender_tree_ids(runtime_config)?;

    Ok(SenderPolicy {
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(
            symbols_per_block,
            tree_ids,
        )),
    })
}

fn derive_sender_tree_ids(runtime_config: &LosslessConfig) -> Result<Vec<u16>, PreflightError> {
    let requested_tree_ids = match runtime_config.fec_tree_ids_source {
        FecTreeIdsSource::Config => runtime_config.canonical_fec_default_tree_ids(),
        FecTreeIdsSource::InstalledRoutes => {
            return Err(PreflightError::InstalledRoutesTreeIdsUnsupported);
        }
    };

    if requested_tree_ids.is_empty() {
        return Err(PreflightError::MissingTreeIds);
    }
    if !requested_tree_ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(PreflightError::TreeIdsMustBeSortedUnique {
            tree_ids: requested_tree_ids,
        });
    }

    let tree_count = requested_tree_ids.len();
    if tree_count > runtime_config.fec_max_tree_lanes {
        return Err(PreflightError::TooManyTreeIds {
            configured: tree_count,
            max: runtime_config.fec_max_tree_lanes,
        });
    }

    if tree_count > 1 {
        if !runtime_config.fec_collaborative_multitree_enabled {
            return Err(PreflightError::CollaborativeMultiTreeDisabled);
        }
        if runtime_config.ingress_feature != Feature::Sequential {
            return Err(PreflightError::MultiTreeRequiresSequentialIngress {
                feature: runtime_config.ingress_feature.clone(),
            });
        }
        if !runtime_config.ingress_channel_backpressure {
            return Err(PreflightError::MultiTreeRequiresIngressBackpressure);
        }
    }

    Ok(requested_tree_ids)
}

fn feature_mode_label(feature: &Feature) -> &'static str {
    match feature {
        Feature::Sequential => "sequential",
        Feature::Concurrent => "concurrent",
    }
}

impl Display for PreflightError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeChannelClosed => {
                write!(f, "lossless runtime channel closed before session start completed")
            }
            Self::InvalidBlockSize { value } => {
                write!(f, "block_size must be >= 1 (got {value})")
            }
            Self::BlockSizeTooLarge { value } => {
                write!(f, "block_size {value} exceeds u32 wire capacity")
            }
            Self::InvalidMaxTreeLanes { value } => {
                write!(f, "fec_max_tree_lanes must be >= 1 (got {value})")
            }
            Self::ZeroSymbolsPerBlock => {
                write!(f, "symbols_per_block must be >= 1 for fec mode")
            }
            Self::InstalledRoutesTreeIdsUnsupported => write!(
                f,
                "fec_tree_ids_source=installed_routes is not implemented; set fec_tree_ids_source=config with fec_default_tree_ids"
            ),
            Self::MissingTreeIds => {
                write!(f, "fec_default_tree_ids must be non-empty for fec sender sessions")
            }
            Self::TreeIdsMustBeSortedUnique { tree_ids } => write!(
                f,
                "fec tree_ids must be sorted ascending and unique (got {tree_ids:?})"
            ),
            Self::TooManyTreeIds { configured, max } => write!(
                f,
                "configured fec tree_ids length {configured} exceeds fec_max_tree_lanes {max}"
            ),
            Self::CollaborativeMultiTreeDisabled => write!(
                f,
                "collaborative multi-tree fec is disabled by local runtime configuration"
            ),
            Self::MultiTreeRequiresSequentialIngress { feature } => write!(
                f,
                "collaborative multi-tree fec requires sequential ingress (feature={})",
                feature_mode_label(feature)
            ),
            Self::MultiTreeRequiresIngressBackpressure => write!(
                f,
                "collaborative multi-tree fec requires channel_backpressure=true to avoid ingress drops"
            ),
        }
    }
}
