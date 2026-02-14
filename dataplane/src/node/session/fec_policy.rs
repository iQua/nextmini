use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

use crate::node::config::{Feature, LosslessConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    RuntimeChannelClosed,
    DisabledByConfig,
    CapabilityRequirementDisabled,
    UnknownScheme { scheme: u8 },
    CollaborativeMultiTreeDisabled,
    InvalidTreeLaneDepth { value: usize },
    InvalidDispatchBurst { value: usize },
    InvalidMaxTreeLanes { value: usize },
    TreeIdsRequireFec,
    MissingTreeIds,
    TreeIdsMustBeSortedUnique { tree_ids: Vec<u16> },
    TooManyTreeIds { configured: usize, max: usize },
    MultiTreeRequiresSequentialIngress { feature: Feature },
    MultiTreeRequiresIngressBackpressure,
    SymbolsPerBlockOutOfBounds { value: u16, min: u16, max: u16 },
    SymbolSizeOutOfBounds { value: u16, min: u16, max: u16 },
    ChunkSizeExceedsSymbolSize { chunk_size: usize, symbol_size: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SenderFecPolicy {
    pub manifest: Option<FecManifest>,
    pub tree_ids: Vec<u16>,
    pub tree_lane_depth: usize,
    pub dispatch_burst: usize,
}

pub(super) fn derive_sender_policy(
    runtime_config: &LosslessConfig,
    chunk_size: usize,
    requested_manifest: Option<FecManifest>,
    requested_tree_ids: &[u16],
) -> Result<SenderFecPolicy, PreflightError> {
    let tree_lane_depth = runtime_config.fec_tree_lane_depth;
    let dispatch_burst = runtime_config.fec_dispatch_burst;
    let manifest = derive_sender_manifest(runtime_config, chunk_size, requested_manifest)?;

    if manifest.is_none() {
        if !requested_tree_ids.is_empty() {
            return Err(PreflightError::TreeIdsRequireFec);
        }
        return Ok(SenderFecPolicy {
            manifest: None,
            tree_ids: Vec::new(),
            tree_lane_depth,
            dispatch_burst,
        });
    }

    if tree_lane_depth == 0 {
        return Err(PreflightError::InvalidTreeLaneDepth {
            value: tree_lane_depth,
        });
    }
    if dispatch_burst == 0 {
        return Err(PreflightError::InvalidDispatchBurst {
            value: dispatch_burst,
        });
    }
    if runtime_config.fec_max_tree_lanes == 0 {
        return Err(PreflightError::InvalidMaxTreeLanes {
            value: runtime_config.fec_max_tree_lanes,
        });
    }

    let tree_ids = derive_sender_tree_ids(runtime_config, requested_tree_ids)?;

    Ok(SenderFecPolicy {
        manifest,
        tree_ids,
        tree_lane_depth,
        dispatch_burst,
    })
}

pub(super) fn derive_receiver_capabilities(
    runtime_config: &LosslessConfig,
    requested: FecCapabilities,
) -> FecCapabilities {
    if runtime_config.fec_enabled {
        requested
    } else {
        FecCapabilities::empty()
    }
}

fn derive_sender_manifest(
    runtime_config: &LosslessConfig,
    chunk_size: usize,
    requested_manifest: Option<FecManifest>,
) -> Result<Option<FecManifest>, PreflightError> {
    let Some(manifest) = requested_manifest else {
        return Ok(None);
    };

    if !runtime_config.fec_enabled {
        return Err(PreflightError::DisabledByConfig);
    }
    if !runtime_config.fec_require_capability {
        return Err(PreflightError::CapabilityRequirementDisabled);
    }
    if manifest.scheme_kind().is_none() {
        return Err(PreflightError::UnknownScheme {
            scheme: manifest.scheme,
        });
    }

    let (symbols_min, symbols_max) = runtime_config.fec_symbols_per_block_bounds();
    if manifest.symbols_per_block < symbols_min || manifest.symbols_per_block > symbols_max {
        return Err(PreflightError::SymbolsPerBlockOutOfBounds {
            value: manifest.symbols_per_block,
            min: symbols_min,
            max: symbols_max,
        });
    }

    let (size_min, size_max) = runtime_config.fec_symbol_size_bounds();
    if manifest.symbol_size < size_min || manifest.symbol_size > size_max {
        return Err(PreflightError::SymbolSizeOutOfBounds {
            value: manifest.symbol_size,
            min: size_min,
            max: size_max,
        });
    }

    if chunk_size > usize::from(manifest.symbol_size) {
        return Err(PreflightError::ChunkSizeExceedsSymbolSize {
            chunk_size,
            symbol_size: manifest.symbol_size,
        });
    }

    Ok(Some(manifest))
}

fn derive_sender_tree_ids(
    runtime_config: &LosslessConfig,
    requested_tree_ids: &[u16],
) -> Result<Vec<u16>, PreflightError> {
    if requested_tree_ids.is_empty() {
        return Err(PreflightError::MissingTreeIds);
    }
    if !requested_tree_ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(PreflightError::TreeIdsMustBeSortedUnique {
            tree_ids: requested_tree_ids.to_vec(),
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

    Ok(requested_tree_ids.to_vec())
}

fn feature_mode_label(feature: &Feature) -> &'static str {
    match feature {
        Feature::Sequential => "sequential",
        Feature::Concurrent => "concurrent",
    }
}

impl std::fmt::Display for PreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeChannelClosed => write!(
                f,
                "lossless runtime channel closed before sender start could complete"
            ),
            Self::DisabledByConfig => write!(f, "fec is disabled by local runtime configuration"),
            Self::CapabilityRequirementDisabled => write!(
                f,
                "fec_require_capability=false is unsupported with strict no-fallback sessions"
            ),
            Self::UnknownScheme { scheme } => write!(f, "unknown fec scheme {scheme} requested"),
            Self::CollaborativeMultiTreeDisabled => write!(
                f,
                "collaborative multi-tree fec is disabled by local runtime configuration"
            ),
            Self::InvalidTreeLaneDepth { value } => {
                write!(f, "fec_tree_lane_depth must be >= 1 (got {value})")
            }
            Self::InvalidDispatchBurst { value } => {
                write!(f, "fec_dispatch_burst must be >= 1 (got {value})")
            }
            Self::InvalidMaxTreeLanes { value } => {
                write!(f, "fec_max_tree_lanes must be >= 1 (got {value})")
            }
            Self::TreeIdsRequireFec => write!(
                f,
                "fec_tree_ids requires an active fec manifest for sender sessions"
            ),
            Self::MissingTreeIds => {
                write!(f, "fec_tree_ids must be non-empty for fec sender sessions")
            }
            Self::TreeIdsMustBeSortedUnique { tree_ids } => write!(
                f,
                "fec_tree_ids must be sorted ascending and unique (got {tree_ids:?})"
            ),
            Self::TooManyTreeIds { configured, max } => write!(
                f,
                "configured fec_tree_ids length {configured} exceeds fec_max_tree_lanes {max}"
            ),
            Self::MultiTreeRequiresSequentialIngress { feature } => {
                write!(
                    f,
                    "collaborative multi-tree fec requires sequential ingress (feature={})",
                    feature_mode_label(feature)
                )
            }
            Self::MultiTreeRequiresIngressBackpressure => write!(
                f,
                "collaborative multi-tree fec requires channel_backpressure=true to avoid ingress drops"
            ),
            Self::SymbolsPerBlockOutOfBounds { value, min, max } => write!(
                f,
                "fec symbols_per_block {value} out of bounds [{min}, {max}]"
            ),
            Self::SymbolSizeOutOfBounds { value, min, max } => {
                write!(f, "fec symbol_size {value} out of bounds [{min}, {max}]")
            }
            Self::ChunkSizeExceedsSymbolSize {
                chunk_size,
                symbol_size,
            } => write!(
                f,
                "chunk_size {chunk_size} exceeds fec symbol_size {symbol_size}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

    use super::{PreflightError, derive_receiver_capabilities, derive_sender_policy};
    use crate::node::config::{Feature, LosslessConfig};

    fn enabled_runtime() -> LosslessConfig {
        LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_symbols_per_block_min: 8,
            fec_symbols_per_block_max: 64,
            fec_symbol_size_min: 1200,
            fec_symbol_size_max: 2000,
            ..Default::default()
        }
    }

    #[test]
    fn sender_policy_rejects_fec_when_runtime_disables_it() {
        let runtime = LosslessConfig::default();
        let err = derive_sender_policy(
            &runtime,
            1200,
            Some(FecManifest::new_raptorq(16, 1400)),
            &[0],
        )
        .expect_err("fec should be rejected when runtime kill-switch is off");

        assert_eq!(err, PreflightError::DisabledByConfig);
    }

    #[test]
    fn sender_policy_rejects_chunk_size_larger_than_symbol_size() {
        let runtime = enabled_runtime();
        let err = derive_sender_policy(
            &runtime,
            1501,
            Some(FecManifest::new_raptorq(16, 1500)),
            &[0],
        )
        .expect_err("chunk_size must remain <= derived symbol_size");

        assert_eq!(
            err,
            PreflightError::ChunkSizeExceedsSymbolSize {
                chunk_size: 1501,
                symbol_size: 1500
            }
        );
    }

    #[test]
    fn sender_policy_rejects_tree_ids_without_fec_manifest() {
        let runtime = enabled_runtime();
        let err =
            derive_sender_policy(&runtime, 1200, None, &[1]).expect_err("tree IDs require FEC");

        assert_eq!(err, PreflightError::TreeIdsRequireFec);
    }

    #[test]
    fn sender_policy_rejects_unsorted_or_duplicate_tree_ids() {
        let runtime = enabled_runtime();
        let err = derive_sender_policy(
            &runtime,
            1200,
            Some(FecManifest::new_raptorq(16, 1400)),
            &[3, 1, 1],
        )
        .expect_err("tree IDs must be strictly ascending and duplicate free");

        assert_eq!(
            err,
            PreflightError::TreeIdsMustBeSortedUnique {
                tree_ids: vec![3, 1, 1]
            }
        );
    }

    #[test]
    fn sender_policy_rejects_multi_tree_on_concurrent_ingress() {
        let runtime = LosslessConfig {
            ingress_feature: Feature::Concurrent,
            ..enabled_runtime()
        };
        let err = derive_sender_policy(
            &runtime,
            1200,
            Some(FecManifest::new_raptorq(16, 1400)),
            &[1, 3],
        )
        .expect_err("multi-tree requires sequential ingress feature");

        assert_eq!(
            err,
            PreflightError::MultiTreeRequiresSequentialIngress {
                feature: Feature::Concurrent
            }
        );
    }

    #[test]
    fn sender_policy_accepts_non_fec_sessions_without_tree_ids() {
        let runtime = enabled_runtime();
        let policy = derive_sender_policy(&runtime, 1200, None, &[])
            .expect("non-fec sessions should pass with empty tree set");

        assert_eq!(policy.manifest, None);
        assert!(policy.tree_ids.is_empty());
        assert_eq!(policy.tree_lane_depth, runtime.fec_tree_lane_depth);
        assert_eq!(policy.dispatch_burst, runtime.fec_dispatch_burst);
    }

    #[test]
    fn receiver_policy_disables_capabilities_when_fec_runtime_disabled() {
        let runtime = LosslessConfig::default();
        let derived = derive_receiver_capabilities(&runtime, FecCapabilities::default());
        assert_eq!(derived, FecCapabilities::empty());
    }

    #[test]
    fn receiver_policy_preserves_requested_capabilities_when_enabled() {
        let runtime = enabled_runtime();
        let requested = FecCapabilities::default();
        let derived = derive_receiver_capabilities(&runtime, requested);
        assert_eq!(derived, requested);
    }
}
