use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

use crate::node::config::{Feature, FecTreeIdsSource, LosslessConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    RuntimeChannelClosed,
    CapabilityRequirementDisabled,
    CollaborativeMultiTreeDisabled,
    InvalidTreeLaneDepth { value: usize },
    InvalidDispatchBurst { value: usize },
    InvalidMaxTreeLanes { value: usize },
    InstalledRoutesTreeIdsUnsupported,
    ChunkSizeCannotDeriveDefaultSymbolSize { chunk_size: usize },
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
) -> Result<SenderFecPolicy, PreflightError> {
    let tree_lane_depth = runtime_config.fec_tree_lane_depth;
    let dispatch_burst = runtime_config.fec_dispatch_burst;
    let manifest = derive_sender_manifest(runtime_config, chunk_size)?;

    if manifest.is_none() {
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

    let tree_ids = derive_sender_tree_ids(runtime_config)?;

    Ok(SenderFecPolicy {
        manifest,
        tree_ids,
        tree_lane_depth,
        dispatch_burst,
    })
}

pub(super) fn derive_receiver_capabilities(runtime_config: &LosslessConfig) -> FecCapabilities {
    if runtime_config.fec_enabled {
        FecCapabilities::default()
    } else {
        FecCapabilities::empty()
    }
}

fn derive_sender_manifest(
    runtime_config: &LosslessConfig,
    chunk_size: usize,
) -> Result<Option<FecManifest>, PreflightError> {
    if !runtime_config.fec_enabled {
        return Ok(None);
    }
    if !runtime_config.fec_require_capability {
        return Err(PreflightError::CapabilityRequirementDisabled);
    }

    let symbols_per_block = runtime_config.canonical_fec_default_symbols_per_block();
    let symbol_size = runtime_config
        .canonical_fec_default_symbol_size(chunk_size)
        .ok_or(PreflightError::ChunkSizeCannotDeriveDefaultSymbolSize { chunk_size })?;
    let manifest = FecManifest::new_raptorq(symbols_per_block, symbol_size);

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

impl std::fmt::Display for PreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeChannelClosed => write!(
                f,
                "lossless runtime channel closed before sender start could complete"
            ),
            Self::CapabilityRequirementDisabled => write!(
                f,
                "fec_require_capability=false is unsupported with strict no-fallback sessions"
            ),
            Self::InstalledRoutesTreeIdsUnsupported => write!(
                f,
                "fec_tree_ids_source=installed_routes is not implemented; set fec_tree_ids_source=config with fec_default_tree_ids"
            ),
            Self::ChunkSizeCannotDeriveDefaultSymbolSize { chunk_size } => write!(
                f,
                "chunk_size {chunk_size} cannot derive default fec symbol_size under current fec_symbol_size_policy"
            ),
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
    use nextmini_messages::lossless_session::FecCapabilities;

    use super::{PreflightError, derive_receiver_capabilities, derive_sender_policy};
    use crate::node::config::{Feature, FecSymbolSizePolicy, FecTreeIdsSource, LosslessConfig};

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
    fn sender_policy_disables_fec_when_runtime_disables_it() {
        let runtime = LosslessConfig::default();
        let policy = derive_sender_policy(&runtime, 1200)
            .expect("non-fec policy should be derived when runtime kill-switch is off");

        assert_eq!(policy.manifest, None);
        assert!(policy.tree_ids.is_empty());
    }

    #[test]
    fn sender_policy_rejects_chunk_size_larger_than_symbol_size() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_symbol_size_policy: FecSymbolSizePolicy::Fixed,
            fec_default_symbol_size: 1500,
            fec_symbol_size_min: 1,
            fec_symbol_size_max: 4096,
            fec_default_tree_ids: vec![0],
            ..Default::default()
        };
        let err = derive_sender_policy(&runtime, 1501)
            .expect_err("chunk_size must remain <= symbol_size");

        assert_eq!(
            err,
            PreflightError::ChunkSizeExceedsSymbolSize {
                chunk_size: 1501,
                symbol_size: 1500
            }
        );
    }

    #[test]
    fn sender_policy_rejects_when_capability_requirement_disabled() {
        let runtime = LosslessConfig {
            fec_require_capability: false,
            ..enabled_runtime()
        };
        let err = derive_sender_policy(&runtime, 1200).expect_err(
            "strict runtime-owned FEC negotiation should reject disabled capability requirements",
        );

        assert_eq!(err, PreflightError::CapabilityRequirementDisabled);
    }

    #[test]
    fn sender_policy_rejects_chunk_size_that_cannot_derive_default_symbol_size() {
        let runtime = enabled_runtime();
        let chunk_size = usize::from(u16::MAX) + 1;
        let err = derive_sender_policy(&runtime, chunk_size)
            .expect_err("chunk-size-based symbol policy should reject chunk sizes larger than u16");

        assert_eq!(
            err,
            PreflightError::ChunkSizeCannotDeriveDefaultSymbolSize { chunk_size }
        );
    }

    #[test]
    fn sender_policy_canonicalizes_runtime_defaults_for_manifest_and_tree_ids() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_default_symbols_per_block: 1,
            fec_symbols_per_block_min: 8,
            fec_symbols_per_block_max: 64,
            fec_symbol_size_policy: FecSymbolSizePolicy::Fixed,
            fec_default_symbol_size: 4096,
            fec_symbol_size_min: 1200,
            fec_symbol_size_max: 1500,
            fec_default_tree_ids: vec![3, 1, 3],
            ingress_feature: Feature::Sequential,
            ingress_channel_backpressure: true,
            ..Default::default()
        };
        let policy = derive_sender_policy(&runtime, 1200).expect(
            "policy should use canonical runtime defaults when caller provides no FEC data",
        );
        let manifest = policy
            .manifest
            .expect("FEC runtime should derive a manifest from canonical defaults");

        assert_eq!(manifest.symbols_per_block, 8);
        assert_eq!(manifest.symbol_size, 1500);
        assert_eq!(policy.tree_ids, vec![1, 3]);
    }

    #[test]
    fn sender_policy_derives_manifest_and_tree_ids_from_runtime_defaults() {
        let runtime = enabled_runtime();
        let policy = derive_sender_policy(&runtime, 1200)
            .expect("runtime should derive sender manifest/tree defaults");

        let manifest = policy
            .manifest
            .expect("fec should be enabled in test runtime");
        assert_eq!(manifest.symbols_per_block, 32);
        assert_eq!(manifest.symbol_size, 1200);
        assert_eq!(policy.tree_ids, vec![0]);
    }

    #[test]
    fn sender_policy_rejects_missing_config_tree_ids_for_fec_sessions() {
        let runtime = LosslessConfig {
            fec_default_tree_ids: vec![],
            ..enabled_runtime()
        };
        let err = derive_sender_policy(&runtime, 1200).expect_err("fec sessions require tree IDs");

        assert_eq!(err, PreflightError::MissingTreeIds);
    }

    #[test]
    fn sender_policy_rejects_installed_routes_tree_source_until_implemented() {
        let runtime = LosslessConfig {
            fec_tree_ids_source: FecTreeIdsSource::InstalledRoutes,
            ..enabled_runtime()
        };
        let err = derive_sender_policy(&runtime, 1200)
            .expect_err("installed_routes source should fail deterministically until implemented");

        assert_eq!(err, PreflightError::InstalledRoutesTreeIdsUnsupported);
    }

    #[test]
    fn sender_policy_rejects_multi_tree_on_concurrent_ingress() {
        let runtime = LosslessConfig {
            fec_default_tree_ids: vec![1, 3],
            ingress_feature: Feature::Concurrent,
            ..enabled_runtime()
        };
        let err = derive_sender_policy(&runtime, 1200)
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
        let runtime = LosslessConfig {
            fec_enabled: false,
            ..enabled_runtime()
        };
        let policy = derive_sender_policy(&runtime, 1200)
            .expect("non-fec sessions should pass with empty tree set");

        assert_eq!(policy.manifest, None);
        assert!(policy.tree_ids.is_empty());
        assert_eq!(policy.tree_lane_depth, runtime.fec_tree_lane_depth);
        assert_eq!(policy.dispatch_burst, runtime.fec_dispatch_burst);
    }

    #[test]
    fn receiver_policy_disables_capabilities_when_fec_runtime_disabled() {
        let runtime = LosslessConfig::default();
        let derived = derive_receiver_capabilities(&runtime);
        assert_eq!(derived, FecCapabilities::empty());
    }

    #[test]
    fn receiver_policy_advertises_default_capabilities_when_enabled() {
        let runtime = enabled_runtime();
        let derived = derive_receiver_capabilities(&runtime);
        assert_eq!(derived, FecCapabilities::default());
    }
}
