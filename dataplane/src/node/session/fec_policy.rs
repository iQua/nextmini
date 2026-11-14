//! Sender-side validation and mode selection for optional session FEC.

use std::fmt::{Display, Formatter};

use nextmini_messages::lossless_session::{
    LosslessSessionFecMode, LosslessSessionMode, MAX_MANIFEST_TREE_IDS,
};

use crate::node::config::{LosslessConfig, LosslessFecScheme};

/// Errors reported before a sender session is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    InvalidBlockSize { value: usize },
    BlockSizeTooLarge { value: usize },
    ZeroSymbolsPerBlock,
    MissingTreeIds,
    TreeIdsMustBeSortedUnique { tree_ids: Vec<u16> },
    TooManyTreeIds { configured: usize, max: usize },
    InvalidMettleCodedRate { numerator: u32, denominator: u32 },
}

/// Runtime-derived sender policy after local validation succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SenderPolicy {
    pub mode: LosslessSessionMode,
}

/// Validate that a configured block size fits in the wire manifest.
pub(super) fn validate_block_size(block_size: usize) -> Result<u32, PreflightError> {
    if block_size == 0 {
        return Err(PreflightError::InvalidBlockSize { value: block_size });
    }

    u32::try_from(block_size).map_err(|_| PreflightError::BlockSizeTooLarge { value: block_size })
}

/// Derive the sender's transfer mode from the local runtime configuration.
pub(super) fn derive_sender_policy(
    runtime_config: &LosslessConfig,
) -> Result<SenderPolicy, PreflightError> {
    if !runtime_config.fec_enabled {
        return Ok(SenderPolicy {
            mode: LosslessSessionMode::Plain,
        });
    }

    let symbols_per_block = runtime_config.fec_default_symbols_per_block;
    if symbols_per_block == 0 {
        return Err(PreflightError::ZeroSymbolsPerBlock);
    }

    let tree_ids = derive_sender_tree_ids(runtime_config)?;

    let mode = match runtime_config.fec_default_scheme {
        LosslessFecScheme::RaptorQ => LosslessSessionMode::Fec(
            LosslessSessionFecMode::new_raptorq(symbols_per_block, tree_ids),
        ),
        LosslessFecScheme::Mettle => {
            let numerator = runtime_config.mettle_default_coded_rate_num;
            let denominator = runtime_config.mettle_default_coded_rate_den;
            if denominator == 0 || numerator < denominator {
                return Err(PreflightError::InvalidMettleCodedRate {
                    numerator,
                    denominator,
                });
            }
            LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle_with_coded_rate(
                symbols_per_block,
                tree_ids,
                numerator,
                denominator,
            ))
        }
    };

    Ok(SenderPolicy { mode })
}

/// Resolve and validate the tree set used for FEC symbol striping.
pub(super) fn derive_sender_tree_ids(
    runtime_config: &LosslessConfig,
) -> Result<Vec<u16>, PreflightError> {
    let requested_tree_ids = runtime_config.fec_default_tree_ids.clone();

    if requested_tree_ids.is_empty() {
        return Err(PreflightError::MissingTreeIds);
    }
    if !requested_tree_ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(PreflightError::TreeIdsMustBeSortedUnique {
            tree_ids: requested_tree_ids,
        });
    }

    let tree_count = requested_tree_ids.len();
    if tree_count > MAX_MANIFEST_TREE_IDS {
        return Err(PreflightError::TooManyTreeIds {
            configured: tree_count,
            max: MAX_MANIFEST_TREE_IDS,
        });
    }

    Ok(requested_tree_ids)
}

impl Display for PreflightError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBlockSize { value } => {
                write!(f, "block_size must be >= 1 (got {value})")
            }
            Self::BlockSizeTooLarge { value } => {
                write!(f, "block_size {value} exceeds u32 wire capacity")
            }
            Self::ZeroSymbolsPerBlock => {
                write!(f, "symbols_per_block must be >= 1 for fec mode")
            }
            Self::MissingTreeIds => {
                write!(
                    f,
                    "fec_default_tree_ids must be non-empty for fec sender sessions"
                )
            }
            Self::TreeIdsMustBeSortedUnique { tree_ids } => write!(
                f,
                "fec tree_ids must be sorted ascending and unique (got {tree_ids:?})"
            ),
            Self::TooManyTreeIds { configured, max } => write!(
                f,
                "configured fec tree_ids length {configured} exceeds wire manifest capacity {max}"
            ),
            Self::InvalidMettleCodedRate {
                numerator,
                denominator,
            } => write!(
                f,
                "mettle_default_coded_rate_num/den must describe a rate >= 1 with non-zero denominator (got {numerator}/{denominator})"
            ),
        }
    }
}
