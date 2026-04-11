//! Sender-side validation and mode selection for optional session FEC.

use std::fmt::{Display, Formatter};

use nextmini_messages::lossless_session::{
    LosslessSessionFecMode, LosslessSessionHeader, LosslessSessionMode, MAX_MANIFEST_TREE_IDS,
};

use crate::node::config::LosslessConfig;
use crate::node::packet::MAX_FRAMED_PACKET_SIZE;
use crate::node::session::mettle::params::CodedRate;

const IPV4_HEADER_LEN: usize = 20;
const TCP_BASE_HEADER_LEN: usize = 20;
const LOSSLESS_META_OPTION_LEN: usize = 16;
const METTLE_SYMBOL_FIXED_BODY_LEN: usize = 8 + 4 + 8 + 8 + 2 + 2;

const fn max_mettle_symbol_payload_bytes() -> usize {
    MAX_FRAMED_PACKET_SIZE
        - (IPV4_HEADER_LEN + TCP_BASE_HEADER_LEN + LOSSLESS_META_OPTION_LEN)
        - (LosslessSessionHeader::LEN + METTLE_SYMBOL_FIXED_BODY_LEN)
}

/// Errors reported before a sender session is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    InvalidBlockSize { value: usize },
    BlockSizeTooLarge { value: usize },
    MettleBlockSizeExceedsFramedPacket { value: usize, max_payload: usize },
    InvalidMettleCodedRate { numerator: u16, denominator: u16 },
    ZeroSymbolsPerBlock,
    MissingTreeIds,
    TreeIdsMustBeSortedUnique { tree_ids: Vec<u16> },
    TooManyTreeIds { configured: usize, max: usize },
    TreeScheduleReferencesUnknownTree { tree_ids: Vec<u16> },
    MultiTreeRequiresTreeVisibleIngress,
}

/// Runtime-derived sender policy after local validation succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SenderPolicy {
    pub mode: LosslessSessionMode,
    pub tree_schedule: Vec<u16>,
}

/// Validate that a configured block size fits in the wire manifest.
pub(super) fn validate_block_size(block_size: usize) -> Result<u32, PreflightError> {
    if block_size == 0 {
        return Err(PreflightError::InvalidBlockSize { value: block_size });
    }

    u32::try_from(block_size).map_err(|_| PreflightError::BlockSizeTooLarge { value: block_size })
}

/// Validate any mode-specific packetization constraints that depend on block size.
pub(super) fn validate_mode_block_size(
    mode: &LosslessSessionMode,
    block_size: usize,
) -> Result<(), PreflightError> {
    let LosslessSessionMode::Fec(fec) = mode else {
        return Ok(());
    };
    if fec.scheme_kind() == Some(nextmini_messages::lossless_session::FecScheme::MettleV1) {
        let max_payload = max_mettle_symbol_payload_bytes();
        if block_size > max_payload {
            return Err(PreflightError::MettleBlockSizeExceedsFramedPacket {
                value: block_size,
                max_payload,
            });
        }
    }
    Ok(())
}

/// Derive the sender's transfer mode from the local runtime configuration.
pub(super) fn derive_sender_policy(
    runtime_config: &LosslessConfig,
    session_id: u64,
) -> Result<SenderPolicy, PreflightError> {
    if !runtime_config.fec_enabled {
        return Ok(SenderPolicy {
            mode: LosslessSessionMode::Plain,
            tree_schedule: Vec::new(),
        });
    }

    let tree_ids = derive_sender_tree_ids(runtime_config)?;
    let tree_schedule = derive_sender_tree_schedule(runtime_config, &tree_ids)?;
    if runtime_config.mettle_enabled {
        let rate = CodedRate::new(
            u32::from(runtime_config.mettle_coded_rate_numerator),
            u32::from(runtime_config.mettle_coded_rate_denominator),
        )
        .map_err(|_| PreflightError::InvalidMettleCodedRate {
            numerator: runtime_config.mettle_coded_rate_numerator,
            denominator: runtime_config.mettle_coded_rate_denominator,
        })?;
        let seed = derive_mettle_seed(session_id);
        return Ok(SenderPolicy {
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(
                rate.numerator() as u16,
                rate.denominator() as u16,
                seed,
                tree_ids,
            )),
            tree_schedule,
        });
    }

    let symbols_per_block = runtime_config.fec_default_symbols_per_block;
    if symbols_per_block == 0 {
        return Err(PreflightError::ZeroSymbolsPerBlock);
    }

    Ok(SenderPolicy {
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(
            symbols_per_block,
            tree_ids,
        )),
        tree_schedule,
    })
}

/// Resolve and validate the tree set used for FEC symbol striping.
fn derive_sender_tree_ids(runtime_config: &LosslessConfig) -> Result<Vec<u16>, PreflightError> {
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

fn derive_sender_tree_schedule(
    runtime_config: &LosslessConfig,
    tree_ids: &[u16],
) -> Result<Vec<u16>, PreflightError> {
    if runtime_config.fec_default_tree_schedule.is_empty() {
        return Ok(tree_ids.to_vec());
    }

    if runtime_config
        .fec_default_tree_schedule
        .iter()
        .any(|tree_id| !tree_ids.contains(tree_id))
    {
        return Err(PreflightError::TreeScheduleReferencesUnknownTree {
            tree_ids: runtime_config.fec_default_tree_schedule.clone(),
        });
    }

    Ok(runtime_config.fec_default_tree_schedule.clone())
}

fn derive_mettle_seed(session_id: u64) -> u64 {
    session_id ^ 0x4D45_5454_4C45_5631
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
            Self::MettleBlockSizeExceedsFramedPacket { value, max_payload } => write!(
                f,
                "mettle block_size {value} exceeds the current lossless framed-packet payload limit {max_payload}"
            ),
            Self::InvalidMettleCodedRate {
                numerator,
                denominator,
            } => write!(
                f,
                "mettle coded rate must be >= 1.0 with a non-zero denominator (got {numerator}/{denominator})"
            ),
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
            Self::TreeScheduleReferencesUnknownTree { tree_ids } => write!(
                f,
                "fec tree schedule may only reference configured manifest tree_ids (got {tree_ids:?})"
            ),
            Self::MultiTreeRequiresTreeVisibleIngress => write!(
                f,
                "collaborative multi-tree fec requires tree-visible non-blocking ingress for this session path"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nextmini_messages::lossless_session::FecScheme;

    #[test]
    fn derive_sender_policy_selects_mettle_when_enabled() {
        let cfg = LosslessConfig {
            fec_enabled: true,
            mettle_enabled: true,
            fec_default_tree_ids: vec![1, 3],
            mettle_coded_rate_numerator: 21,
            mettle_coded_rate_denominator: 20,
            ..Default::default()
        };

        let policy = derive_sender_policy(&cfg, 99).expect("mettle policy");
        let LosslessSessionMode::Fec(fec) = policy.mode else {
            panic!("expected fec mode");
        };
        assert_eq!(fec.scheme_kind(), Some(FecScheme::MettleV1));
        assert_eq!(fec.coded_rate_numerator, 21);
        assert_eq!(fec.coded_rate_denominator, 20);
        assert_eq!(fec.tree_ids, vec![1, 3]);
        assert_eq!(fec.symbols_per_block, 0);
    }

    #[test]
    fn derive_sender_policy_rejects_invalid_mettle_rate() {
        let cfg = LosslessConfig {
            fec_enabled: true,
            mettle_enabled: true,
            mettle_coded_rate_numerator: 19,
            mettle_coded_rate_denominator: 20,
            ..Default::default()
        };

        assert_eq!(
            derive_sender_policy(&cfg, 99),
            Err(PreflightError::InvalidMettleCodedRate {
                numerator: 19,
                denominator: 20,
            })
        );
    }

    #[test]
    fn validate_mode_block_size_rejects_oversized_mettle_payloads() {
        let mode = LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(21, 20, 9, vec![1]));

        assert_eq!(
            validate_mode_block_size(&mode, max_mettle_symbol_payload_bytes() + 1),
            Err(PreflightError::MettleBlockSizeExceedsFramedPacket {
                value: max_mettle_symbol_payload_bytes() + 1,
                max_payload: max_mettle_symbol_payload_bytes(),
            })
        );
        assert_eq!(
            validate_mode_block_size(&mode, max_mettle_symbol_payload_bytes()),
            Ok(())
        );
    }
}
