//! Sender-side validation and mode selection for optional session FEC.

use std::fmt::{Display, Formatter};

use nextmini_messages::lossless_session::{
    FecFeedbackMode, LosslessSessionFecMode, LosslessSessionMode, MAX_MANIFEST_TREE_IDS,
};

use crate::node::config::{LosslessConfig, LosslessFecScheme};
use crate::node::session::fec::{self, FecError};

/// Errors reported before a sender session is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    InvalidBlockSize {
        value: usize,
    },
    BlockSizeTooLarge {
        value: usize,
    },
    ZeroSymbolsPerBlock,
    MissingTreeIds,
    TreeIdsMustBeSortedUnique {
        tree_ids: Vec<u16>,
    },
    TooManyTreeIds {
        configured: usize,
        max: usize,
    },
    InvalidMettleCodedRate {
        numerator: u32,
        denominator: u32,
    },
    UnsupportedFeedbackModeForScheme {
        feedback_mode: FecFeedbackMode,
        scheme: LosslessFecScheme,
    },
    InvalidCarouselTiming {
        reason: &'static str,
    },
    InvalidFecGeometry {
        reason: FecError,
    },
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
    block_size: u32,
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

    let feedback_mode = runtime_config.fec_feedback_mode;
    if feedback_mode == FecFeedbackMode::Carousel {
        validate_carousel_timing(runtime_config)?;
    }
    let fec_mode = match runtime_config.fec_default_scheme {
        LosslessFecScheme::RaptorQ => {
            LosslessSessionFecMode::new_raptorq(symbols_per_block, tree_ids)
                .with_feedback_mode(feedback_mode)
        }
        LosslessFecScheme::Mettle => {
            if feedback_mode == FecFeedbackMode::Carousel {
                return Err(PreflightError::UnsupportedFeedbackModeForScheme {
                    feedback_mode,
                    scheme: LosslessFecScheme::Mettle,
                });
            }
            let numerator = runtime_config.mettle_default_coded_rate_num;
            let denominator = runtime_config.mettle_default_coded_rate_den;
            if denominator == 0 || numerator < denominator {
                return Err(PreflightError::InvalidMettleCodedRate {
                    numerator,
                    denominator,
                });
            }
            LosslessSessionFecMode::new_mettle_with_coded_rate(
                symbols_per_block,
                tree_ids,
                numerator,
                denominator,
            )
            .with_feedback_mode(feedback_mode)
        }
    };
    fec::validate_fec_geometry(block_size, &fec_mode)
        .map_err(|reason| PreflightError::InvalidFecGeometry { reason })?;

    Ok(SenderPolicy {
        mode: LosslessSessionMode::Fec(fec_mode),
    })
}

pub(super) fn validate_carousel_timing(
    runtime_config: &LosslessConfig,
) -> Result<(), PreflightError> {
    let nonzero = [
        runtime_config.carousel_ack_debounce_ms,
        runtime_config.carousel_ack_heartbeat_ms,
        runtime_config.carousel_ack_probe_interval_ms,
        runtime_config.carousel_peer_silence_timeout_ms,
        runtime_config.carousel_peer_stall_timeout_ms,
        runtime_config.carousel_receiver_passive_window_ms,
        runtime_config.carousel_session_complete_interval_ms,
    ];
    if nonzero.contains(&0) || runtime_config.carousel_session_complete_repeats == 0 {
        return Err(PreflightError::InvalidCarouselTiming {
            reason: "all carousel intervals and repeat counts must be non-zero",
        });
    }
    if runtime_config.carousel_ack_debounce_ms >= runtime_config.carousel_ack_heartbeat_ms {
        return Err(PreflightError::InvalidCarouselTiming {
            reason: "ack debounce must be shorter than the ack heartbeat",
        });
    }
    if runtime_config.carousel_ack_heartbeat_ms >= runtime_config.carousel_peer_silence_timeout_ms
        || runtime_config.carousel_ack_probe_interval_ms
            >= runtime_config.carousel_peer_silence_timeout_ms
    {
        return Err(PreflightError::InvalidCarouselTiming {
            reason: "ack heartbeat and probe intervals must be shorter than peer silence timeout",
        });
    }
    if runtime_config.carousel_peer_silence_timeout_ms
        >= runtime_config.carousel_peer_stall_timeout_ms
    {
        return Err(PreflightError::InvalidCarouselTiming {
            reason: "peer stall timeout must be longer than peer silence timeout",
        });
    }
    let required_passive_window = runtime_config
        .carousel_peer_stall_timeout_ms
        .checked_add(runtime_config.carousel_passive_margin_ms)
        .ok_or(PreflightError::InvalidCarouselTiming {
            reason: "carousel abort budget plus passive margin overflows milliseconds",
        })?;
    if runtime_config.carousel_receiver_passive_window_ms < required_passive_window {
        return Err(PreflightError::InvalidCarouselTiming {
            reason: "receiver passive window must cover sender abort budget plus margin",
        });
    }
    Ok(())
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
            Self::UnsupportedFeedbackModeForScheme {
                feedback_mode,
                scheme,
            } => write!(
                f,
                "feedback mode {feedback_mode:?} is not implemented for FEC scheme {scheme:?}"
            ),
            Self::InvalidCarouselTiming { reason } => {
                write!(f, "invalid carousel timing configuration: {reason}")
            }
            Self::InvalidFecGeometry { reason } => {
                write!(f, "invalid FEC geometry: {reason}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fec_config() -> LosslessConfig {
        LosslessConfig {
            fec_enabled: true,
            ..LosslessConfig::default()
        }
    }

    #[test]
    fn sender_policy_defaults_to_rounds_feedback() {
        let policy = derive_sender_policy(&fec_config(), 1024).expect("valid default FEC policy");
        let LosslessSessionMode::Fec(mode) = policy.mode else {
            panic!("expected FEC policy");
        };
        assert_eq!(mode.feedback_mode, FecFeedbackMode::Rounds);
    }

    #[test]
    fn sender_policy_negotiates_raptorq_carousel() {
        let mut config = fec_config();
        config.fec_feedback_mode = FecFeedbackMode::Carousel;

        let policy = derive_sender_policy(&config, 1024).expect("valid carousel FEC policy");
        let LosslessSessionMode::Fec(mode) = policy.mode else {
            panic!("expected FEC policy");
        };
        assert_eq!(mode.feedback_mode, FecFeedbackMode::Carousel);
    }

    #[test]
    fn sender_policy_rejects_mettle_carousel_until_stage_two() {
        let mut config = fec_config();
        config.fec_default_scheme = LosslessFecScheme::Mettle;
        config.fec_feedback_mode = FecFeedbackMode::Carousel;

        assert_eq!(
            derive_sender_policy(&config, 1024),
            Err(PreflightError::UnsupportedFeedbackModeForScheme {
                feedback_mode: FecFeedbackMode::Carousel,
                scheme: LosslessFecScheme::Mettle,
            })
        );
    }

    #[test]
    fn carousel_timing_requires_passive_window_beyond_abort_budget() {
        let mut config = fec_config();
        config.fec_feedback_mode = FecFeedbackMode::Carousel;
        config.carousel_receiver_passive_window_ms =
            config.carousel_peer_stall_timeout_ms + config.carousel_passive_margin_ms - 1;

        assert!(matches!(
            derive_sender_policy(&config, 1024),
            Err(PreflightError::InvalidCarouselTiming { .. })
        ));
    }
}
