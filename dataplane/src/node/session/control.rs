//! Helpers for interpreting and tracking lossless session control frames.

use std::collections::BTreeMap;

use nextmini_messages::lossless_session::{
    FecCapabilities, FecManifest, LOSSLESS_SESSION_FEC_VERSION, LosslessSessionControl,
};

/// Track cumulative ACK progress for each receiver. Returns `Some(new_value)`
/// when the receiver reports forward progress, `None` otherwise.
pub fn update_receiver_progress(
    from_node: usize,
    ctrl: &LosslessSessionControl,
    progress: &mut BTreeMap<usize, u64>,
) -> Option<u64> {
    match ctrl {
        LosslessSessionControl::Ack { up_to } => {
            if let Some(entry) = progress.get_mut(&from_node)
                && *up_to > *entry
            {
                *entry = *up_to;
                return Some(*entry);
            }
            None
        }
        LosslessSessionControl::Manifest { .. }
        | LosslessSessionControl::FecManifest { .. }
        | LosslessSessionControl::Ready { .. }
        | LosslessSessionControl::FecCapabilities { .. }
        | LosslessSessionControl::FecStatus { .. }
        | LosslessSessionControl::Eot { .. } => None,
    }
}

/// Track per-block FEC completion for each receiver.
///
/// `FecStatus` is interpreted as explicit block state, not cumulative ACK
/// progress. Only `deficit_symbols == 0` advances the completed block watermark.
pub fn update_receiver_fec_status(
    from_node: usize,
    ctrl: &LosslessSessionControl,
    progress: &mut BTreeMap<usize, u64>,
) -> Option<u64> {
    match ctrl {
        LosslessSessionControl::FecStatus { status } => {
            if status.deficit_symbols != 0 {
                return None;
            }
            if let Some(entry) = progress.get_mut(&from_node)
                && status.block_id > *entry
            {
                *entry = status.block_id;
                return Some(*entry);
            }
            None
        }
        LosslessSessionControl::Manifest { .. }
        | LosslessSessionControl::FecManifest { .. }
        | LosslessSessionControl::Ready { .. }
        | LosslessSessionControl::FecCapabilities { .. }
        | LosslessSessionControl::Ack { .. }
        | LosslessSessionControl::Eot { .. } => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FecCompatibilityError {
    UnsupportedProtocolVersion { required: u8, advertised: u8 },
    UnknownScheme { scheme: u8 },
    UnsupportedScheme { scheme: u8 },
}

/// Evaluates whether a peer capability advertisement is compatible with the requested FEC manifest.
pub fn ensure_fec_compatible(
    manifest: &FecManifest,
    capabilities: &FecCapabilities,
) -> Result<(), FecCompatibilityError> {
    let required_version = manifest.protocol_version.max(LOSSLESS_SESSION_FEC_VERSION);
    if capabilities.protocol_version < required_version {
        return Err(FecCompatibilityError::UnsupportedProtocolVersion {
            required: required_version,
            advertised: capabilities.protocol_version,
        });
    }
    if manifest.scheme_kind().is_none() {
        return Err(FecCompatibilityError::UnknownScheme {
            scheme: manifest.scheme,
        });
    }
    if !capabilities.supports_scheme_wire(manifest.scheme) {
        return Err(FecCompatibilityError::UnsupportedScheme {
            scheme: manifest.scheme,
        });
    }
    Ok(())
}

/// Returns true when sender-side strict FEC preflight must abort before any FEC data is emitted.
pub fn should_abort_fec_preflight(
    required_receivers: &[usize],
    manifest: &FecManifest,
    capabilities_by_peer: &BTreeMap<usize, FecCapabilities>,
) -> bool {
    for peer in required_receivers {
        let Some(capabilities) = capabilities_by_peer.get(peer) else {
            return true;
        };
        if ensure_fec_compatible(manifest, capabilities).is_err() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use nextmini_messages::lossless_session::FecScheme;

    #[test]
    fn update_receiver_progress_advances_when_monotonic() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(7, 2);

        let updated =
            update_receiver_progress(7, &LosslessSessionControl::Ack { up_to: 5 }, &mut progress);

        assert_eq!(updated, Some(5));
        assert_eq!(progress.get(&7).copied(), Some(5));
    }

    #[test]
    fn update_receiver_progress_ignores_missing_or_regressions() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(1, 4);

        let regression =
            update_receiver_progress(1, &LosslessSessionControl::Ack { up_to: 2 }, &mut progress);
        assert!(regression.is_none());
        assert_eq!(progress.get(&1).copied(), Some(4));

        let missing = update_receiver_progress(
            99,
            &LosslessSessionControl::Ack { up_to: 10 },
            &mut progress,
        );
        assert!(missing.is_none());
        assert!(!progress.contains_key(&99));
    }

    #[test]
    fn update_receiver_fec_status_advances_only_on_zero_deficit() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(7, 3);

        let pending = update_receiver_fec_status(
            7,
            &LosslessSessionControl::FecStatus {
                status: nextmini_messages::lossless_session::FecStatus {
                    block_id: 4,
                    deficit_symbols: 2,
                },
            },
            &mut progress,
        );
        assert!(pending.is_none());
        assert_eq!(progress.get(&7).copied(), Some(3));

        let complete = update_receiver_fec_status(
            7,
            &LosslessSessionControl::FecStatus {
                status: nextmini_messages::lossless_session::FecStatus {
                    block_id: 4,
                    deficit_symbols: 0,
                },
            },
            &mut progress,
        );
        assert_eq!(complete, Some(4));
        assert_eq!(progress.get(&7).copied(), Some(4));
    }

    #[test]
    fn update_receiver_fec_status_ignores_missing_or_regression() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(1, 8);

        let regression = update_receiver_fec_status(
            1,
            &LosslessSessionControl::FecStatus {
                status: nextmini_messages::lossless_session::FecStatus {
                    block_id: 7,
                    deficit_symbols: 0,
                },
            },
            &mut progress,
        );
        assert!(regression.is_none());
        assert_eq!(progress.get(&1).copied(), Some(8));

        let missing = update_receiver_fec_status(
            99,
            &LosslessSessionControl::FecStatus {
                status: nextmini_messages::lossless_session::FecStatus {
                    block_id: 9,
                    deficit_symbols: 0,
                },
            },
            &mut progress,
        );
        assert!(missing.is_none());
        assert!(!progress.contains_key(&99));
    }

    #[test]
    fn ensure_fec_compatible_rejects_unknown_and_unsupported_schemes() {
        let manifest = FecManifest::new_raptorq(32, 1400);
        let supported = FecCapabilities::default();
        assert!(ensure_fec_compatible(&manifest, &supported).is_ok());

        let unsupported = FecCapabilities::empty();
        assert_eq!(
            ensure_fec_compatible(&manifest, &unsupported),
            Err(FecCompatibilityError::UnsupportedScheme {
                scheme: FecScheme::RaptorQ.to_wire()
            })
        );

        let unknown_manifest = FecManifest {
            scheme: 99,
            ..manifest
        };
        assert_eq!(
            ensure_fec_compatible(&unknown_manifest, &supported),
            Err(FecCompatibilityError::UnknownScheme { scheme: 99 })
        );
    }

    #[test]
    fn should_abort_fec_preflight_requires_all_peers_to_be_compatible() {
        let manifest = FecManifest::new_raptorq(32, 1400);
        let required = vec![1usize, 2usize];

        let mut caps = BTreeMap::new();
        caps.insert(1, FecCapabilities::default());
        assert!(should_abort_fec_preflight(&required, &manifest, &caps));

        caps.insert(2, FecCapabilities::empty());
        assert!(should_abort_fec_preflight(&required, &manifest, &caps));

        caps.insert(2, FecCapabilities::default());
        assert!(!should_abort_fec_preflight(&required, &manifest, &caps));
    }
}
