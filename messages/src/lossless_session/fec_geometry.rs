//! Dependency-free wire geometry for lossless-session FEC symbols.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Largest serialized IPv4 packet accepted by the local framing layer.
pub const MAX_IPV4_PACKET_LEN: usize = u16::MAX as usize;
/// IPv4 header length emitted by the lossless packet envelope.
pub const LOSSLESS_IPV4_HEADER_LEN: usize = 20;
/// Base TCP header length emitted by the lossless packet envelope.
pub const LOSSLESS_TCP_BASE_HEADER_LEN: usize = 20;
/// TCP option bytes used to carry lossless session and tree metadata.
pub const LOSSLESS_TCP_META_OPTION_LEN: usize = 16;
/// TCP header length for a lossless-session packet.
pub const LOSSLESS_TCP_HEADER_LEN: usize =
    LOSSLESS_TCP_BASE_HEADER_LEN + LOSSLESS_TCP_META_OPTION_LEN;
/// Fixed `BlockSymbol` body metadata before its symbol payload.
pub const LOSSLESS_BLOCK_SYMBOL_METADATA_LEN: usize = 8 + 4 + 2 + 2;
/// Largest FEC symbol payload that fits the local IPv4/TCP/session envelope.
pub const MAX_FEC_SYMBOL_PAYLOAD: usize = MAX_IPV4_PACKET_LEN
    - LOSSLESS_IPV4_HEADER_LEN
    - LOSSLESS_TCP_HEADER_LEN
    - super::LosslessSessionHeader::LEN
    - LOSSLESS_BLOCK_SYMBOL_METADATA_LEN;

/// Checked wire-level source-symbol geometry shared by manifests and dataplane code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireFecGeometry {
    source_symbols: u32,
    symbol_size: u32,
    padded_block_size: u64,
}

impl WireFecGeometry {
    /// Derive fixed-width source symbols for one manifest block.
    pub fn new(block_size: u32, source_symbols: u32) -> Result<Self, WireFecGeometryError> {
        if block_size == 0 {
            return Err(WireFecGeometryError::ZeroBlockSize);
        }
        if source_symbols == 0 {
            return Err(WireFecGeometryError::ZeroSourceSymbols);
        }

        let symbol_size = block_size.div_ceil(source_symbols);
        let max_symbol_size = u32::try_from(MAX_FEC_SYMBOL_PAYLOAD)
            .map_err(|_| WireFecGeometryError::SymbolPayloadCeilingUnrepresentable)?;
        if symbol_size > max_symbol_size {
            return Err(WireFecGeometryError::SymbolPayloadTooLarge {
                symbol_size,
                max: max_symbol_size,
            });
        }

        let padded_block_size = u64::from(source_symbols)
            .checked_mul(u64::from(symbol_size))
            .ok_or(WireFecGeometryError::PaddedBlockSizeOverflow {
                source_symbols,
                symbol_size,
            })?;

        Ok(Self {
            source_symbols,
            symbol_size,
            padded_block_size,
        })
    }

    /// Configured source-symbol count (`K`).
    #[must_use]
    pub const fn source_symbols(self) -> u32 {
        self.source_symbols
    }

    /// Derived fixed symbol payload length (`T`).
    #[must_use]
    pub const fn symbol_size(self) -> u32 {
        self.symbol_size
    }

    /// Checked padded block length (`K*T`).
    #[must_use]
    pub const fn padded_block_size(self) -> u64 {
        self.padded_block_size
    }
}

/// Wire-level FEC geometry validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFecGeometryError {
    ZeroBlockSize,
    ZeroSourceSymbols,
    SymbolPayloadCeilingUnrepresentable,
    SymbolPayloadTooLarge {
        symbol_size: u32,
        max: u32,
    },
    PaddedBlockSizeOverflow {
        source_symbols: u32,
        symbol_size: u32,
    },
}

impl Display for WireFecGeometryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBlockSize => write!(f, "block_size must be >= 1"),
            Self::ZeroSourceSymbols => write!(f, "source_symbols must be >= 1"),
            Self::SymbolPayloadCeilingUnrepresentable => {
                write!(f, "FEC symbol payload ceiling does not fit the wire type")
            }
            Self::SymbolPayloadTooLarge { symbol_size, max } => write!(
                f,
                "FEC symbol payload size {symbol_size} exceeds envelope ceiling {max}"
            ),
            Self::PaddedBlockSizeOverflow {
                source_symbols,
                symbol_size,
            } => write!(
                f,
                "FEC padded block size overflows for K={source_symbols}, T={symbol_size}"
            ),
        }
    }
}

impl Error for WireFecGeometryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_payload_ceiling_matches_lossless_envelope() {
        assert_eq!(MAX_FEC_SYMBOL_PAYLOAD, 65_443);
    }

    #[test]
    fn geometry_records_checked_padding() {
        let geometry = WireFecGeometry::new(10, 4).expect("valid geometry");

        assert_eq!(geometry.source_symbols(), 4);
        assert_eq!(geometry.symbol_size(), 3);
        assert_eq!(geometry.padded_block_size(), 12);
    }
}
