//! Thin, wire-agnostic bridge between session logic and FEC primitives.
//!
//! The lossless session subsystem can use this module without taking a direct
//! dependency on frame layout or transport metadata.

use nextmini_messages::lossless_session::{
    FecScheme, LosslessSessionFecMode, MAX_FEC_SYMBOL_PAYLOAD, WireFecGeometry,
    WireFecGeometryError,
};
use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockDecoder,
    SourceBlockEncoder,
};

const FEC_BLOCK_SEED_SESSION_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const FEC_BLOCK_SEED_BLOCK_MULTIPLIER: u64 = 0xBF58_476D_1CE4_E5B9;

/// RFC 6330 maximum source symbols in one source block.
pub(crate) const RAPTORQ_MAX_SOURCE_SYMBOLS: u32 = 56_403;
/// RaptorQ encoding symbol identifiers are unsigned 24-bit values.
pub(crate) const RAPTORQ_SYMBOL_ID_END_EXCLUSIVE: u32 = 1 << 24;

/// Checked FEC construction and adapter failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FecError {
    WireGeometry(WireFecGeometryError),
    UnknownScheme {
        scheme: u8,
    },
    UnsupportedAdapterScheme {
        scheme: FecScheme,
    },
    RaptorQSourceSymbolsOutOfRange {
        source_symbols: u32,
        max: u32,
    },
    RaptorQSymbolSizeOutOfRange {
        symbol_size: u32,
    },
    SourceSymbolsDoNotFitHost {
        source_symbols: u32,
    },
    SymbolSizeDoesNotFitHost {
        symbol_size: u32,
    },
    PaddedBlockSizeDoesNotFitHost {
        padded_block_size: u64,
    },
    InvalidMettleCodedRate {
        numerator: u32,
        denominator: u32,
    },
    InvalidMettleGeometry,
    MettleStreamTooLong {
        symbol_count: u128,
        max: u32,
    },
    PaddedBlockSizeOverflow {
        source_symbols: usize,
        symbol_size: usize,
    },
    SourceBlockLengthMismatch {
        expected: usize,
        actual: usize,
    },
    SymbolIdOutOfRange {
        scheme: FecScheme,
        symbol_id: u32,
        end_exclusive: u32,
    },
    SourceSymbolIdOutOfRange {
        symbol_id: u32,
        source_symbols: u32,
    },
    CodedSymbolIdBeforeRepairRange {
        symbol_id: u32,
        source_symbols: u32,
    },
    SymbolPayloadLengthMismatch {
        expected: usize,
        actual: usize,
    },
    SymbolIdExhausted {
        scheme: FecScheme,
        last_symbol_id: u32,
    },
    GeneratedSymbolMissing {
        symbol_id: u32,
    },
}

impl std::fmt::Display for FecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WireGeometry(err) => std::fmt::Display::fmt(err, f),
            Self::UnknownScheme { scheme } => write!(f, "unknown FEC scheme {scheme}"),
            Self::UnsupportedAdapterScheme { scheme } => {
                write!(f, "{scheme:?} does not use the RaptorQ block adapter")
            }
            Self::RaptorQSourceSymbolsOutOfRange {
                source_symbols,
                max,
            } => write!(
                f,
                "RaptorQ source-symbol count {source_symbols} is outside 1..={max}"
            ),
            Self::RaptorQSymbolSizeOutOfRange { symbol_size } => write!(
                f,
                "RaptorQ symbol size {symbol_size} does not fit its 16-bit OTI field"
            ),
            Self::SourceSymbolsDoNotFitHost { source_symbols } => write!(
                f,
                "source-symbol count {source_symbols} does not fit this host"
            ),
            Self::SymbolSizeDoesNotFitHost { symbol_size } => {
                write!(f, "symbol size {symbol_size} does not fit this host")
            }
            Self::PaddedBlockSizeDoesNotFitHost { padded_block_size } => write!(
                f,
                "padded block size {padded_block_size} does not fit this host"
            ),
            Self::InvalidMettleCodedRate {
                numerator,
                denominator,
            } => write!(f, "invalid METTLE coded rate {numerator}/{denominator}"),
            Self::InvalidMettleGeometry => write!(f, "invalid terminated METTLE geometry"),
            Self::MettleStreamTooLong { symbol_count, max } => write!(
                f,
                "terminated METTLE symbol count {symbol_count} exceeds wire limit {max}"
            ),
            Self::PaddedBlockSizeOverflow {
                source_symbols,
                symbol_size,
            } => write!(
                f,
                "padded block size overflows for K={source_symbols}, T={symbol_size}"
            ),
            Self::SourceBlockLengthMismatch { expected, actual } => write!(
                f,
                "source block length {actual} does not match checked padded length {expected}"
            ),
            Self::SymbolIdOutOfRange {
                scheme,
                symbol_id,
                end_exclusive,
            } => write!(
                f,
                "{scheme:?} symbol id {symbol_id} is outside 0..{end_exclusive}"
            ),
            Self::SourceSymbolIdOutOfRange {
                symbol_id,
                source_symbols,
            } => write!(
                f,
                "source symbol id {symbol_id} is outside 0..{source_symbols}"
            ),
            Self::CodedSymbolIdBeforeRepairRange {
                symbol_id,
                source_symbols,
            } => write!(
                f,
                "coded symbol id {symbol_id} is below source-symbol count {source_symbols}"
            ),
            Self::SymbolPayloadLengthMismatch { expected, actual } => write!(
                f,
                "symbol payload length {actual} does not match expected length {expected}"
            ),
            Self::SymbolIdExhausted {
                scheme,
                last_symbol_id,
            } => write!(
                f,
                "{scheme:?} symbol-id space exhausted after {last_symbol_id}"
            ),
            Self::GeneratedSymbolMissing { symbol_id } => {
                write!(f, "codec did not generate requested symbol {symbol_id}")
            }
        }
    }
}

impl std::error::Error for FecError {}

impl From<WireFecGeometryError> for FecError {
    fn from(value: WireFecGeometryError) -> Self {
        Self::WireGeometry(value)
    }
}

/// Codec-validated geometry derived from a syntactically valid manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValidatedFecGeometry {
    wire: WireFecGeometry,
    scheme: FecScheme,
    mettle_stream_symbol_limit: Option<u32>,
}

impl ValidatedFecGeometry {
    pub(crate) const fn wire(self) -> WireFecGeometry {
        self.wire
    }

    pub(crate) const fn mettle_stream_symbol_limit(self) -> Option<u32> {
        self.mettle_stream_symbol_limit
    }

    pub(crate) const fn symbol_id_bounds(self) -> FecSymbolIdBounds {
        FecSymbolIdBounds {
            scheme: self.scheme,
            end_exclusive: match self.mettle_stream_symbol_limit {
                Some(limit) => limit,
                None => RAPTORQ_SYMBOL_ID_END_EXCLUSIVE,
            },
        }
    }
}

/// Scheme-specific symbol-id namespace accepted by a validated session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FecSymbolIdBounds {
    scheme: FecScheme,
    end_exclusive: u32,
}

impl FecSymbolIdBounds {
    #[cfg(test)]
    pub(crate) const fn raptorq() -> Self {
        Self {
            scheme: FecScheme::RaptorQ,
            end_exclusive: RAPTORQ_SYMBOL_ID_END_EXCLUSIVE,
        }
    }

    pub(crate) fn validate(self, symbol_id: u32) -> Result<(), FecError> {
        if symbol_id >= self.end_exclusive {
            return Err(FecError::SymbolIdOutOfRange {
                scheme: self.scheme,
                symbol_id,
                end_exclusive: self.end_exclusive,
            });
        }
        Ok(())
    }

    pub(crate) fn next_after(self, symbol_id: u32) -> Result<u32, FecError> {
        self.validate(symbol_id)?;
        let next = symbol_id
            .checked_add(1)
            .ok_or(FecError::SymbolIdExhausted {
                scheme: self.scheme,
                last_symbol_id: symbol_id,
            })?;
        if next >= self.end_exclusive {
            return Err(FecError::SymbolIdExhausted {
                scheme: self.scheme,
                last_symbol_id: symbol_id,
            });
        }
        Ok(next)
    }

    #[cfg(test)]
    pub(crate) const fn end_exclusive(self) -> u32 {
        self.end_exclusive
    }
}

/// Apply codec-specific constraints after dependency-free wire validation.
pub(crate) fn validate_fec_geometry(
    block_size: u32,
    fec_mode: &LosslessSessionFecMode,
) -> Result<ValidatedFecGeometry, FecError> {
    let wire = WireFecGeometry::new(block_size, fec_mode.symbols_per_block)?;
    let scheme = fec_mode.scheme_kind().ok_or(FecError::UnknownScheme {
        scheme: fec_mode.scheme,
    })?;

    let mettle_stream_symbol_limit = match scheme {
        FecScheme::RaptorQ => {
            if wire.source_symbols() > RAPTORQ_MAX_SOURCE_SYMBOLS {
                return Err(FecError::RaptorQSourceSymbolsOutOfRange {
                    source_symbols: wire.source_symbols(),
                    max: RAPTORQ_MAX_SOURCE_SYMBOLS,
                });
            }
            u16::try_from(wire.symbol_size()).map_err(|_| {
                FecError::RaptorQSymbolSizeOutOfRange {
                    symbol_size: wire.symbol_size(),
                }
            })?;
            None
        }
        FecScheme::Mettle => {
            let overhead = mettle_overhead_from_fec_mode(fec_mode).ok_or(
                FecError::InvalidMettleCodedRate {
                    numerator: fec_mode.coded_rate_num,
                    denominator: fec_mode.coded_rate_den,
                },
            )?;
            let source_symbols = usize::try_from(wire.source_symbols()).map_err(|_| {
                FecError::SourceSymbolsDoNotFitHost {
                    source_symbols: wire.source_symbols(),
                }
            })?;
            let symbol_size = usize::try_from(wire.symbol_size()).map_err(|_| {
                FecError::SymbolSizeDoesNotFitHost {
                    symbol_size: wire.symbol_size(),
                }
            })?;
            usize::try_from(wire.padded_block_size()).map_err(|_| {
                FecError::PaddedBlockSizeDoesNotFitHost {
                    padded_block_size: wire.padded_block_size(),
                }
            })?;
            let metadata =
                mettle::block::BlockParams::with_overhead(source_symbols, symbol_size, 0, overhead)
                    .metadata()
                    .map_err(|_| FecError::InvalidMettleGeometry)?;
            let symbol_count = u32::try_from(metadata.symbol_count()).map_err(|_| {
                FecError::MettleStreamTooLong {
                    symbol_count: u128::try_from(metadata.symbol_count()).unwrap_or(u128::MAX),
                    max: u32::MAX,
                }
            })?;
            Some(symbol_count)
        }
    };

    Ok(ValidatedFecGeometry {
        wire,
        scheme,
        mettle_stream_symbol_limit,
    })
}

/// Convert the configured METTLE coded-rate knob into the overhead ratio `c`.
///
/// `1/1` maps to `c=0`; `21/20` maps to `c=1/20`.
pub(crate) fn mettle_overhead_from_coded_rate(
    numerator: u32,
    denominator: u32,
) -> Option<mettle::OverheadRatio> {
    if denominator == 0 || numerator < denominator {
        return None;
    }
    if numerator == denominator {
        return Some(mettle::OverheadRatio::ZERO);
    }
    mettle::OverheadRatio::new(numerator - denominator, denominator).ok()
}

/// Return the METTLE overhead encoded in the session manifest.
pub(crate) fn mettle_overhead_from_fec_mode(
    fec_mode: &LosslessSessionFecMode,
) -> Option<mettle::OverheadRatio> {
    mettle_overhead_from_coded_rate(fec_mode.coded_rate_num, fec_mode.coded_rate_den)
}

/// Deterministically derives the FEC block seed shared by sender and receiver.
#[must_use]
pub const fn block_seed(session_id: u64, block_id: u64) -> u64 {
    session_id.wrapping_mul(FEC_BLOCK_SEED_SESSION_MULTIPLIER)
        ^ block_id.wrapping_mul(FEC_BLOCK_SEED_BLOCK_MULTIPLIER)
}

/// Shared block-level parameters for encoder/decoder construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockParams {
    pub source_symbols: usize,
    pub symbol_size: usize,
    pub seed: u64,
    pub scheme: FecScheme,
}

impl BlockParams {
    /// Build a reusable RaptorQ encoder/decoder parameter bundle for one logical block.
    #[must_use]
    #[allow(dead_code)]
    pub const fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self::with_scheme(source_symbols, symbol_size, seed, FecScheme::RaptorQ)
    }

    /// Build a reusable encoder/decoder parameter bundle for one logical block.
    #[must_use]
    pub const fn with_scheme(
        source_symbols: usize,
        symbol_size: usize,
        seed: u64,
        scheme: FecScheme,
    ) -> Self {
        Self {
            source_symbols,
            symbol_size,
            seed,
            scheme,
        }
    }

    /// Build the RFC 6330 Object Transmission Information for this block.
    fn oti(&self) -> Result<ObjectTransmissionInformation, FecError> {
        if self.scheme != FecScheme::RaptorQ {
            return Err(FecError::UnsupportedAdapterScheme {
                scheme: self.scheme,
            });
        }
        let source_symbols = u32::try_from(self.source_symbols).map_err(|_| {
            FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols: u32::MAX,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            }
        })?;
        if source_symbols == 0 || source_symbols > RAPTORQ_MAX_SOURCE_SYMBOLS {
            return Err(FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            });
        }
        let symbol_size =
            u32::try_from(self.symbol_size).map_err(|_| FecError::RaptorQSymbolSizeOutOfRange {
                symbol_size: u32::MAX,
            })?;
        let symbol_size_usize = usize::try_from(symbol_size)
            .map_err(|_| FecError::SymbolSizeDoesNotFitHost { symbol_size })?;
        if symbol_size == 0 || symbol_size_usize > MAX_FEC_SYMBOL_PAYLOAD {
            return Err(FecError::RaptorQSymbolSizeOutOfRange { symbol_size });
        }
        let symbol_size = u16::try_from(symbol_size)
            .map_err(|_| FecError::RaptorQSymbolSizeOutOfRange { symbol_size })?;
        let transfer_length = u64::try_from(self.source_symbols)
            .ok()
            .and_then(|source_symbols| {
                u64::try_from(self.symbol_size)
                    .ok()
                    .and_then(|symbol_size| source_symbols.checked_mul(symbol_size))
            })
            .ok_or(FecError::PaddedBlockSizeOverflow {
                source_symbols: self.source_symbols,
                symbol_size: self.symbol_size,
            })?;

        Ok(ObjectTransmissionInformation::new(
            transfer_length,
            symbol_size,
            1, // source_blocks
            1, // sub_blocks
            1, // alignment — use 1 to avoid sub-symbol interleaving
        ))
    }
}

/// Reason for decode failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    InsufficientSymbols,
    InvalidSymbol,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientSymbols => write!(f, "InsufficientSymbols"),
            Self::InvalidSymbol => write!(f, "InvalidSymbol"),
        }
    }
}

/// Opaque received symbol for decoder input.
#[derive(Debug, Clone)]
pub struct ReceivedSymbol {
    kind: ReceivedSymbolKind,
    payload: Vec<u8>,
}

#[derive(Debug, Clone)]
enum ReceivedSymbolKind {
    RaptorQ { esi: u32 },
}

/// Thin encoder wrapper around block-materialized FEC backends.
///
/// METTLE intentionally does not use this path: its sender advances the
/// paper-native streaming encoder directly and produces coded bins on demand.
#[derive(Debug)]
pub struct Encoder {
    inner: EncoderInner,
    k: usize,
}

#[derive(Debug)]
enum EncoderInner {
    RaptorQ(SourceBlockEncoder),
}

impl Encoder {
    /// Constructs an encoder from one padded block image.
    pub fn from_block(params: BlockParams, source_block: &[u8]) -> Result<Self, FecError> {
        let oti = params.oti()?;
        let expected = params
            .source_symbols
            .checked_mul(params.symbol_size)
            .ok_or(FecError::PaddedBlockSizeOverflow {
                source_symbols: params.source_symbols,
                symbol_size: params.symbol_size,
            })?;
        if source_block.len() != expected {
            return Err(FecError::SourceBlockLengthMismatch {
                expected,
                actual: source_block.len(),
            });
        }
        let inner = match params.scheme {
            FecScheme::RaptorQ => {
                EncoderInner::RaptorQ(SourceBlockEncoder::new(0, &oti, source_block))
            }
            FecScheme::Mettle => {
                return Err(FecError::UnsupportedAdapterScheme {
                    scheme: params.scheme,
                });
            }
        };
        Ok(Self {
            inner,
            k: params.source_symbols,
        })
    }

    /// Generates a deterministic RaptorQ repair symbol payload for the provided ESI.
    pub fn coded_symbol(&self, esi: u32) -> Result<Vec<u8>, FecError> {
        let bounds = FecSymbolIdBounds {
            scheme: FecScheme::RaptorQ,
            end_exclusive: RAPTORQ_SYMBOL_ID_END_EXCLUSIVE,
        };
        bounds.validate(esi)?;
        match &self.inner {
            EncoderInner::RaptorQ(inner) => {
                let repair_index = self.repair_index(esi)?;
                let packets = inner.repair_packets(repair_index, 1);
                packets
                    .into_iter()
                    .next()
                    .map(|packet| packet.data().to_vec())
                    .ok_or(FecError::GeneratedSymbolMissing { symbol_id: esi })
            }
        }
    }

    fn repair_index(&self, esi: u32) -> Result<u32, FecError> {
        let source_symbols =
            u32::try_from(self.k).map_err(|_| FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols: u32::MAX,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            })?;
        esi.checked_sub(source_symbols)
            .ok_or(FecError::CodedSymbolIdBeforeRepairRange {
                symbol_id: esi,
                source_symbols,
            })
    }
}

/// Adapter-level decode output.
#[derive(Debug, Clone)]
pub struct DecodeOutput {
    /// Reconstructed source symbols in systematic order.
    pub source_symbols: Vec<Vec<u8>>,
}

/// Thin decoder wrapper around `raptorq::SourceBlockDecoder`.
pub struct Decoder {
    k: usize,
    symbol_size: usize,
    params: BlockParams,
    oti: ObjectTransmissionInformation,
    block_length: u64,
}

impl Decoder {
    /// Construct a decoder from shared block parameters.
    pub fn from_block(params: BlockParams) -> Result<Self, FecError> {
        let oti = params.oti()?;
        let block_length = u64::try_from(params.source_symbols)
            .ok()
            .and_then(|source_symbols| {
                u64::try_from(params.symbol_size)
                    .ok()
                    .and_then(|symbol_size| source_symbols.checked_mul(symbol_size))
            })
            .ok_or(FecError::PaddedBlockSizeOverflow {
                source_symbols: params.source_symbols,
                symbol_size: params.symbol_size,
            })?;
        Ok(Self {
            k: params.source_symbols,
            symbol_size: params.symbol_size,
            params,
            oti,
            block_length,
        })
    }

    /// Builds a source symbol in decoder input format.
    pub fn source_symbol(&self, esi: u32, payload: Vec<u8>) -> Result<ReceivedSymbol, FecError> {
        self.validate_payload(&payload)?;
        self.validate_adapter_scheme()?;
        let source_symbols =
            u32::try_from(self.k).map_err(|_| FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols: u32::MAX,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            })?;
        if esi >= source_symbols {
            return Err(FecError::SourceSymbolIdOutOfRange {
                symbol_id: esi,
                source_symbols,
            });
        }
        Ok(ReceivedSymbol {
            kind: ReceivedSymbolKind::RaptorQ { esi },
            payload,
        })
    }

    /// Builds a coded symbol in decoder input format.
    pub fn coded_symbol(&self, esi: u32, payload: Vec<u8>) -> Result<ReceivedSymbol, FecError> {
        self.validate_payload(&payload)?;
        self.validate_adapter_scheme()?;
        let source_symbols =
            u32::try_from(self.k).map_err(|_| FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols: u32::MAX,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            })?;
        if esi < source_symbols {
            return Err(FecError::CodedSymbolIdBeforeRepairRange {
                symbol_id: esi,
                source_symbols,
            });
        }
        FecSymbolIdBounds {
            scheme: FecScheme::RaptorQ,
            end_exclusive: RAPTORQ_SYMBOL_ID_END_EXCLUSIVE,
        }
        .validate(esi)?;
        Ok(ReceivedSymbol {
            kind: ReceivedSymbolKind::RaptorQ { esi },
            payload,
        })
    }

    fn validate_adapter_scheme(&self) -> Result<(), FecError> {
        if self.params.scheme != FecScheme::RaptorQ {
            return Err(FecError::UnsupportedAdapterScheme {
                scheme: self.params.scheme,
            });
        }
        Ok(())
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<(), FecError> {
        if payload.len() != self.symbol_size {
            return Err(FecError::SymbolPayloadLengthMismatch {
                expected: self.symbol_size,
                actual: payload.len(),
            });
        }
        Ok(())
    }

    /// Attempt to reconstruct the source symbols from the received symbol set.
    pub fn decode(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        match self.params.scheme {
            FecScheme::RaptorQ => self.decode_raptorq(symbols),
            FecScheme::Mettle => Err(DecodeError::InvalidSymbol),
        }
    }

    fn decode_raptorq(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        let mut decoder = SourceBlockDecoder::new(0, &self.oti, self.block_length);
        let packets: Vec<EncodingPacket> = symbols
            .iter()
            .map(|sym| match sym.kind {
                ReceivedSymbolKind::RaptorQ { esi } if esi < RAPTORQ_SYMBOL_ID_END_EXCLUSIVE => Ok(
                    EncodingPacket::new(PayloadId::new(0, esi), sym.payload.clone()),
                ),
                ReceivedSymbolKind::RaptorQ { .. } => Err(DecodeError::InvalidSymbol),
            })
            .collect::<Result<_, _>>()?;
        match decoder.decode(packets) {
            Some(flat_data) => {
                let mut source_syms = Vec::with_capacity(self.k);
                for i in 0..self.k {
                    let start = i * self.symbol_size;
                    let end = start + self.symbol_size;
                    if end <= flat_data.len() {
                        source_syms.push(flat_data[start..end].to_vec());
                    } else if start < flat_data.len() {
                        let mut sym = vec![0u8; self.symbol_size];
                        sym[..flat_data.len() - start].copy_from_slice(&flat_data[start..]);
                        source_syms.push(sym);
                    } else {
                        source_syms.push(vec![0u8; self.symbol_size]);
                    }
                }
                Ok(DecodeOutput {
                    source_symbols: source_syms,
                })
            }
            None => Err(DecodeError::InsufficientSymbols),
        }
    }
}

/// Compute how many future repair symbols are needed for the selected scheme.
#[must_use]
#[allow(dead_code)]
pub fn repair_deficit(params: BlockParams, symbol_ids: impl IntoIterator<Item = u32>) -> u16 {
    match params.scheme {
        FecScheme::RaptorQ => {
            let present = symbol_ids.into_iter().count();
            if present >= params.source_symbols {
                1
            } else {
                u16::try_from(params.source_symbols - present)
                    .unwrap_or(u16::MAX)
                    .max(1)
            }
        }
        FecScheme::Mettle => mettle_repair_deficit(params, symbol_ids),
    }
}

/// Return the number of symbols to emit before opening the first FEC feedback
/// round for the selected scheme.
#[must_use]
pub fn initial_symbol_count(
    params: BlockParams,
    mettle_overhead: mettle::OverheadRatio,
) -> Option<u32> {
    match params.scheme {
        FecScheme::RaptorQ => u32::try_from(params.source_symbols).ok(),
        FecScheme::Mettle => mettle::block::BlockParams::with_overhead(
            params.source_symbols,
            params.symbol_size,
            params.seed,
            mettle_overhead,
        )
        .metadata()
        .ok()
        .and_then(|metadata| u32::try_from(metadata.initial_symbol_count()).ok()),
    }
}

#[allow(dead_code)]
fn mettle_repair_deficit(params: BlockParams, symbol_ids: impl IntoIterator<Item = u32>) -> u16 {
    let Ok(metadata) = mettle::block::BlockParams::with_overhead(
        params.source_symbols,
        params.symbol_size,
        params.seed,
        mettle::OverheadRatio::ZERO,
    )
    .metadata() else {
        return 1;
    };

    let initial_symbol_count = metadata.initial_symbol_count();
    let mut initial_bins = Vec::new();
    let mut repairs = Vec::new();
    for symbol_id in symbol_ids {
        if (symbol_id as usize) < initial_symbol_count {
            initial_bins.push(symbol_id as usize);
        } else {
            repairs.push(symbol_id.saturating_sub(initial_symbol_count as u32) as usize);
        }
    }

    let additional = match metadata.estimate_repair_deficit(initial_bins, repairs) {
        Ok(Some(additional)) => additional,
        _ => return 1,
    };
    u16::try_from(additional).unwrap_or(u16::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAPER_SCALE_METTLE_K: usize = 2400;

    #[test]
    fn block_seed_is_stable_for_known_input() {
        assert_eq!(block_seed(0xA55A, 17), 0xD429_E47F_291A_692B);
    }

    #[test]
    fn raptorq_oti_rejects_oversized_symbol_without_truncation() {
        let params = BlockParams::new(32, 65_536, 0);

        assert_eq!(
            params.oti(),
            Err(FecError::RaptorQSymbolSizeOutOfRange {
                symbol_size: 65_536,
            })
        );
    }

    #[test]
    fn codec_geometry_rejects_raptorq_k_above_rfc_limit() {
        let fec_mode = LosslessSessionFecMode::new_raptorq(56_404, vec![0]);

        assert_eq!(
            validate_fec_geometry(56_404, &fec_mode),
            Err(FecError::RaptorQSourceSymbolsOutOfRange {
                source_symbols: 56_404,
                max: RAPTORQ_MAX_SOURCE_SYMBOLS,
            })
        );
    }

    #[test]
    fn peer_raptorq_symbol_inputs_are_checked_before_codec_construction() {
        let decoder =
            Decoder::from_block(BlockParams::new(4, 2, 0)).expect("valid RaptorQ decoder geometry");
        let bounds = FecSymbolIdBounds::raptorq();
        let mut symbol_ids = vec![
            0,
            3,
            4,
            bounds.end_exclusive() - 1,
            bounds.end_exclusive(),
            u32::MAX,
        ];
        let mut state = 0xA5A5_5A5Au32;
        for _ in 0..512 {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            symbol_ids.push(state);
        }

        for symbol_id in symbol_ids {
            for payload_len in 0..=4 {
                let payload = vec![0; payload_len];
                assert_eq!(
                    decoder.source_symbol(symbol_id, payload.clone()).is_ok(),
                    payload_len == 2 && symbol_id < 4,
                    "source classification mismatch for ESI {symbol_id} and length {payload_len}"
                );
                assert_eq!(
                    decoder.coded_symbol(symbol_id, payload).is_ok(),
                    payload_len == 2 && (4..bounds.end_exclusive()).contains(&symbol_id),
                    "repair classification mismatch for ESI {symbol_id} and length {payload_len}"
                );
            }
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let k = 32usize;
        let symbol_size = 64;
        let source_data: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                let mut v = vec![0u8; symbol_size];
                v[0] = (i & 0xFF) as u8;
                v
            })
            .collect();

        let params = BlockParams::new(k, symbol_size, 0);
        let decoder = Decoder::from_block(params).expect("valid RaptorQ decoder geometry");

        let symbols: Vec<ReceivedSymbol> = source_data
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect::<Result<_, _>>()
            .expect("valid source symbols");
        let output = decoder.decode(&symbols).unwrap();
        for (i, (decoded, expected)) in output
            .source_symbols
            .iter()
            .zip(source_data.iter())
            .enumerate()
        {
            assert_eq!(decoded, expected, "symbol {i} mismatch");
        }
    }

    #[test]
    fn decode_with_coded() {
        let k = 32usize;
        let symbol_size = 64;
        let source_data: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                let mut v = vec![0u8; symbol_size];
                v[0] = (i & 0xFF) as u8;
                v
            })
            .collect();

        let params = BlockParams::new(k, symbol_size, 0);
        let flat: Vec<u8> = source_data
            .iter()
            .flat_map(|symbol| symbol.iter().copied())
            .collect();
        let encoder = Encoder::from_block(params, &flat).unwrap();
        let decoder = Decoder::from_block(params).expect("valid RaptorQ decoder geometry");

        let half_k = k / 2;

        let mut symbols: Vec<ReceivedSymbol> = source_data[..half_k]
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect::<Result<_, _>>()
            .expect("valid source symbols");
        symbols.extend((0..(k - half_k)).map(|offset| {
            let esi = k as u32 + offset as u32;
            decoder
                .coded_symbol(esi, encoder.coded_symbol(esi).expect("coded symbol"))
                .expect("valid coded symbol")
        }));

        let output = decoder.decode(&symbols).unwrap();
        for (i, (decoded, expected)) in output
            .source_symbols
            .iter()
            .zip(source_data.iter())
            .enumerate()
        {
            assert_eq!(decoded, expected, "symbol {i} mismatch");
        }
    }

    #[test]
    fn decode_with_coded_large_symbols() {
        let k = 8usize;
        let symbol_size = 16 * 1024;
        let source_data: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                (0..symbol_size)
                    .map(|j| ((i * 31 + j) % 251) as u8)
                    .collect()
            })
            .collect();

        let params = BlockParams::new(k, symbol_size, 0);
        let flat: Vec<u8> = source_data
            .iter()
            .flat_map(|symbol| symbol.iter().copied())
            .collect();
        let encoder = Encoder::from_block(params, &flat).unwrap();
        let decoder = Decoder::from_block(params).expect("valid RaptorQ decoder geometry");

        let half_k = k / 2;

        let mut symbols: Vec<ReceivedSymbol> = source_data[..half_k]
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect::<Result<_, _>>()
            .expect("valid source symbols");
        symbols.extend((0..(k - half_k)).map(|offset| {
            let esi = k as u32 + offset as u32;
            decoder
                .coded_symbol(esi, encoder.coded_symbol(esi).expect("coded symbol"))
                .expect("valid coded symbol")
        }));

        let output = decoder.decode(&symbols).unwrap();
        for (i, (decoded, expected)) in output
            .source_symbols
            .iter()
            .zip(source_data.iter())
            .enumerate()
        {
            assert_eq!(decoded, expected, "symbol {i} mismatch");
        }
    }

    #[test]
    fn mettle_deficit_accounts_for_non_contiguous_repairs() {
        let k = PAPER_SCALE_METTLE_K;
        let params = BlockParams::with_scheme(k, 1, 0xA55A, FecScheme::Mettle);
        let received_sources = (0..(k - 3)).map(|source_index| source_index as u32);
        let received_repairs = [10u32, 12, 17].map(|repair_index| k as u32 + repair_index);
        let deficit = repair_deficit(params, received_sources.chain(received_repairs));

        assert!(
            deficit > 1,
            "non-contiguous METTLE repairs should not be treated as an immediately decodable K-count set"
        );
    }
}
