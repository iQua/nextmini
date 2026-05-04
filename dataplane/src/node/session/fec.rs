//! Thin, wire-agnostic adapter between session logic and FEC primitives.
//!
//! The lossless session subsystem can use this module without taking a direct
//! dependency on frame layout or transport metadata.

use nextmini_messages::lossless_session::FecScheme;
use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockDecoder,
    SourceBlockEncoder,
};

const FEC_BLOCK_SEED_SESSION_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const FEC_BLOCK_SEED_BLOCK_MULTIPLIER: u64 = 0xBF58_476D_1CE4_E5B9;

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
    fn oti(&self) -> ObjectTransmissionInformation {
        ObjectTransmissionInformation::new(
            (self.source_symbols * self.symbol_size) as u64,
            self.symbol_size as u16,
            1, // source_blocks
            1, // sub_blocks
            1, // alignment — use 1 to avoid sub-symbol interleaving
        )
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
    #[must_use]
    pub fn from_block(params: BlockParams, source_block: &[u8]) -> Option<Self> {
        if params.source_symbols == 0 {
            return None;
        }
        if source_block.len() != params.source_symbols * params.symbol_size {
            return None;
        }
        let inner = match params.scheme {
            FecScheme::RaptorQ => {
                EncoderInner::RaptorQ(SourceBlockEncoder::new(0, &params.oti(), source_block))
            }
            FecScheme::Mettle => return None,
        };
        Some(Self {
            inner,
            k: params.source_symbols,
        })
    }

    /// Generates a deterministic RaptorQ repair symbol payload for the provided ESI.
    #[must_use]
    pub fn coded_symbol(&self, esi: u32) -> Option<Vec<u8>> {
        match &self.inner {
            EncoderInner::RaptorQ(inner) => {
                let repair_index = self.repair_index(esi)?;
                let packets = inner.repair_packets(repair_index, 1);
                packets
                    .into_iter()
                    .next()
                    .map(|packet| packet.data().to_vec())
            }
        }
    }

    fn repair_index(&self, esi: u32) -> Option<u32> {
        esi.checked_sub(u32::try_from(self.k).ok()?)
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
}

impl Decoder {
    /// Construct a decoder from shared block parameters.
    #[must_use]
    pub fn from_block(params: BlockParams) -> Self {
        Self {
            k: params.source_symbols,
            symbol_size: params.symbol_size,
            params,
        }
    }

    /// Builds a source symbol in decoder input format.
    #[must_use]
    pub fn source_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        assert!((esi as usize) < self.k, "source ESI must be less than K");
        assert_eq!(
            self.params.scheme,
            FecScheme::RaptorQ,
            "METTLE uses the streaming decoder path, not block decoder symbols"
        );
        ReceivedSymbol {
            kind: ReceivedSymbolKind::RaptorQ { esi },
            payload,
        }
    }

    /// Builds a coded symbol in decoder input format.
    #[must_use]
    pub fn coded_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        assert_eq!(
            self.params.scheme,
            FecScheme::RaptorQ,
            "METTLE uses the streaming decoder path, not block decoder symbols"
        );
        ReceivedSymbol {
            kind: ReceivedSymbolKind::RaptorQ { esi },
            payload,
        }
    }

    /// Attempt to reconstruct the source symbols from the received symbol set.
    pub fn decode(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        match self.params.scheme {
            FecScheme::RaptorQ => self.decode_raptorq(symbols),
            FecScheme::Mettle => Err(DecodeError::InvalidSymbol),
        }
    }

    fn decode_raptorq(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        let oti = self.params.oti();
        let block_length = (self.params.source_symbols * self.params.symbol_size) as u64;
        let mut decoder = SourceBlockDecoder::new(0, &oti, block_length);
        let packets: Vec<EncodingPacket> = symbols
            .iter()
            .map(|sym| match sym.kind {
                ReceivedSymbolKind::RaptorQ { esi } => Ok(EncodingPacket::new(
                    PayloadId::new(0, esi),
                    sym.payload.clone(),
                )),
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
pub fn initial_symbol_count(params: BlockParams) -> Option<u32> {
    match params.scheme {
        FecScheme::RaptorQ => u32::try_from(params.source_symbols).ok(),
        FecScheme::Mettle => {
            mettle::block::BlockParams::new(params.source_symbols, params.symbol_size, params.seed)
                .metadata()
                .ok()
                .and_then(|metadata| u32::try_from(metadata.initial_symbol_count()).ok())
        }
    }
}

#[allow(dead_code)]
fn mettle_repair_deficit(params: BlockParams, symbol_ids: impl IntoIterator<Item = u32>) -> u16 {
    let Ok(metadata) =
        mettle::block::BlockParams::new(params.source_symbols, params.symbol_size, params.seed)
            .metadata()
    else {
        return 1;
    };

    let mut sources = Vec::new();
    let mut repairs = Vec::new();
    for symbol_id in symbol_ids {
        if (symbol_id as usize) < params.source_symbols {
            sources.push(symbol_id as usize);
        } else {
            repairs.push(symbol_id.saturating_sub(params.source_symbols as u32) as usize);
        }
    }

    let additional = match metadata.estimate_repair_deficit(sources, repairs) {
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
        let decoder = Decoder::from_block(params);

        let symbols: Vec<ReceivedSymbol> = source_data
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect();
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
        let decoder = Decoder::from_block(params);

        let half_k = k / 2;

        let mut symbols: Vec<ReceivedSymbol> = source_data[..half_k]
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect();
        symbols.extend((0..(k - half_k)).map(|offset| {
            let esi = k as u32 + offset as u32;
            decoder.coded_symbol(esi, encoder.coded_symbol(esi).expect("coded symbol"))
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
        let decoder = Decoder::from_block(params);

        let half_k = k / 2;

        let mut symbols: Vec<ReceivedSymbol> = source_data[..half_k]
            .iter()
            .enumerate()
            .map(|(esi, payload)| decoder.source_symbol(esi as u32, payload.clone()))
            .collect();
        symbols.extend((0..(k - half_k)).map(|offset| {
            let esi = k as u32 + offset as u32;
            decoder.coded_symbol(esi, encoder.coded_symbol(esi).expect("coded symbol"))
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
