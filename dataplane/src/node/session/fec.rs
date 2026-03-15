//! Thin, wire-agnostic adapter between session logic and `raptorq` primitives.
//!
//! The lossless session subsystem can use this module without taking a direct
//! dependency on frame layout or transport metadata.

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
}

impl BlockParams {
    /// Build a reusable encoder/decoder parameter bundle for one logical block.
    #[must_use]
    pub const fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self {
            source_symbols,
            symbol_size,
            seed,
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
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InsufficientSymbols")
    }
}

/// Opaque received symbol for decoder input.
#[derive(Debug, Clone)]
pub struct ReceivedSymbol {
    esi: u32,
    payload: Vec<u8>,
}

/// Thin encoder wrapper around `raptorq::SourceBlockEncoder`.
#[derive(Debug)]
pub struct Encoder {
    inner: SourceBlockEncoder,
    k: usize,
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
        let inner = SourceBlockEncoder::new(0, &params.oti(), source_block);
        Some(Self {
            inner,
            k: params.source_symbols,
        })
    }

    /// Generates a deterministic coded symbol payload for the provided ESI (ESI >= K).
    #[must_use]
    pub fn coded_symbol(&self, esi: u32) -> Vec<u8> {
        let coded_index = esi.saturating_sub(self.k as u32);
        let packets = self.inner.repair_packets(coded_index, 1);
        packets.into_iter().next().unwrap().data().to_vec()
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
    oti: ObjectTransmissionInformation,
    block_length: u64,
}

impl Decoder {
    /// Construct a decoder for one logical block.
    #[must_use]
    pub fn new(source_symbols: usize, symbol_size: usize, _seed: u64) -> Self {
        let params = BlockParams::new(source_symbols, symbol_size, _seed);
        let oti = params.oti();
        Self {
            k: source_symbols,
            symbol_size,
            oti,
            block_length: (source_symbols * symbol_size) as u64,
        }
    }

    /// Construct a decoder from shared block parameters.
    #[must_use]
    pub fn from_block(params: BlockParams) -> Self {
        Self::new(params.source_symbols, params.symbol_size, params.seed)
    }

    /// Builds a source symbol in decoder input format.
    #[must_use]
    pub fn source_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        assert!((esi as usize) < self.k, "source ESI must be less than K");
        ReceivedSymbol { esi, payload }
    }

    /// Builds a coded symbol in decoder input format.
    #[must_use]
    pub fn coded_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        ReceivedSymbol { esi, payload }
    }

    /// Attempt to reconstruct the source symbols from the received symbol set.
    pub fn decode(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        let mut decoder = SourceBlockDecoder::new(0, &self.oti, self.block_length);
        let packets: Vec<EncodingPacket> = symbols
            .iter()
            .map(|sym| EncodingPacket::new(PayloadId::new(0, sym.esi), sym.payload.clone()))
            .collect();
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

#[cfg(test)]
mod tests {
    use super::*;

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
            decoder.coded_symbol(esi, encoder.coded_symbol(esi))
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
}
