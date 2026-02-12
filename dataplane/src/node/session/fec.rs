//! Thin, wire-agnostic adapter between session logic and `raptorq` primitives.
//!
//! The lossless session subsystem can use this module without taking a direct
//! dependency on frame layout or transport metadata.
#![allow(dead_code)]

use raptorq::{EmittedSymbol, InactivationDecoder, SystematicEncoder};

pub use raptorq::{DecodeError, DecodeStats, ReceivedSymbol};

/// Shared block-level parameters for encoder/decoder construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockParams {
    pub source_symbols: usize,
    pub symbol_size: usize,
    pub seed: u64,
}

impl BlockParams {
    #[must_use]
    pub const fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self {
            source_symbols,
            symbol_size,
            seed,
        }
    }
}

/// Adapter-level representation of emitted source/repair symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedSymbol {
    pub esi: u32,
    pub payload: Vec<u8>,
    pub is_source: bool,
    pub degree: usize,
}

impl From<EmittedSymbol> for EncodedSymbol {
    fn from(symbol: EmittedSymbol) -> Self {
        Self {
            esi: symbol.esi,
            payload: symbol.data,
            is_source: symbol.is_source,
            degree: symbol.degree,
        }
    }
}

/// Thin encoder wrapper around `raptorq`.
#[derive(Debug)]
pub struct Encoder {
    inner: SystematicEncoder,
}

impl Encoder {
    /// Constructs an encoder from already partitioned source symbols.
    #[must_use]
    pub fn new(source_symbols: &[Vec<u8>], symbol_size: usize, seed: u64) -> Option<Self> {
        SystematicEncoder::new(source_symbols, symbol_size, seed).map(|inner| Self { inner })
    }

    /// Constructs an encoder from shared block parameters.
    #[must_use]
    pub fn from_block(params: BlockParams, source_symbols: &[Vec<u8>]) -> Option<Self> {
        if source_symbols.len() != params.source_symbols {
            return None;
        }
        Self::new(source_symbols, params.symbol_size, params.seed)
    }

    /// Emits systematic symbols (ESI 0..K-1) in deterministic order.
    #[must_use]
    pub fn emit_systematic(&mut self) -> Vec<EncodedSymbol> {
        self.inner
            .emit_systematic()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Emits `repair_count` repair symbols in deterministic ESI order.
    #[must_use]
    pub fn emit_repair(&mut self, repair_count: usize) -> Vec<EncodedSymbol> {
        self.inner
            .emit_repair(repair_count)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Generates a deterministic repair symbol for the provided ESI.
    #[must_use]
    pub fn repair_symbol(&self, esi: u32) -> Vec<u8> {
        self.inner.repair_symbol(esi)
    }

    #[must_use]
    pub const fn next_repair_esi(&self) -> u32 {
        self.inner.next_repair_esi()
    }

    #[must_use]
    pub fn source_symbol_count(&self) -> usize {
        self.inner.params().k
    }

    #[must_use]
    pub fn symbol_size(&self) -> usize {
        self.inner.params().symbol_size
    }
}

/// Lightweight decoder parameter view for callers that need sizing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecoderParams {
    pub source_symbols: usize,
    pub intermediate_symbols: usize,
    pub symbol_size: usize,
}

/// Adapter-level decode output.
#[derive(Debug, Clone)]
pub struct DecodeOutput {
    pub source_symbols: Vec<Vec<u8>>,
    pub intermediate_symbols: Vec<Vec<u8>>,
    pub stats: DecodeStats,
}

/// Thin decoder wrapper around `raptorq`.
pub struct Decoder {
    inner: InactivationDecoder,
}

impl Decoder {
    #[must_use]
    pub fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self {
            inner: InactivationDecoder::new(source_symbols, symbol_size, seed),
        }
    }

    #[must_use]
    pub fn from_block(params: BlockParams) -> Self {
        Self::new(params.source_symbols, params.symbol_size, params.seed)
    }

    #[must_use]
    pub fn params(&self) -> DecoderParams {
        let inner = self.inner.params();
        DecoderParams {
            source_symbols: inner.k,
            intermediate_symbols: inner.l,
            symbol_size: inner.symbol_size,
        }
    }

    /// Builds a source symbol in decoder input format.
    #[must_use]
    pub fn source_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        assert!(
            (esi as usize) < self.inner.params().k,
            "source ESI must be less than K"
        );
        ReceivedSymbol::source(esi, payload)
    }

    /// Builds a repair symbol in decoder input format.
    #[must_use]
    pub fn repair_symbol(&self, esi: u32, payload: Vec<u8>) -> ReceivedSymbol {
        let (columns, coefficients) = self.inner.repair_equation(esi);
        ReceivedSymbol::repair(esi, columns, coefficients, payload)
    }

    /// Returns deterministic zero-valued constraint symbols (LDPC + HDPC).
    #[must_use]
    pub fn constraint_symbols(&self) -> Vec<ReceivedSymbol> {
        self.inner.constraint_symbols()
    }

    pub fn decode(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        self.inner.decode(symbols).map(|decoded| DecodeOutput {
            source_symbols: decoded.source,
            intermediate_symbols: decoded.intermediate,
            stats: decoded.stats,
        })
    }
}
