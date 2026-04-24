//! Finite, wire-agnostic block adapter for the METTLE paper kernel.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::decoder::MettleDecoder;
use crate::encoder::{MettleBin, MettleEncoder};
use crate::{MettleParams, OverheadRatio};

/// Shared block-level parameters for encoder/decoder construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockParams {
    /// Fixed terminal source-symbol count for this block.
    pub source_symbols: usize,
    /// Size in bytes of every source and repair symbol.
    pub symbol_size: usize,
    /// Deterministic graph seed shared by sender and receiver.
    pub seed: u64,
}

impl BlockParams {
    /// Build a reusable parameter bundle with the paper-native METTLE profile.
    #[must_use]
    pub const fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self {
            source_symbols,
            symbol_size,
            seed,
        }
    }

    fn validate(self) -> Result<ValidatedBlockParams, BlockError> {
        if self.source_symbols == 0 {
            return Err(BlockError::InvalidParams);
        }
        let source_symbol_bytes =
            NonZeroUsize::new(self.symbol_size).ok_or(BlockError::InvalidParams)?;
        let source_block_len = self
            .source_symbols
            .checked_mul(self.symbol_size)
            .ok_or(BlockError::InvalidParams)?;
        let terminal_source_count =
            u64::try_from(self.source_symbols).map_err(|_| BlockError::InvalidParams)?;

        Ok(ValidatedBlockParams {
            params: self,
            source_symbol_bytes,
            source_block_len,
            terminal_source_count,
        })
    }

    /// Build metadata for this block without requiring source payload bytes.
    pub fn metadata(self) -> Result<BlockMetadata, BlockError> {
        BlockMetadata::new(self)
    }

    fn mettle_params(self) -> MettleParams {
        // Paper: Phase-1 block integration fixes the graph profile to the paper
        // constants (l=4, w=600) and 5% coded-bin expansion, rather than taking
        // locally configurable parameters that are not negotiated on the wire.
        let overhead = OverheadRatio::new(1, 20).expect("paper overhead is non-zero");
        MettleParams::new(overhead)
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedBlockParams {
    params: BlockParams,
    source_symbol_bytes: NonZeroUsize,
    source_block_len: usize,
    terminal_source_count: u64,
}

/// Block construction and symbol lookup failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockError {
    InvalidParams,
    WrongSourceBlockLength,
    SourceIndexOutOfRange,
    RepairIndexOutOfRange,
}

/// Adapter-level decode failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Block(BlockError),
    WrongSymbolLength,
    InsufficientSymbols,
}

impl From<BlockError> for DecodeError {
    fn from(error: BlockError) -> Self {
        Self::Block(error)
    }
}

/// Opaque received symbol for decoder input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSymbol {
    kind: ReceivedSymbolKind,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceivedSymbolKind {
    Source { source_index: usize },
    Repair { repair_index: usize },
}

/// Metadata-only view of a finite METTLE block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    params: BlockParams,
    repair_bin_ids: Vec<u128>,
}

impl BlockMetadata {
    /// Build metadata for this block without allocating payload storage.
    pub fn new(params: BlockParams) -> Result<Self, BlockError> {
        let validated = params.validate()?;
        Ok(Self {
            params,
            repair_bin_ids: repair_bin_ids(validated),
        })
    }

    /// Returns the finite number of repair symbols available for this block.
    #[must_use]
    pub fn repair_symbol_count(&self) -> usize {
        self.repair_bin_ids.len()
    }

    /// Returns the METTLE repair bin id represented by an adapter repair index.
    pub fn repair_bin_id(&self, repair_index: usize) -> Result<u128, BlockError> {
        self.repair_bin_ids
            .get(repair_index)
            .copied()
            .ok_or(BlockError::RepairIndexOutOfRange)
    }

    /// Estimate how many future repair symbols are needed if future repairs
    /// arrive monotonically after the highest already-received repair index.
    pub fn estimate_repair_deficit(
        &self,
        received_sources: impl IntoIterator<Item = usize>,
        received_repairs: impl IntoIterator<Item = usize>,
    ) -> Result<Option<usize>, BlockError> {
        let mut estimator = MetadataPeelingEstimator::new(self.params.source_symbols);

        for source_index in received_sources {
            estimator.observe_source(self, source_index)?;
        }

        let mut next_future_repair_index = 0usize;
        for repair_index in received_repairs {
            self.repair_bin_id(repair_index)?;
            next_future_repair_index = next_future_repair_index.max(repair_index + 1);
            estimator.observe_repair(self, repair_index)?;
        }
        estimator.drain();

        let mut additional_repair_symbols = 0usize;
        while !estimator.is_complete() && next_future_repair_index < self.repair_bin_ids.len() {
            estimator.observe_repair(self, next_future_repair_index)?;
            next_future_repair_index += 1;
            additional_repair_symbols += 1;
            estimator.drain();
        }

        Ok(estimator.is_complete().then_some(additional_repair_symbols))
    }

    fn bin_touchers(&self, bin_id: u128) -> Vec<usize> {
        let terminal_source_count =
            u64::try_from(self.params.source_symbols).expect("validated in constructor");
        let Some((earliest_source_id, latest_source_id)) = self
            .params
            .mettle_params()
            .possible_source_id_range_for_bin(bin_id, Some(terminal_source_count))
        else {
            return Vec::new();
        };

        (earliest_source_id..=latest_source_id)
            .filter(|&source_id| {
                self.params
                    .mettle_params()
                    .unique_edge_bin_ids_with_terminal_source_count(
                        source_id,
                        self.params.seed,
                        Some(terminal_source_count),
                    )
                    .contains(&bin_id)
            })
            .map(|source_id| source_id as usize)
            .collect()
    }

    fn repair_touchers(&self, repair_index: usize) -> Result<Vec<usize>, BlockError> {
        let repair_bin_id = self.repair_bin_id(repair_index)?;
        Ok(self.bin_touchers(repair_bin_id))
    }
}

#[derive(Debug)]
struct MetadataPeelingEstimator {
    known_sources: Vec<bool>,
    repairs: Vec<Vec<usize>>,
}

impl MetadataPeelingEstimator {
    fn new(source_symbols: usize) -> Self {
        Self {
            known_sources: vec![false; source_symbols],
            repairs: Vec::new(),
        }
    }

    fn observe_source(
        &mut self,
        metadata: &BlockMetadata,
        source_index: usize,
    ) -> Result<(), BlockError> {
        if source_index >= self.known_sources.len() {
            return Err(BlockError::SourceIndexOutOfRange);
        }
        let source_id = u64::try_from(source_index).expect("source index fits u64");
        let source_bin_id = metadata.params.mettle_params().tle_bin_id(source_id);
        let touchers = metadata.bin_touchers(source_bin_id);
        if !touchers.is_empty() {
            self.repairs.push(touchers);
        }
        Ok(())
    }

    fn observe_repair(
        &mut self,
        metadata: &BlockMetadata,
        repair_index: usize,
    ) -> Result<(), BlockError> {
        let touchers = metadata.repair_touchers(repair_index)?;
        if !touchers.is_empty() {
            self.repairs.push(touchers);
        }
        Ok(())
    }

    fn drain(&mut self) {
        loop {
            let Some(source_index) = self.repairs.iter().find_map(|touchers| {
                let mut unknown_touchers = touchers
                    .iter()
                    .copied()
                    .filter(|&source_index| !self.known_sources[source_index]);
                let source_index = unknown_touchers.next()?;
                unknown_touchers.next().is_none().then_some(source_index)
            }) else {
                break;
            };
            self.known_sources[source_index] = true;
        }
    }

    fn is_complete(&self) -> bool {
        self.known_sources
            .iter()
            .all(|&source_is_known| source_is_known)
    }
}

/// Thin encoder wrapper around the METTLE paper encoder.
#[derive(Debug)]
pub struct Encoder {
    repair_symbols: Vec<Vec<u8>>,
}

impl Encoder {
    /// Constructs an encoder from one already-padded block image.
    pub fn from_block(params: BlockParams, source_block: &[u8]) -> Result<Self, BlockError> {
        let validated = params.validate()?;
        if source_block.len() != validated.source_block_len {
            return Err(BlockError::WrongSourceBlockLength);
        }

        let metadata = BlockMetadata::new(params)?;
        let mut encoder = MettleEncoder::new_terminated(
            params.mettle_params(),
            validated.source_symbol_bytes,
            params.seed,
            validated.terminal_source_count,
        );
        let mut encoded_bins = Vec::new();

        for source_payload in source_block.chunks_exact(params.symbol_size) {
            encoded_bins.extend(encoder.push_source(source_payload));
        }
        encoded_bins.extend(encoder.finish());

        let repair_bin_ids = metadata
            .repair_bin_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut repair_symbols = Vec::with_capacity(metadata.repair_bin_ids.len());
        for bin in encoded_bins {
            let (bin_id, payload) = bin.into_parts();
            if repair_bin_ids.contains(&bin_id) {
                repair_symbols.push((bin_id, payload));
            }
        }
        repair_symbols.sort_unstable_by_key(|(bin_id, _)| *bin_id);
        let repair_symbols = repair_symbols
            .into_iter()
            .map(|(_, payload)| payload)
            .collect::<Vec<_>>();

        Ok(Self { repair_symbols })
    }

    /// Generates a deterministic repair symbol payload for the repair index.
    pub fn repair_symbol(&self, repair_index: usize) -> Result<Vec<u8>, BlockError> {
        self.repair_symbols
            .get(repair_index)
            .cloned()
            .ok_or(BlockError::RepairIndexOutOfRange)
    }
}

/// Adapter-level decode output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeOutput {
    /// Reconstructed source symbols in systematic order.
    pub source_symbols: Vec<Vec<u8>>,
}

/// Thin decoder wrapper around the METTLE paper decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decoder {
    params: BlockParams,
}

impl Decoder {
    /// Construct a decoder from shared block parameters.
    #[must_use]
    pub const fn from_block(params: BlockParams) -> Self {
        Self { params }
    }

    /// Builds a source symbol in decoder input format.
    #[must_use]
    pub fn source_symbol(&self, source_index: usize, payload: Vec<u8>) -> ReceivedSymbol {
        ReceivedSymbol {
            kind: ReceivedSymbolKind::Source { source_index },
            payload,
        }
    }

    /// Builds a repair symbol in decoder input format.
    #[must_use]
    pub fn repair_symbol(&self, repair_index: usize, payload: Vec<u8>) -> ReceivedSymbol {
        ReceivedSymbol {
            kind: ReceivedSymbolKind::Repair { repair_index },
            payload,
        }
    }

    /// Attempt to reconstruct the full fixed-K source block.
    pub fn decode(&self, symbols: &[ReceivedSymbol]) -> Result<DecodeOutput, DecodeError> {
        let validated = self.params.validate()?;
        let metadata = BlockMetadata::new(self.params)?;
        let mut decoder = MettleDecoder::new_terminated(
            self.params.mettle_params(),
            validated.source_symbol_bytes,
            self.params.seed,
            validated.terminal_source_count,
        );
        let mut decoded_symbols = vec![None; self.params.source_symbols];

        for symbol in symbols {
            if symbol.payload.len() != self.params.symbol_size {
                return Err(DecodeError::WrongSymbolLength);
            }

            let bin_id = match symbol.kind {
                ReceivedSymbolKind::Source { source_index } => {
                    if source_index >= self.params.source_symbols {
                        return Err(BlockError::SourceIndexOutOfRange.into());
                    }
                    self.params.mettle_params().tle_bin_id(source_index as u64)
                }
                ReceivedSymbolKind::Repair { repair_index } => {
                    metadata.repair_bin_id(repair_index)?
                }
            };

            for decoded in decoder.push_bin(MettleBin::new(bin_id, symbol.payload.clone())) {
                let (source_id, payload) = decoded.into_parts();
                let source_index =
                    usize::try_from(source_id).expect("source id fits validated source count");
                decoded_symbols[source_index] = Some(payload);
            }
        }

        decoded_symbols
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .map(|source_symbols| DecodeOutput { source_symbols })
            .ok_or(DecodeError::InsufficientSymbols)
    }
}

fn repair_bin_ids(validated: ValidatedBlockParams) -> Vec<u128> {
    let end_exclusive = validated
        .params
        .mettle_params()
        .terminal_departure_end_exclusive(validated.terminal_source_count);
    let mut repair_bin_ids = Vec::new();
    let mut next_source_id = 0u64;

    for bin_id in 0..end_exclusive {
        if next_source_id < validated.terminal_source_count
            && validated.params.mettle_params().tle_bin_id(next_source_id) == bin_id
        {
            next_source_id += 1;
        } else {
            repair_bin_ids.push(bin_id);
        }
    }

    repair_bin_ids
}
