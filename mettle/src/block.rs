//! Finite-stream metadata for the METTLE paper kernel.

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
    overhead: OverheadRatio,
}

impl BlockParams {
    /// Build a reusable parameter bundle with this crate's default overhead.
    ///
    /// Use [`Self::with_overhead`] for paper-style experiments where `c` is
    /// chosen for a specific erasure/channel condition.
    #[must_use]
    pub const fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Self {
        Self::with_overhead(source_symbols, symbol_size, seed, OverheadRatio::DEFAULT)
    }

    /// Build a reusable parameter bundle with an explicit METTLE overhead ratio.
    #[must_use]
    pub const fn with_overhead(
        source_symbols: usize,
        symbol_size: usize,
        seed: u64,
        overhead: OverheadRatio,
    ) -> Self {
        Self {
            source_symbols,
            symbol_size,
            seed,
            overhead,
        }
    }

    fn validate(self) -> Result<ValidatedBlockParams, BlockError> {
        if self.source_symbols == 0 || self.symbol_size == 0 {
            return Err(BlockError::InvalidParams);
        }
        let terminal_source_count =
            u64::try_from(self.source_symbols).map_err(|_| BlockError::InvalidParams)?;

        Ok(ValidatedBlockParams {
            params: self,
            terminal_source_count,
        })
    }

    /// Build metadata for this block without requiring source payload bytes.
    pub fn metadata(self) -> Result<BlockMetadata, BlockError> {
        BlockMetadata::new(self)
    }

    /// Return the paper-kernel parameters represented by this finite stream boundary.
    #[must_use]
    pub fn mettle_params(self) -> MettleParams {
        MettleParams::new(self.overhead)
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedBlockParams {
    params: BlockParams,
    terminal_source_count: u64,
}

/// Block construction and symbol lookup failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockError {
    InvalidParams,
    SourceIndexOutOfRange,
    RepairIndexOutOfRange,
}

/// Metadata-only view of a finite METTLE block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    params: BlockParams,
    coded_symbol_count: usize,
}

impl BlockMetadata {
    /// Build metadata for this block without allocating payload storage.
    pub fn new(params: BlockParams) -> Result<Self, BlockError> {
        let validated = params.validate()?;
        Ok(Self {
            params,
            coded_symbol_count: coded_symbol_count(validated)?,
        })
    }

    /// Returns the number of bins emitted before the lossless session protocol
    /// opens its first feedback round.
    #[must_use]
    pub fn initial_symbol_count(&self) -> usize {
        let source_count =
            u64::try_from(self.params.source_symbols).expect("validated source count fits u64");
        usize::try_from(
            self.params
                .mettle_params()
                .departure_frontier_after_source_count(source_count),
        )
        .expect("validated initial symbol count fits usize")
    }

    /// Returns the finite number of coded bins for this terminated stream.
    #[must_use]
    pub const fn symbol_count(&self) -> usize {
        self.coded_symbol_count
    }

    /// Returns the finite number of repair symbols available for this block.
    #[must_use]
    pub fn repair_symbol_count(&self) -> usize {
        self.coded_symbol_count
            .saturating_sub(self.initial_symbol_count())
    }

    /// Returns the METTLE repair bin id represented by an adapter repair index.
    pub fn repair_bin_id(&self, repair_index: usize) -> Result<u128, BlockError> {
        let bin_id = self
            .initial_symbol_count()
            .checked_add(repair_index)
            .ok_or(BlockError::RepairIndexOutOfRange)?;
        if bin_id >= self.coded_symbol_count {
            return Err(BlockError::RepairIndexOutOfRange);
        }
        Ok(bin_id as u128)
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
            if source_index >= self.params.source_symbols {
                return Err(BlockError::SourceIndexOutOfRange);
            }
            estimator.observe_bin(self, source_index as u128)?;
        }

        let mut next_future_repair_index = 0usize;
        for repair_index in received_repairs {
            let bin_id = self.repair_bin_id(repair_index)?;
            next_future_repair_index = next_future_repair_index.max(repair_index + 1);
            estimator.observe_bin(self, bin_id)?;
        }
        estimator.drain();

        let mut additional_repair_symbols = 0usize;
        while !estimator.is_complete() && next_future_repair_index < self.repair_symbol_count() {
            let bin_id = self.repair_bin_id(next_future_repair_index)?;
            estimator.observe_bin(self, bin_id)?;
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

    fn observe_bin(&mut self, metadata: &BlockMetadata, bin_id: u128) -> Result<(), BlockError> {
        if bin_id >= metadata.coded_symbol_count as u128 {
            return Err(BlockError::RepairIndexOutOfRange);
        }
        let touchers = metadata.bin_touchers(bin_id);
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

fn coded_symbol_count(validated: ValidatedBlockParams) -> Result<usize, BlockError> {
    let end_exclusive = validated
        .params
        .mettle_params()
        .terminal_departure_end_exclusive(validated.terminal_source_count);
    usize::try_from(end_exclusive).map_err(|_| BlockError::InvalidParams)
}
