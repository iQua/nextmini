//! Shared block and symbol geometry for block-first lossless sessions.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Construction or derivation failures for shared block geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    BlockSizeZero,
    SymbolsPerBlockZero,
    SymbolsPerBlockTooLarge,
}

impl Display for PlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockSizeZero => write!(f, "block_size must be >= 1"),
            Self::SymbolsPerBlockZero => write!(f, "symbols_per_block must be >= 1"),
            Self::SymbolsPerBlockTooLarge => write!(f, "symbols_per_block does not fit usize"),
        }
    }
}

impl Error for PlanError {}

/// Immutable geometry for a block-first session object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockPlan {
    total_bytes: u64,
    block_size: usize,
    total_blocks: u64,
}

impl BlockPlan {
    /// Derive block geometry for a transfer of `total_bytes` using `block_size`.
    pub fn new(total_bytes: u64, block_size: usize) -> Result<Self, PlanError> {
        if block_size == 0 {
            return Err(PlanError::BlockSizeZero);
        }

        let total_blocks = if total_bytes == 0 {
            0
        } else {
            total_bytes.div_ceil(block_size as u64)
        };

        Ok(Self {
            total_bytes,
            block_size,
            total_blocks,
        })
    }

    /// Return the total number of logical blocks in the transfer.
    pub const fn total_blocks(&self) -> u64 {
        self.total_blocks
    }

    /// Return the logical object length in bytes when it fits on this host.
    pub(crate) fn total_bytes_usize(&self) -> Option<usize> {
        usize::try_from(self.total_bytes).ok()
    }

    /// Report whether `block_id` lies within the planned transfer range.
    pub fn contains_block(&self, block_id: u64) -> bool {
        block_id < self.total_blocks
    }

    /// Return the highest valid block identifier, if any blocks exist.
    pub fn last_block_id(&self) -> Option<u64> {
        self.total_blocks.checked_sub(1)
    }

    /// Return the byte offset of the start of `block_id`.
    pub fn block_offset(&self, block_id: u64) -> Option<u64> {
        if !self.contains_block(block_id) {
            return None;
        }

        block_id.checked_mul(self.block_size as u64)
    }

    /// Return the payload length for `block_id`, trimming the final block as needed.
    pub fn block_len(&self, block_id: u64) -> Option<usize> {
        if !self.contains_block(block_id) {
            return None;
        }

        let is_final = Some(block_id) == self.last_block_id();
        if !is_final {
            return Some(self.block_size);
        }

        let tail = (self.total_bytes % self.block_size as u64) as usize;
        if tail == 0 {
            Some(self.block_size)
        } else {
            Some(tail)
        }
    }

    /// Return the absolute byte span for one block.
    pub(crate) fn block_span(&self, block_id: u64) -> Option<BlockSpan> {
        let offset = self.block_offset(block_id)?;
        let len = self.block_len(block_id)?;

        Some(BlockSpan { offset, len })
    }

    /// Derive the source-symbol layout used when FEC mode is enabled.
    pub fn symbol_geometry(&self, symbols_per_block: u32) -> Result<SymbolGeometry, PlanError> {
        SymbolGeometry::new(self.block_size, symbols_per_block)
    }
}

/// Absolute object span for a single block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockSpan {
    offset: u64,
    len: usize,
}

impl BlockSpan {
    /// Return the absolute byte offset of this block within the transfer object.
    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the number of payload bytes stored in this block.
    pub(crate) const fn len(&self) -> usize {
        self.len
    }
}

/// Deterministic source-symbol layout derived from the shared block geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolGeometry {
    symbols_per_block: u32,
    source_symbols: usize,
    symbol_size: usize,
}

impl SymbolGeometry {
    /// Construct source-symbol geometry for one block size and symbol count.
    pub fn new(block_size: usize, symbols_per_block: u32) -> Result<Self, PlanError> {
        if block_size == 0 {
            return Err(PlanError::BlockSizeZero);
        }
        if symbols_per_block == 0 {
            return Err(PlanError::SymbolsPerBlockZero);
        }

        let source_symbols =
            usize::try_from(symbols_per_block).map_err(|_| PlanError::SymbolsPerBlockTooLarge)?;
        let symbol_size = block_size.div_ceil(source_symbols);

        Ok(Self {
            symbols_per_block,
            source_symbols,
            symbol_size,
        })
    }

    /// Return the configured source-symbol count per block.
    pub fn source_symbols(&self) -> usize {
        self.source_symbols
    }

    /// Return the fixed on-the-wire symbol size in bytes.
    pub const fn symbol_size(&self) -> usize {
        self.symbol_size
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockPlan, PlanError, SymbolGeometry};

    #[test]
    fn block_plan_tracks_full_and_final_blocks() {
        let plan = BlockPlan::new(25, 10).expect("valid block plan");

        assert_eq!(plan.total_blocks(), 3);
        assert_eq!(plan.block_offset(0), Some(0));
        assert_eq!(plan.block_offset(1), Some(10));
        assert_eq!(plan.block_offset(2), Some(20));
        assert_eq!(plan.block_len(0), Some(10));
        assert_eq!(plan.block_len(1), Some(10));
        assert_eq!(plan.block_len(2), Some(5));
        assert_eq!(plan.block_len(3), None);
    }

    #[test]
    fn block_plan_handles_empty_and_exact_fit_objects() {
        let empty = BlockPlan::new(0, 8).expect("empty plan should still be valid");
        assert_eq!(empty.total_blocks(), 0);
        assert!(empty.block_span(0).is_none());

        let exact = BlockPlan::new(24, 8).expect("exact fit plan");
        let final_block = exact.block_span(2).expect("final block should exist");
        assert_eq!(final_block.len(), 8);
        assert_eq!(exact.last_block_id(), Some(2));
        assert_eq!(final_block.offset + final_block.len as u64, 24);
    }

    #[test]
    fn symbol_geometry_derives_ceil_symbol_size_and_tail_lengths() {
        let plan = BlockPlan::new(25, 10).expect("valid block plan");
        let symbols = plan.symbol_geometry(4).expect("valid symbol geometry");

        assert_eq!(symbols.symbol_size(), 3);
        let populated = |block_len: usize| {
            block_len
                .div_ceil(symbols.symbol_size())
                .min(symbols.source_symbols())
        };
        let source_symbol_len = |block_len: usize, symbol_id: usize| {
            let offset = symbol_id * symbols.symbol_size();
            block_len.saturating_sub(offset).min(symbols.symbol_size())
        };

        assert_eq!(populated(10), 4);
        assert_eq!(populated(5), 2);

        assert_eq!(source_symbol_len(10, 0), 3);
        assert_eq!(source_symbol_len(10, 1), 3);
        assert_eq!(source_symbol_len(10, 2), 3);
        assert_eq!(source_symbol_len(10, 3), 1);

        assert_eq!(source_symbol_len(5, 0), 3);
        assert_eq!(source_symbol_len(5, 1), 2);
        assert_eq!(source_symbol_len(5, 2), 0);
        assert_eq!(source_symbol_len(5, 3), 0);

        let block = plan.block_span(2).expect("final block should exist");
        assert_eq!(block.offset + symbols.symbol_size() as u64, 23);
    }

    #[test]
    fn symbol_geometry_accepts_large_mettle_scale_k() {
        let block_size = 1_073_741_824usize;
        let symbols = SymbolGeometry::new(block_size, 131_072).expect("large K geometry");

        assert_eq!(symbols.source_symbols(), 131_072);
        assert_eq!(symbols.symbol_size(), 8192);
    }

    #[test]
    fn plan_and_symbol_geometry_reject_zero_dimensions() {
        assert_eq!(BlockPlan::new(4, 0), Err(PlanError::BlockSizeZero));
        assert_eq!(
            SymbolGeometry::new(16, 0),
            Err(PlanError::SymbolsPerBlockZero)
        );
    }
}
