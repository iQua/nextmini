use std::error::Error;
use std::fmt::{Display, Formatter};

/// Construction or derivation failures for shared block geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    BlockSizeZero,
    SymbolsPerBlockZero,
}

impl Display for PlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockSizeZero => write!(f, "block_size must be >= 1"),
            Self::SymbolsPerBlockZero => write!(f, "symbols_per_block must be >= 1"),
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

    #[cfg(test)]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    #[cfg(test)]
    pub const fn block_size(&self) -> usize {
        self.block_size
    }

    pub const fn total_blocks(&self) -> u64 {
        self.total_blocks
    }

    #[cfg(test)]
    pub const fn is_empty(&self) -> bool {
        self.total_blocks == 0
    }

    pub fn contains_block(&self, block_id: u64) -> bool {
        block_id < self.total_blocks
    }

    pub fn last_block_id(&self) -> Option<u64> {
        self.total_blocks.checked_sub(1)
    }

    pub fn block_offset(&self, block_id: u64) -> Option<u64> {
        if !self.contains_block(block_id) {
            return None;
        }

        block_id.checked_mul(self.block_size as u64)
    }

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

    pub fn block_span(&self, block_id: u64) -> Option<BlockSpan> {
        let offset = self.block_offset(block_id)?;
        let len = self.block_len(block_id)?;

        Some(BlockSpan {
            block_id,
            offset,
            len,
            is_final: Some(block_id) == self.last_block_id(),
        })
    }

    pub fn symbol_geometry(&self, symbols_per_block: u16) -> Result<SymbolGeometry, PlanError> {
        SymbolGeometry::new(self.block_size, symbols_per_block)
    }
}

/// Absolute object span for a single block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSpan {
    block_id: u64,
    offset: u64,
    len: usize,
    is_final: bool,
}

impl BlockSpan {
    #[cfg(test)]
    pub const fn block_id(&self) -> u64 {
        self.block_id
    }

    pub const fn offset(&self) -> u64 {
        self.offset
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub const fn is_final(&self) -> bool {
        self.is_final
    }

    #[cfg(test)]
    pub fn end_offset(&self) -> u64 {
        self.offset + self.len as u64
    }
}

/// Deterministic source-symbol layout derived from the shared block geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolGeometry {
    symbols_per_block: u16,
    symbol_size: usize,
}

impl SymbolGeometry {
    pub fn new(block_size: usize, symbols_per_block: u16) -> Result<Self, PlanError> {
        if block_size == 0 {
            return Err(PlanError::BlockSizeZero);
        }
        if symbols_per_block == 0 {
            return Err(PlanError::SymbolsPerBlockZero);
        }

        let source_symbols = usize::from(symbols_per_block);
        let symbol_size = block_size.div_ceil(source_symbols);

        Ok(Self {
            symbols_per_block,
            symbol_size,
        })
    }

    #[cfg(test)]
    pub const fn symbols_per_block(&self) -> u16 {
        self.symbols_per_block
    }

    pub fn source_symbols(&self) -> usize {
        usize::from(self.symbols_per_block)
    }

    pub const fn symbol_size(&self) -> usize {
        self.symbol_size
    }

    #[cfg(test)]
    pub fn populated_source_symbols(&self, block_len: usize) -> usize {
        if block_len == 0 {
            0
        } else {
            block_len
                .div_ceil(self.symbol_size)
                .min(self.source_symbols())
        }
    }

    #[cfg(test)]
    pub fn source_symbol_offset(&self, symbol_id: u32) -> Option<usize> {
        let symbol_id = usize::try_from(symbol_id).ok()?;
        if symbol_id >= self.source_symbols() {
            return None;
        }

        Some(symbol_id * self.symbol_size)
    }

    #[cfg(test)]
    pub fn source_symbol_len(&self, block_len: usize, symbol_id: u32) -> Option<usize> {
        let offset = self.source_symbol_offset(symbol_id)?;
        Some(block_len.saturating_sub(offset).min(self.symbol_size))
    }

    #[cfg(test)]
    pub fn source_symbol_absolute_offset(
        &self,
        plan: &BlockPlan,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<u64> {
        let block = plan.block_span(block_id)?;
        Some(block.offset() + self.source_symbol_offset(symbol_id)? as u64)
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
        assert!(final_block.is_final());
        assert_eq!(final_block.end_offset(), 24);
    }

    #[test]
    fn symbol_geometry_derives_ceil_symbol_size_and_tail_lengths() {
        let plan = BlockPlan::new(25, 10).expect("valid block plan");
        let symbols = plan.symbol_geometry(4).expect("valid symbol geometry");

        assert_eq!(symbols.symbol_size(), 3);
        assert_eq!(symbols.populated_source_symbols(10), 4);
        assert_eq!(symbols.populated_source_symbols(5), 2);

        assert_eq!(symbols.source_symbol_len(10, 0), Some(3));
        assert_eq!(symbols.source_symbol_len(10, 1), Some(3));
        assert_eq!(symbols.source_symbol_len(10, 2), Some(3));
        assert_eq!(symbols.source_symbol_len(10, 3), Some(1));

        assert_eq!(symbols.source_symbol_len(5, 0), Some(3));
        assert_eq!(symbols.source_symbol_len(5, 1), Some(2));
        assert_eq!(symbols.source_symbol_len(5, 2), Some(0));
        assert_eq!(symbols.source_symbol_len(5, 3), Some(0));

        assert_eq!(symbols.source_symbol_absolute_offset(&plan, 2, 1), Some(23));
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
