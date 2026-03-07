use std::fmt::{Display, Formatter};
use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockPlanError {
    ZeroBlockSize,
    ZeroSymbolsPerBlock,
    BlockOutOfRange { block_id: u64, total_blocks: u64 },
    SymbolOutOfRange { symbol_id: u16, symbols_per_block: u16 },
}

impl Display for BlockPlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBlockSize => write!(f, "block_size must be >= 1"),
            Self::ZeroSymbolsPerBlock => write!(f, "symbols_per_block must be >= 1"),
            Self::BlockOutOfRange {
                block_id,
                total_blocks,
            } => write!(
                f,
                "block_id {block_id} is out of range for total_blocks={total_blocks}"
            ),
            Self::SymbolOutOfRange {
                symbol_id,
                symbols_per_block,
            } => write!(
                f,
                "symbol_id {symbol_id} is out of range for symbols_per_block={symbols_per_block}"
            ),
        }
    }
}

impl std::error::Error for BlockPlanError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockDescriptor {
    pub block_id: u64,
    pub offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolDescriptor {
    pub block_id: u64,
    pub symbol_id: u16,
    pub offset_within_block: usize,
    pub len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockLayout {
    total_bytes: u64,
    block_size: usize,
    total_blocks: u64,
}

impl BlockLayout {
    pub fn new(total_bytes: u64, block_size: usize) -> Result<Self, BlockPlanError> {
        if block_size == 0 {
            return Err(BlockPlanError::ZeroBlockSize);
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

    pub fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    pub fn block_size(self) -> usize {
        self.block_size
    }

    pub fn total_blocks(self) -> u64 {
        self.total_blocks
    }

    pub fn is_empty(self) -> bool {
        self.total_blocks == 0
    }

    pub fn block_offset(self, block_id: u64) -> Result<u64, BlockPlanError> {
        self.ensure_block(block_id)?;
        Ok(block_id * self.block_size as u64)
    }

    pub fn block_len(self, block_id: u64) -> Result<usize, BlockPlanError> {
        self.ensure_block(block_id)?;
        if block_id + 1 < self.total_blocks {
            return Ok(self.block_size);
        }

        let used = block_id * self.block_size as u64;
        Ok((self.total_bytes - used) as usize)
    }

    pub fn block_range(self, block_id: u64) -> Result<Range<u64>, BlockPlanError> {
        let offset = self.block_offset(block_id)?;
        let len = self.block_len(block_id)? as u64;
        Ok(offset..offset + len)
    }

    pub fn block_descriptor(self, block_id: u64) -> Result<BlockDescriptor, BlockPlanError> {
        Ok(BlockDescriptor {
            block_id,
            offset: self.block_offset(block_id)?,
            len: self.block_len(block_id)?,
        })
    }

    pub fn blocks(self) -> impl Iterator<Item = BlockDescriptor> {
        let total_blocks = self.total_blocks;
        (0..total_blocks).map(move |block_id| BlockDescriptor {
            block_id,
            offset: block_id * self.block_size as u64,
            len: self.block_len(block_id).expect("validated block id"),
        })
    }

    fn ensure_block(self, block_id: u64) -> Result<(), BlockPlanError> {
        if block_id >= self.total_blocks {
            Err(BlockPlanError::BlockOutOfRange {
                block_id,
                total_blocks: self.total_blocks,
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FecSymbolLayout {
    symbols_per_block: u16,
}

impl FecSymbolLayout {
    pub fn new(symbols_per_block: u16) -> Result<Self, BlockPlanError> {
        if symbols_per_block == 0 {
            return Err(BlockPlanError::ZeroSymbolsPerBlock);
        }
        Ok(Self { symbols_per_block })
    }

    pub fn symbols_per_block(self) -> u16 {
        self.symbols_per_block
    }

    pub fn symbol_size_for_block(self, block_len: usize) -> usize {
        if block_len == 0 {
            return 0;
        }
        block_len.div_ceil(self.symbols_per_block as usize)
    }

    pub fn symbol_range_for_block(
        self,
        block_len: usize,
        symbol_id: u16,
    ) -> Result<Range<usize>, BlockPlanError> {
        if symbol_id >= self.symbols_per_block {
            return Err(BlockPlanError::SymbolOutOfRange {
                symbol_id,
                symbols_per_block: self.symbols_per_block,
            });
        }

        let symbol_size = self.symbol_size_for_block(block_len);
        let start = symbol_id as usize * symbol_size;
        let len = block_len.saturating_sub(start).min(symbol_size);
        Ok(start..start + len)
    }

    pub fn symbol_descriptor(
        self,
        blocks: BlockLayout,
        block_id: u64,
        symbol_id: u16,
    ) -> Result<SymbolDescriptor, BlockPlanError> {
        let block_len = blocks.block_len(block_id)?;
        let range = self.symbol_range_for_block(block_len, symbol_id)?;
        Ok(SymbolDescriptor {
            block_id,
            symbol_id,
            offset_within_block: range.start,
            len: range.end - range.start,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockLayout, BlockPlanError, FecSymbolLayout};

    #[test]
    fn computes_block_offsets_and_final_block_length() {
        let layout = BlockLayout::new(25, 8).expect("layout");

        assert_eq!(layout.total_blocks(), 4);
        assert_eq!(layout.block_offset(0).expect("offset"), 0);
        assert_eq!(layout.block_offset(3).expect("offset"), 24);
        assert_eq!(layout.block_len(0).expect("len"), 8);
        assert_eq!(layout.block_len(3).expect("len"), 1);
        assert_eq!(layout.block_range(2).expect("range"), 16..24);
    }

    #[test]
    fn iterates_block_descriptors() {
        let layout = BlockLayout::new(17, 8).expect("layout");
        let blocks = layout.blocks().collect::<Vec<_>>();

        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].offset, 0);
        assert_eq!(blocks[1].offset, 8);
        assert_eq!(blocks[2].len, 1);
    }

    #[test]
    fn rejects_zero_block_size() {
        assert_eq!(
            BlockLayout::new(100, 0).expect_err("zero block size should fail"),
            BlockPlanError::ZeroBlockSize
        );
    }

    #[test]
    fn derives_symbol_ranges_from_block_length() {
        let layout = BlockLayout::new(25, 8).expect("layout");
        let symbols = FecSymbolLayout::new(3).expect("symbols");

        assert_eq!(symbols.symbol_size_for_block(8), 3);
        assert_eq!(
            symbols.symbol_descriptor(layout, 0, 0).expect("symbol").len,
            3
        );
        assert_eq!(
            symbols.symbol_descriptor(layout, 0, 1).expect("symbol").len,
            3
        );
        assert_eq!(
            symbols.symbol_descriptor(layout, 0, 2).expect("symbol").len,
            2
        );
        assert_eq!(
            symbols.symbol_descriptor(layout, 3, 0).expect("symbol").len,
            1
        );
        assert_eq!(
            symbols.symbol_descriptor(layout, 3, 1).expect("symbol").len,
            0
        );
    }

    #[test]
    fn rejects_zero_symbols_per_block() {
        assert_eq!(
            FecSymbolLayout::new(0).expect_err("zero symbols should fail"),
            BlockPlanError::ZeroSymbolsPerBlock
        );
    }
}
