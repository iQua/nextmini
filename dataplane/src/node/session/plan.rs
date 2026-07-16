//! Shared block and symbol geometry for block-first lossless sessions.

use std::error::Error;
use std::fmt::{Display, Formatter};

use nextmini_messages::lossless_session::{WireFecGeometry, WireFecGeometryError};

/// Construction or derivation failures for shared block geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    BlockSizeZero,
    BlockSizeTooLarge,
    #[allow(dead_code)] // Public library geometry API; the binary embeds this module separately.
    SymbolsPerBlockZero,
    SymbolsPerBlockTooLarge,
    SymbolPayloadTooLarge,
    PaddedBlockSizeTooLarge,
    #[allow(dead_code)]
    ObjectSymbolSizeZero,
    #[allow(dead_code)]
    ObjectSymbolSizeTooLarge,
    #[allow(dead_code)]
    StreamSourceLimitZero,
    #[allow(dead_code)]
    StreamSourceLimitTooLarge,
    #[allow(dead_code)]
    StreamPayloadTooLarge,
    #[allow(dead_code)]
    InvalidStreamCount,
    #[allow(dead_code)]
    InvalidFinalStreamSourceCount,
    #[allow(dead_code)]
    ObjectGeometryOverflow,
}

impl Display for PlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockSizeZero => write!(f, "block_size must be >= 1"),
            Self::BlockSizeTooLarge => write!(f, "block_size does not fit the wire geometry"),
            Self::SymbolsPerBlockZero => write!(f, "symbols_per_block must be >= 1"),
            Self::SymbolsPerBlockTooLarge => write!(f, "symbols_per_block does not fit usize"),
            Self::SymbolPayloadTooLarge => {
                write!(f, "symbol payload exceeds the lossless packet envelope")
            }
            Self::PaddedBlockSizeTooLarge => {
                write!(f, "padded FEC block does not fit the local address space")
            }
            Self::ObjectSymbolSizeZero => write!(f, "object symbol size must be >= 1"),
            Self::ObjectSymbolSizeTooLarge => {
                write!(f, "object symbol size does not fit the wire geometry")
            }
            Self::StreamSourceLimitZero => {
                write!(f, "METTLE stream source limit must be >= 1")
            }
            Self::StreamSourceLimitTooLarge => write!(
                f,
                "METTLE stream source limit exceeds the checked source-count cap"
            ),
            Self::StreamPayloadTooLarge => write!(
                f,
                "METTLE stream source payload exceeds the checked byte cap"
            ),
            Self::InvalidStreamCount => {
                write!(f, "negotiated METTLE stream count is inconsistent")
            }
            Self::InvalidFinalStreamSourceCount => write!(
                f,
                "negotiated final METTLE stream source count is inconsistent"
            ),
            Self::ObjectGeometryOverflow => write!(f, "object symbol geometry overflowed"),
        }
    }
}

impl Error for PlanError {}

/// Maximum number of source symbols in one negotiated METTLE prefix.
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) const METTLE_STREAM_SOURCE_CAP: u32 = 65_536;
/// Maximum source-payload image represented by one negotiated METTLE prefix.
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) const METTLE_STREAM_PAYLOAD_CAP_BYTES: u64 = 96 * 1024 * 1024;

/// Manifest-negotiated geometry for paper-native object-stream METTLE.
///
/// The sender derives this geometry once. Receivers validate the exact values
/// instead of making a local prefix-size decision after the READY handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) struct ObjectStreamGeometry {
    source_symbol_bytes: u32,
    source_symbols_per_stream: u32,
    stream_count: u64,
    final_stream_source_symbols: u32,
}

#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
impl ObjectStreamGeometry {
    pub(crate) const fn new(
        source_symbol_bytes: u32,
        source_symbols_per_stream: u32,
        stream_count: u64,
        final_stream_source_symbols: u32,
    ) -> Self {
        Self {
            source_symbol_bytes,
            source_symbols_per_stream,
            stream_count,
            final_stream_source_symbols,
        }
    }

    pub(crate) const fn source_symbol_bytes(self) -> u32 {
        self.source_symbol_bytes
    }

    pub(crate) const fn source_symbols_per_stream(self) -> u32 {
        self.source_symbols_per_stream
    }

    pub(crate) const fn stream_count(self) -> u64 {
        self.stream_count
    }

    pub(crate) const fn final_stream_source_symbols(self) -> u32 {
        self.final_stream_source_symbols
    }
}

/// Absolute object span represented by one source symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) struct ObjectSymbolSpan {
    offset: u64,
    len: usize,
}

#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
impl ObjectSymbolSpan {
    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn len(self) -> usize {
        self.len
    }
}

/// One global source id expressed in the negotiated prefix namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) struct StreamSourceId {
    stream_id: u64,
    source_id: u32,
}

#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
impl StreamSourceId {
    pub(crate) const fn stream_id(self) -> u64 {
        self.stream_id
    }

    pub(crate) const fn source_id(self) -> u32 {
        self.source_id
    }
}

/// Global source segmentation for paper-native Carousel + METTLE.
///
/// Unlike [`BlockPlan`], this plan has no independently padded blocks. The
/// object is one source-symbol sequence and only its final source is padded.
/// Large objects are partitioned into deterministic, sequential decoder
/// prefixes whose geometry is carried by the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
pub(crate) struct ObjectSymbolPlan {
    total_bytes: u64,
    total_sources: u64,
    symbol_size: usize,
    symbol_size_u64: u64,
    geometry: ObjectStreamGeometry,
}

#[allow(dead_code)] // Stage 2.1 API is consumed by the Stage 2.2/2.3 protocol slice.
impl ObjectSymbolPlan {
    /// Derive the canonical largest legal prefix geometry for an object.
    pub(crate) fn derive(total_bytes: u64, symbol_size: u32) -> Result<Self, PlanError> {
        if symbol_size == 0 {
            return Err(PlanError::ObjectSymbolSizeZero);
        }
        let byte_limited_sources = METTLE_STREAM_PAYLOAD_CAP_BYTES / u64::from(symbol_size);
        let source_symbols_per_stream =
            u32::try_from(byte_limited_sources.min(u64::from(METTLE_STREAM_SOURCE_CAP)))
                .map_err(|_| PlanError::ObjectGeometryOverflow)?;
        if source_symbols_per_stream == 0 {
            return Err(PlanError::StreamPayloadTooLarge);
        }

        let symbol_size_u64 = u64::from(symbol_size);
        let total_sources = if total_bytes == 0 {
            0
        } else {
            total_bytes.div_ceil(symbol_size_u64)
        };
        let source_limit_u64 = u64::from(source_symbols_per_stream);
        let stream_count = if total_sources == 0 {
            0
        } else {
            total_sources.div_ceil(source_limit_u64)
        };
        let final_stream_source_symbols = if total_sources == 0 {
            0
        } else {
            let remainder = total_sources % source_limit_u64;
            u32::try_from(if remainder == 0 {
                source_limit_u64
            } else {
                remainder
            })
            .map_err(|_| PlanError::ObjectGeometryOverflow)?
        };

        Self::from_negotiated(
            total_bytes,
            ObjectStreamGeometry::new(
                symbol_size,
                source_symbols_per_stream,
                stream_count,
                final_stream_source_symbols,
            ),
        )
    }

    /// Validate and install geometry received in a manifest.
    pub(crate) fn from_negotiated(
        total_bytes: u64,
        geometry: ObjectStreamGeometry,
    ) -> Result<Self, PlanError> {
        let symbol_size_u32 = geometry.source_symbol_bytes();
        if symbol_size_u32 == 0 {
            return Err(PlanError::ObjectSymbolSizeZero);
        }
        let symbol_size =
            usize::try_from(symbol_size_u32).map_err(|_| PlanError::ObjectSymbolSizeTooLarge)?;
        let symbol_size_u64 = u64::from(symbol_size_u32);
        let source_limit = geometry.source_symbols_per_stream();
        if source_limit == 0 {
            return Err(PlanError::StreamSourceLimitZero);
        }
        if source_limit > METTLE_STREAM_SOURCE_CAP {
            return Err(PlanError::StreamSourceLimitTooLarge);
        }
        let stream_payload_bytes = u64::from(source_limit)
            .checked_mul(symbol_size_u64)
            .ok_or(PlanError::ObjectGeometryOverflow)?;
        if stream_payload_bytes > METTLE_STREAM_PAYLOAD_CAP_BYTES {
            return Err(PlanError::StreamPayloadTooLarge);
        }

        let total_sources = if total_bytes == 0 {
            0
        } else {
            total_bytes.div_ceil(symbol_size_u64)
        };
        let source_limit_u64 = u64::from(source_limit);
        let expected_stream_count = if total_sources == 0 {
            0
        } else {
            total_sources.div_ceil(source_limit_u64)
        };
        if geometry.stream_count() != expected_stream_count {
            return Err(PlanError::InvalidStreamCount);
        }
        let expected_final_sources = if total_sources == 0 {
            0
        } else {
            let remainder = total_sources % source_limit_u64;
            u32::try_from(if remainder == 0 {
                source_limit_u64
            } else {
                remainder
            })
            .map_err(|_| PlanError::ObjectGeometryOverflow)?
        };
        if geometry.final_stream_source_symbols() != expected_final_sources {
            return Err(PlanError::InvalidFinalStreamSourceCount);
        }

        Ok(Self {
            total_bytes,
            total_sources,
            symbol_size,
            symbol_size_u64,
            geometry,
        })
    }

    pub(crate) const fn geometry(self) -> ObjectStreamGeometry {
        self.geometry
    }

    pub(crate) const fn total_sources(self) -> u64 {
        self.total_sources
    }

    pub(crate) const fn symbol_size(self) -> usize {
        self.symbol_size
    }

    pub(crate) const fn stream_count(self) -> u64 {
        self.geometry.stream_count()
    }

    pub(crate) fn stream_source_count(self, stream_id: u64) -> Option<u32> {
        if stream_id >= self.stream_count() {
            return None;
        }
        if stream_id.checked_add(1) == Some(self.stream_count()) {
            Some(self.geometry.final_stream_source_symbols())
        } else {
            Some(self.geometry.source_symbols_per_stream())
        }
    }

    pub(crate) fn stream_first_source_id(self, stream_id: u64) -> Option<u64> {
        if stream_id >= self.stream_count() {
            return None;
        }
        stream_id.checked_mul(u64::from(self.geometry.source_symbols_per_stream()))
    }

    pub(crate) fn global_source_id(self, stream_id: u64, source_id: u32) -> Option<u64> {
        if source_id >= self.stream_source_count(stream_id)? {
            return None;
        }
        self.stream_first_source_id(stream_id)?
            .checked_add(u64::from(source_id))
            .filter(|&global_id| global_id < self.total_sources)
    }

    pub(crate) fn stream_source_id(self, global_source_id: u64) -> Option<StreamSourceId> {
        if global_source_id >= self.total_sources {
            return None;
        }
        let source_limit = u64::from(self.geometry.source_symbols_per_stream());
        let stream_id = global_source_id / source_limit;
        let source_id = u32::try_from(global_source_id % source_limit).ok()?;
        Some(StreamSourceId {
            stream_id,
            source_id,
        })
    }

    pub(crate) fn source_span(self, global_source_id: u64) -> Option<ObjectSymbolSpan> {
        if global_source_id >= self.total_sources {
            return None;
        }
        let offset = global_source_id.checked_mul(self.symbol_size_u64)?;
        let remaining = self.total_bytes.checked_sub(offset)?;
        let len = usize::try_from(remaining.min(self.symbol_size_u64)).ok()?;
        Some(ObjectSymbolSpan { offset, len })
    }
}

/// Immutable geometry for a block-first session object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockPlan {
    total_bytes: u64,
    block_size: usize,
    block_size_u64: u64,
    total_blocks: u64,
}

impl BlockPlan {
    /// Derive block geometry for a transfer of `total_bytes` using `block_size`.
    pub fn new(total_bytes: u64, block_size: usize) -> Result<Self, PlanError> {
        if block_size == 0 {
            return Err(PlanError::BlockSizeZero);
        }
        let block_size_u64 = u64::try_from(block_size).map_err(|_| PlanError::BlockSizeTooLarge)?;

        let total_blocks = if total_bytes == 0 {
            0
        } else {
            total_bytes.div_ceil(block_size_u64)
        };

        Ok(Self {
            total_bytes,
            block_size,
            block_size_u64,
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

        block_id.checked_mul(self.block_size_u64)
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

        let tail = usize::try_from(self.total_bytes % self.block_size_u64).ok()?;
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
    #[allow(dead_code)] // Public library geometry API; production uses codec-validated wire geometry.
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
    padded_block_size: usize,
}

impl SymbolGeometry {
    /// Construct source-symbol geometry for one block size and symbol count.
    #[allow(dead_code)] // Public library geometry API; production uses `from_wire` after validation.
    pub fn new(block_size: usize, symbols_per_block: u32) -> Result<Self, PlanError> {
        let block_size = u32::try_from(block_size).map_err(|_| PlanError::BlockSizeTooLarge)?;
        let wire =
            WireFecGeometry::new(block_size, symbols_per_block).map_err(|err| match err {
                WireFecGeometryError::ZeroBlockSize => PlanError::BlockSizeZero,
                WireFecGeometryError::ZeroSourceSymbols => PlanError::SymbolsPerBlockZero,
                WireFecGeometryError::SymbolPayloadTooLarge { .. }
                | WireFecGeometryError::SymbolPayloadCeilingUnrepresentable => {
                    PlanError::SymbolPayloadTooLarge
                }
                WireFecGeometryError::PaddedBlockSizeOverflow { .. } => {
                    PlanError::PaddedBlockSizeTooLarge
                }
            })?;
        Self::from_wire(wire)
    }

    /// Convert validated wire geometry into host-sized indexes.
    pub fn from_wire(wire: WireFecGeometry) -> Result<Self, PlanError> {
        let source_symbols = usize::try_from(wire.source_symbols())
            .map_err(|_| PlanError::SymbolsPerBlockTooLarge)?;
        let symbol_size =
            usize::try_from(wire.symbol_size()).map_err(|_| PlanError::SymbolPayloadTooLarge)?;
        let padded_block_size = usize::try_from(wire.padded_block_size())
            .map_err(|_| PlanError::PaddedBlockSizeTooLarge)?;

        Ok(Self {
            symbols_per_block: wire.source_symbols(),
            source_symbols,
            symbol_size,
            padded_block_size,
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

    /// Return the checked host-sized padded block length (`K*T`).
    pub const fn padded_block_size(&self) -> usize {
        self.padded_block_size
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockPlan, METTLE_STREAM_PAYLOAD_CAP_BYTES, METTLE_STREAM_SOURCE_CAP, ObjectStreamGeometry,
        ObjectSymbolPlan, PlanError, SymbolGeometry,
    };

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
        assert_eq!(symbols.padded_block_size(), 12);
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

    #[test]
    fn host_usize_conversion_boundaries_are_checked() {
        let maximum_object = BlockPlan::new(u64::MAX, 1).expect("valid block plan");
        let wire = nextmini_messages::lossless_session::WireFecGeometry::new(u32::MAX, 100_000)
            .expect("valid large wire geometry");

        if usize::BITS < u64::BITS {
            assert!(maximum_object.total_bytes_usize().is_none());
            assert_eq!(
                SymbolGeometry::from_wire(wire),
                Err(PlanError::PaddedBlockSizeTooLarge)
            );
        } else {
            assert_eq!(maximum_object.total_bytes_usize(), Some(usize::MAX));
            assert_eq!(
                SymbolGeometry::from_wire(wire)
                    .expect("64-bit host should hold padded geometry")
                    .padded_block_size(),
                4_295_000_000
            );
        }
    }

    #[test]
    fn object_symbol_plan_maps_global_sources_and_prefixes() {
        let plan = ObjectSymbolPlan::from_negotiated(25, ObjectStreamGeometry::new(4, 3, 3, 1))
            .expect("valid negotiated object plan");

        assert_eq!(plan.total_sources(), 7);
        assert_eq!(plan.stream_count(), 3);
        assert_eq!(plan.stream_source_count(0), Some(3));
        assert_eq!(plan.stream_source_count(1), Some(3));
        assert_eq!(plan.stream_source_count(2), Some(1));
        assert_eq!(plan.stream_source_count(3), None);
        assert_eq!(plan.global_source_id(1, 2), Some(5));
        assert_eq!(plan.global_source_id(2, 1), None);

        let mapped = plan.stream_source_id(6).expect("last source maps");
        assert_eq!(mapped.stream_id(), 2);
        assert_eq!(mapped.source_id(), 0);
        assert_eq!(
            plan.source_span(0).map(|span| (span.offset(), span.len())),
            Some((0, 4))
        );
        assert_eq!(
            plan.source_span(6).map(|span| (span.offset(), span.len())),
            Some((24, 1))
        );
        assert_eq!(plan.source_span(7), None);
    }

    #[test]
    fn object_symbol_plan_derives_checked_prefix_caps() {
        let at_source_cap =
            ObjectSymbolPlan::derive(u64::from(METTLE_STREAM_SOURCE_CAP) * 1400, 1400)
                .expect("source cap is valid");
        assert_eq!(
            at_source_cap.geometry().source_symbols_per_stream(),
            METTLE_STREAM_SOURCE_CAP
        );
        assert_eq!(at_source_cap.stream_count(), 1);

        let large_symbols = ObjectSymbolPlan::derive(METTLE_STREAM_PAYLOAD_CAP_BYTES + 1, 65_000)
            .expect("byte cap reduces the source limit");
        let geometry = large_symbols.geometry();
        assert!(geometry.source_symbols_per_stream() < METTLE_STREAM_SOURCE_CAP);
        assert!(
            u64::from(geometry.source_symbols_per_stream())
                * u64::from(geometry.source_symbol_bytes())
                <= METTLE_STREAM_PAYLOAD_CAP_BYTES
        );
        assert_eq!(large_symbols.stream_count(), 2);
    }

    #[test]
    fn object_symbol_plan_validates_empty_exact_and_partial_objects() {
        let empty = ObjectSymbolPlan::derive(0, 1400).expect("empty object geometry");
        assert_eq!(empty.total_sources(), 0);
        assert_eq!(empty.stream_count(), 0);
        assert_eq!(empty.geometry().final_stream_source_symbols(), 0);

        let exact = ObjectSymbolPlan::from_negotiated(24, ObjectStreamGeometry::new(4, 3, 2, 3))
            .expect("exact final prefix");
        assert_eq!(exact.source_span(5).map(|span| span.len()), Some(4));

        assert_eq!(
            ObjectSymbolPlan::from_negotiated(25, ObjectStreamGeometry::new(4, 3, 2, 1),),
            Err(PlanError::InvalidStreamCount)
        );
        assert_eq!(
            ObjectSymbolPlan::from_negotiated(25, ObjectStreamGeometry::new(4, 3, 3, 2),),
            Err(PlanError::InvalidFinalStreamSourceCount)
        );
    }

    #[test]
    fn object_symbol_mapping_roundtrips_across_prefix_boundaries() {
        let plan =
            ObjectSymbolPlan::from_negotiated(40_003, ObjectStreamGeometry::new(7, 31, 185, 11))
                .expect("valid multi-prefix plan");

        for global_source_id in 0..plan.total_sources() {
            let local = plan
                .stream_source_id(global_source_id)
                .expect("global source maps to a prefix");
            assert_eq!(
                plan.global_source_id(local.stream_id(), local.source_id()),
                Some(global_source_id)
            );
            let span = plan.source_span(global_source_id).expect("source span");
            assert!(span.len() > 0);
            assert!(span.len() <= plan.symbol_size());
        }
        let final_span = plan
            .source_span(plan.total_sources() - 1)
            .expect("final source span");
        assert_eq!(final_span.offset() + final_span.len() as u64, 40_003);
    }
}
