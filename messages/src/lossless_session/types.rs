use serde::{Deserialize, Serialize};

/// Magic constant ("RLM1" ASCII) used by lossless session frames.
pub const LOSSLESS_SESSION_MAGIC: u32 = 0x524C_4D31;
/// Protocol version with METTLE repair-epoch departure checkpoints.
pub const LOSSLESS_SESSION_VERSION: u8 = 10;
/// Maximum number of tree ids representable in a manifest body.
pub const MAX_MANIFEST_TREE_IDS: usize = u8::MAX as usize;

/// Top-level frame kind carried in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionKind {
    BlockData = 1,
    BlockSymbol = 2,
    Control = 3,
}

/// Control sub-kind (only meaningful when kind == Control).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionCtrlKind {
    Manifest = 1,
    Ready = 2,
    SourceDone = 5,
    Need = 6,
    BlockAck = 7,
    AckProbe = 8,
    SessionComplete = 9,
    DepartureCheckpoint = 10,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionModeKind {
    Plain = 1,
    Fec = 2,
}

impl LosslessSessionModeKind {
    #[inline]
    pub fn from_wire(raw: u8) -> Option<Self> {
        match raw {
            x if x == Self::Plain as u8 => Some(Self::Plain),
            x if x == Self::Fec as u8 => Some(Self::Fec),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FecScheme {
    RaptorQ = 1,
    Mettle = 2,
}

/// Receiver-feedback protocol negotiated for an FEC transfer.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FecFeedbackMode {
    /// Barriered `SourceDone`/`Need` rounds (the pre-v8 behavior).
    #[default]
    Rounds = 1,
    /// Continuous cumulative acknowledgement carousel.
    Carousel = 2,
}

impl FecFeedbackMode {
    #[inline]
    pub const fn to_wire(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn from_wire(raw: u8) -> Option<Self> {
        match raw {
            x if x == Self::Rounds as u8 => Some(Self::Rounds),
            x if x == Self::Carousel as u8 => Some(Self::Carousel),
            _ => None,
        }
    }
}

impl FecScheme {
    #[inline]
    pub const fn to_wire(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn from_wire(raw: u8) -> Option<Self> {
        match raw {
            x if x == Self::RaptorQ as u8 => Some(Self::RaptorQ),
            x if x == Self::Mettle as u8 => Some(Self::Mettle),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionFecMode {
    /// Raw wire scheme identifier to preserve clean handling for unknown schemes.
    pub scheme: u8,
    pub symbols_per_block: u32,
    /// Finite-stream coded-rate numerator. `1/1` means no finite prefix expansion.
    pub coded_rate_num: u32,
    /// Finite-stream coded-rate denominator.
    pub coded_rate_den: u32,
    /// Feedback state machine selected for this transfer.
    #[serde(default)]
    pub feedback_mode: FecFeedbackMode,
    /// Present only for paper-native `Carousel + METTLE` object streams.
    #[serde(default)]
    pub mettle_object_stream: Option<MettleObjectStreamGeometry>,
    pub tree_ids: Vec<u16>,
}

/// Maximum source count in one manifest-negotiated METTLE prefix.
pub const METTLE_STREAM_SOURCE_CAP: u32 = 65_536;
/// Maximum source-payload image in one manifest-negotiated METTLE prefix.
pub const METTLE_STREAM_PAYLOAD_CAP_BYTES: u64 = 96 * 1024 * 1024;

/// Deterministic object/prefix geometry for paper-native Carousel + METTLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MettleObjectStreamGeometry {
    pub source_symbol_bytes: u32,
    pub source_symbols_per_stream: u32,
    pub stream_count: u64,
    pub final_stream_source_symbols: u32,
}

impl MettleObjectStreamGeometry {
    #[must_use]
    pub const fn new(
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

    #[must_use]
    pub const fn source_symbol_bytes(self) -> u32 {
        self.source_symbol_bytes
    }

    #[must_use]
    pub const fn source_symbols_per_stream(self) -> u32 {
        self.source_symbols_per_stream
    }

    #[must_use]
    pub const fn stream_count(self) -> u64 {
        self.stream_count
    }

    #[must_use]
    pub const fn final_stream_source_symbols(self) -> u32 {
        self.final_stream_source_symbols
    }

    #[must_use]
    pub fn stream_source_count(self, stream_id: u64) -> Option<u32> {
        if stream_id >= self.stream_count {
            return None;
        }
        if stream_id.checked_add(1) == Some(self.stream_count) {
            Some(self.final_stream_source_symbols)
        } else {
            Some(self.source_symbols_per_stream)
        }
    }
}

impl LosslessSessionFecMode {
    #[must_use]
    pub fn new_raptorq(symbols_per_block: u32, tree_ids: Vec<u16>) -> Self {
        Self {
            scheme: FecScheme::RaptorQ.to_wire(),
            symbols_per_block,
            coded_rate_num: 1,
            coded_rate_den: 1,
            feedback_mode: FecFeedbackMode::Rounds,
            mettle_object_stream: None,
            tree_ids,
        }
    }

    #[must_use]
    pub fn new_mettle(symbols_per_block: u32, tree_ids: Vec<u16>) -> Self {
        Self::new_mettle_with_coded_rate(symbols_per_block, tree_ids, 1, 1)
    }

    #[must_use]
    pub fn new_mettle_with_coded_rate(
        symbols_per_block: u32,
        tree_ids: Vec<u16>,
        coded_rate_num: u32,
        coded_rate_den: u32,
    ) -> Self {
        Self {
            scheme: FecScheme::Mettle.to_wire(),
            symbols_per_block,
            coded_rate_num,
            coded_rate_den,
            feedback_mode: FecFeedbackMode::Rounds,
            mettle_object_stream: None,
            tree_ids,
        }
    }

    #[must_use]
    pub fn with_feedback_mode(mut self, feedback_mode: FecFeedbackMode) -> Self {
        self.feedback_mode = feedback_mode;
        self
    }

    #[must_use]
    pub fn with_mettle_object_stream(mut self, geometry: MettleObjectStreamGeometry) -> Self {
        self.mettle_object_stream = Some(geometry);
        self
    }

    #[inline]
    pub fn scheme_kind(&self) -> Option<FecScheme> {
        FecScheme::from_wire(self.scheme)
    }

    #[inline]
    pub const fn coded_rate_is_valid(&self) -> bool {
        self.coded_rate_den != 0 && self.coded_rate_num >= self.coded_rate_den
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionMode {
    Plain,
    Fec(LosslessSessionFecMode),
}

impl LosslessSessionMode {
    #[inline]
    pub const fn kind(&self) -> LosslessSessionModeKind {
        match self {
            Self::Plain => LosslessSessionModeKind::Plain,
            Self::Fec(_) => LosslessSessionModeKind::Fec,
        }
    }

    #[inline]
    pub const fn is_fec(&self) -> bool {
        matches!(self, Self::Fec(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionManifest {
    pub block_size: u32,
    pub total_bytes: u64,
    pub total_blocks: u64,
    pub mode: LosslessSessionMode,
}

/// Canonical missing-block range used by plain-mode end-of-round feedback.
///
/// `end_block_id` is exclusive, so `[start_block_id, end_block_id)` denotes the
/// missing logical blocks in this range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingBlockRange {
    pub start_block_id: u64,
    pub end_block_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeedBlock {
    pub block_id: u64,
    pub deficit_symbols: u16,
}

/// Canonical half-open interval of completed logical blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedBlockRange {
    pub start_block_id: u64,
    pub end_block_id: u64,
}

/// Canonical half-open range of missing METTLE bin ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingMettleBinRange {
    pub start_bin_id: u32,
    pub end_bin_id: u32,
}

/// Gap evidence tied to one sender departure epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MettleStallEvidence {
    pub repair_epoch: u32,
    pub missing_bin_ranges: Vec<MissingMettleBinRange>,
}

/// Cumulative carousel feedback.
///
/// Variant `1` is a block-completion snapshot. Variant `2` is per-stream
/// METTLE source progress with optional epoch-tagged stall evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BlockAck {
    Blocks {
        /// Every block below this exclusive watermark is complete.
        completed_watermark: u64,
        /// Canonical completed islands strictly above the watermark.
        extra_completed: Vec<CompletedBlockRange>,
    },
    MettleStream {
        /// Deterministic manifest prefix id.
        stream_id: u64,
        /// Next not-yet-released source id within this stream.
        decoded_source_watermark: u32,
        /// Aged missing-bin evidence, absent on ordinary progress heartbeats.
        stalled: Option<MettleStallEvidence>,
    },
}

/// End-of-round receiver feedback emitted after `SourceDone`.
///
/// Empty plain/FEC payloads are canonicalized to `Complete` on encode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NeedReport {
    Complete,
    Plain { ranges: Vec<MissingBlockRange> },
    Fec { blocks: Vec<NeedBlock> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LosslessSessionValidationError {
    ZeroBlockSize,
    InconsistentTotalBlocks {
        expected: u64,
        actual: u64,
    },
    UnknownFecScheme {
        scheme: u8,
    },
    ZeroSymbolsPerBlock,
    FecSymbolPayloadTooLarge {
        symbol_size: u32,
        max: u32,
    },
    FecSymbolPayloadCeilingUnrepresentable,
    FecPaddedBlockSizeOverflow {
        source_symbols: u32,
        symbol_size: u32,
    },
    InvalidFecCodedRate {
        numerator: u32,
        denominator: u32,
    },
    FecTreeIdsEmpty,
    TooManyTreeIds {
        configured: usize,
        max: usize,
    },
    TreeIdsMustBeSortedUnique,
    ZeroDeficitSymbols,
    BlockDataRequiresPlainMode,
    BlockSymbolRequiresFecMode,
    NeedRequiresPlainMode,
    NeedRequiresFecMode,
    BlockIdOutOfRange {
        block_id: u64,
        total_blocks: u64,
    },
    BlockDataLenMismatch {
        block_id: u64,
        expected: u32,
        actual: u32,
    },
    BlockSymbolTreeIdNotAdvertised {
        tree_id: u16,
    },
    NeedRangesEmpty,
    MissingBlockRangeInvalid {
        start_block_id: u64,
        end_block_id: u64,
    },
    NeedRangeOutOfRange {
        end_block_id: u64,
        total_blocks: u64,
    },
    NeedRangesMustBeSortedMerged,
    TooManyNeedRanges {
        configured: usize,
        max: usize,
    },
    NeedBlocksEmpty,
    NeedBlocksMustBeSortedUnique,
    TooManyNeedBlocks {
        configured: usize,
        max: usize,
    },
    BlockAckWatermarkOutOfRange {
        completed_watermark: u64,
        total_blocks: u64,
    },
    CompletedBlockRangeInvalid {
        start_block_id: u64,
        end_block_id: u64,
    },
    BlockAckRangeBeforeWatermark {
        start_block_id: u64,
        completed_watermark: u64,
    },
    BlockAckRangeOutOfRange {
        end_block_id: u64,
        total_blocks: u64,
    },
    BlockAckRangesMustBeSortedMerged,
    TooManyBlockAckRanges {
        configured: usize,
        max: usize,
    },
    MettleObjectStreamGeometryRequired,
    MettleObjectStreamGeometryUnexpected,
    MettleObjectSymbolSizeMismatch {
        expected: u32,
        actual: u32,
    },
    MettleStreamSourceLimitOutOfRange {
        configured: u32,
        max: u32,
    },
    MettleStreamPayloadTooLarge {
        configured: u64,
        max: u64,
    },
    MettleStreamGeometryOverflow,
    MettleStreamCountMismatch {
        expected: u64,
        actual: u64,
    },
    MettleFinalStreamSourceCountMismatch {
        expected: u32,
        actual: u32,
    },
    BlockAckVariantRequiresRaptorQ,
    MettleBlockAckRequiresMettle,
    MettleAckStreamOutOfRange {
        stream_id: u64,
        stream_count: u64,
    },
    MettleAckWatermarkOutOfRange {
        decoded_source_watermark: u32,
        stream_source_count: u32,
    },
    MettleMissingBinRangeInvalid {
        start_bin_id: u32,
        end_bin_id: u32,
    },
    MettleMissingBinRangesMustBeSortedMerged,
    TooManyMettleMissingBinRanges {
        configured: usize,
        max: usize,
    },
    CarouselControlRequiresCarouselMode,
    RoundsControlRequiresRoundsMode,
    DepartureCheckpointRequiresMettleCarousel,
    DepartureCheckpointBinZero,
    DepartureCheckpointStreamOutOfRange {
        stream_id: u64,
        stream_count: u64,
    },
}

pub const MAX_NEED_RANGES: usize = u8::MAX as usize;
pub const MAX_NEED_BLOCKS: usize = u16::MAX as usize;
pub const MAX_BLOCK_ACK_RANGES: usize = MAX_NEED_RANGES;
pub const MAX_METTLE_MISSING_BIN_RANGES: usize = MAX_BLOCK_ACK_RANGES;
pub const BLOCK_ACK_BLOCKS_VARIANT: u8 = 1;
pub const BLOCK_ACK_METTLE_STREAM_VARIANT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionBlockData {
    pub block_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionBlockSymbol {
    pub block_id: u64,
    pub symbol_id: u32,
    pub tree_id: u16,
}

/// CONTROL payload variants (follows `LosslessSessionHeader` when kind == Control).
///
/// Rounds use `SourceDone`/`Need`; carousel uses cumulative `BlockAck`,
/// `AckProbe`, and one-way `SessionComplete` controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionControl {
    Manifest {
        manifest: LosslessSessionManifest,
    },
    Ready,
    SourceDone {
        round_id: u32,
    },
    Need {
        round_id: u32,
        report: NeedReport,
    },
    BlockAck {
        ack: BlockAck,
    },
    AckProbe {
        target_peer_id: u64,
    },
    SessionComplete,
    DepartureCheckpoint {
        stream_id: u64,
        repair_epoch: u32,
        departure_bin_exclusive: u32,
    },
}
