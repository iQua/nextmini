use serde::{Deserialize, Serialize};

/// Magic constant ("RLM1" ASCII) used by lossless session frames.
pub const LOSSLESS_SESSION_MAGIC: u32 = 0x524C_4D31;
/// Single cutover protocol version for the block-first wire model.
pub const LOSSLESS_SESSION_VERSION: u8 = 6;
/// Maximum number of tree ids representable in a manifest body.
pub const MAX_MANIFEST_TREE_IDS: usize = u8::MAX as usize;

/// Top-level frame kind carried in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionKind {
    BlockData = 1,
    BlockSymbol = 2,
    Control = 3,
    MettleSymbol = 4,
}

/// Control sub-kind (only meaningful when kind == Control).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionCtrlKind {
    Manifest = 1,
    Ready = 2,
    SourceDone = 5,
    Need = 6,
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
    MettleV1 = 2,
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
            x if x == Self::MettleV1 as u8 => Some(Self::MettleV1),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionFecMode {
    /// Raw wire scheme identifier to preserve clean handling for unknown schemes.
    pub scheme: u8,
    pub symbols_per_block: u16,
    pub coded_rate_numerator: u16,
    pub coded_rate_denominator: u16,
    pub seed: u64,
    pub tree_ids: Vec<u16>,
}

impl LosslessSessionFecMode {
    #[must_use]
    pub fn new_raptorq(symbols_per_block: u16, tree_ids: Vec<u16>) -> Self {
        Self {
            scheme: FecScheme::RaptorQ.to_wire(),
            symbols_per_block,
            coded_rate_numerator: 0,
            coded_rate_denominator: 0,
            seed: 0,
            tree_ids,
        }
    }

    #[must_use]
    pub fn new_mettle(
        coded_rate_numerator: u16,
        coded_rate_denominator: u16,
        seed: u64,
        tree_ids: Vec<u16>,
    ) -> Self {
        Self {
            scheme: FecScheme::MettleV1.to_wire(),
            symbols_per_block: 0,
            coded_rate_numerator,
            coded_rate_denominator,
            seed,
            tree_ids,
        }
    }

    #[inline]
    pub fn scheme_kind(&self) -> Option<FecScheme> {
        FecScheme::from_wire(self.scheme)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MettleReplayWindow {
    pub stalled_source_id: u64,
    pub replay_start_bin_id: u64,
    pub replay_end_bin_id: u64,
}

/// Receiver feedback for lossless convergence and completion.
///
/// Plain and block-FEC modes primarily emit this after `SourceDone`. METTLE
/// may emit the same control live while peeling stalls or completes before the
/// tail marker arrives. Empty plain/FEC payloads are canonicalized to
/// `Complete` on encode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NeedReport {
    Complete,
    Plain { ranges: Vec<MissingBlockRange> },
    Fec { blocks: Vec<NeedBlock> },
    Mettle { window: MettleReplayWindow },
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
    MettleSymbolsPerBlockMustBeZero {
        value: u16,
    },
    ZeroMettleRateDenominator,
    SubUnitMettleCodedRate {
        numerator: u16,
        denominator: u16,
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
    BlockSymbolRequiresRaptorQMode,
    MettleSymbolRequiresMettleMode,
    NeedRequiresPlainMode,
    NeedRequiresFecMode,
    NeedRequiresMettleMode,
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
    MettleSymbolTreeIdNotAdvertised {
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
    MettleReplayWindowInvalid {
        replay_start_bin_id: u64,
        replay_end_bin_id: u64,
    },
    MettleReplaySourceOutOfRange {
        stalled_source_id: u64,
        total_blocks: u64,
    },
}

pub const MAX_NEED_RANGES: usize = u8::MAX as usize;
pub const MAX_NEED_BLOCKS: usize = u16::MAX as usize;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionMettleSymbol {
    pub bin_id: u64,
    pub degree: u32,
    pub xor_source_id: u64,
    pub xor_source_sig: u64,
    pub tree_id: u16,
}

/// CONTROL payload variants (follows `LosslessSessionHeader` when kind == Control).
///
/// Version 5 keeps a shared `Manifest -> Ready -> payload -> control` envelope.
/// Plain and block-FEC modes still use `SourceDone -> Need` as an end-of-round
/// summary step. METTLE uses the same fields but interprets `SourceDone` as a
/// tail/end-of-stream marker and may emit live `Need` reports before the tail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionControl {
    Manifest { manifest: LosslessSessionManifest },
    Ready,
    SourceDone { round_id: u32 },
    Need { round_id: u32, report: NeedReport },
}
