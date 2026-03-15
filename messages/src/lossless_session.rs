use serde::{Deserialize, Serialize};

/// Magic constant ("RLM1" ASCII) used by lossless session frames.
pub const LOSSLESS_SESSION_MAGIC: u32 = 0x524C_4D31;
/// Single cutover protocol version for the block-first wire model.
pub const LOSSLESS_SESSION_VERSION: u8 = 3;
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
    BlockAck = 3,
    BlockStatus = 4,
    Eot = 5,
    PlainStatus = 6,
    FecStatus = 7,
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
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionFecMode {
    /// Raw wire scheme identifier to preserve clean handling for unknown schemes.
    pub scheme: u8,
    pub symbols_per_block: u16,
    pub tree_ids: Vec<u16>,
}

impl LosslessSessionFecMode {
    #[must_use]
    pub fn new_raptorq(symbols_per_block: u16, tree_ids: Vec<u16>) -> Self {
        Self {
            scheme: FecScheme::RaptorQ.to_wire(),
            symbols_per_block,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStatus {
    pub block_id: u64,
    pub deficit_symbols: u16,
}

/// End-of-round FEC feedback emitted after `Eot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FecStatus {
    Complete,
    MissingBlocks { blocks: Vec<BlockStatus> },
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

/// End-of-round plain-mode feedback emitted after `Eot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlainStatus {
    Complete,
    MissingBlocks { ranges: Vec<MissingBlockRange> },
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
    FecTreeIdsEmpty,
    TooManyTreeIds {
        configured: usize,
        max: usize,
    },
    TreeIdsMustBeSortedUnique,
    ZeroDeficitSymbols,
    BlockDataRequiresPlainMode,
    BlockSymbolRequiresFecMode,
    BlockStatusRequiresFecMode,
    PlainStatusRequiresPlainMode,
    FecStatusRequiresFecMode,
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
    MissingBlockRangesEmpty,
    MissingBlockRangeInvalid {
        start_block_id: u64,
        end_block_id: u64,
    },
    MissingBlockRangeOutOfRange {
        end_block_id: u64,
        total_blocks: u64,
    },
    MissingBlockRangesMustBeSortedMerged,
    TooManyMissingBlockRanges {
        configured: usize,
        max: usize,
    },
    EmptyFecStatusBlocks,
    FecStatusBlocksMustBeSortedUnique,
    TooManyFecStatusBlocks {
        configured: usize,
        max: usize,
    },
}

pub const MAX_MISSING_BLOCK_RANGES: usize = u8::MAX as usize;
pub const MAX_FEC_STATUS_BLOCKS: usize = u16::MAX as usize;

impl PlainStatus {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        let ranges = match self {
            Self::Complete => return Ok(()),
            Self::MissingBlocks { ranges } => ranges,
        };

        if ranges.is_empty() {
            return Err(LosslessSessionValidationError::MissingBlockRangesEmpty);
        }
        if ranges.len() > MAX_MISSING_BLOCK_RANGES {
            return Err(LosslessSessionValidationError::TooManyMissingBlockRanges {
                configured: ranges.len(),
                max: MAX_MISSING_BLOCK_RANGES,
            });
        }

        let mut previous_end = None;
        for range in ranges {
            if range.start_block_id >= range.end_block_id {
                return Err(LosslessSessionValidationError::MissingBlockRangeInvalid {
                    start_block_id: range.start_block_id,
                    end_block_id: range.end_block_id,
                });
            }
            if let Some(prev_end) = previous_end
                && range.start_block_id <= prev_end
            {
                return Err(LosslessSessionValidationError::MissingBlockRangesMustBeSortedMerged);
            }
            previous_end = Some(range.end_block_id);
        }

        Ok(())
    }

    pub fn validate_against_total_blocks(
        &self,
        total_blocks: u64,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if let Self::MissingBlocks { ranges } = self {
            for range in ranges {
                if range.end_block_id > total_blocks {
                    return Err(
                        LosslessSessionValidationError::MissingBlockRangeOutOfRange {
                            end_block_id: range.end_block_id,
                            total_blocks,
                        },
                    );
                }
            }
        }
        Ok(())
    }
}

impl FecStatus {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        let blocks = match self {
            Self::Complete => return Ok(()),
            Self::MissingBlocks { blocks } => blocks,
        };

        if blocks.is_empty() {
            return Err(LosslessSessionValidationError::EmptyFecStatusBlocks);
        }
        if blocks.len() > MAX_FEC_STATUS_BLOCKS {
            return Err(LosslessSessionValidationError::TooManyFecStatusBlocks {
                configured: blocks.len(),
                max: MAX_FEC_STATUS_BLOCKS,
            });
        }
        if blocks.iter().any(|status| status.deficit_symbols == 0) {
            return Err(LosslessSessionValidationError::ZeroDeficitSymbols);
        }
        if !blocks
            .windows(2)
            .all(|pair| pair[0].block_id < pair[1].block_id)
        {
            return Err(LosslessSessionValidationError::FecStatusBlocksMustBeSortedUnique);
        }

        Ok(())
    }

    pub fn validate_against_total_blocks(
        &self,
        total_blocks: u64,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if let Self::MissingBlocks { blocks } = self {
            for status in blocks {
                if status.block_id >= total_blocks {
                    return Err(LosslessSessionValidationError::BlockIdOutOfRange {
                        block_id: status.block_id,
                        total_blocks,
                    });
                }
            }
        }
        Ok(())
    }
}

impl LosslessSessionManifest {
    pub fn total_blocks_for(
        total_bytes: u64,
        block_size: u32,
    ) -> Result<u64, LosslessSessionValidationError> {
        if block_size == 0 {
            return Err(LosslessSessionValidationError::ZeroBlockSize);
        }
        let block_size = u64::from(block_size);
        if total_bytes == 0 {
            Ok(0)
        } else {
            Ok(total_bytes.div_ceil(block_size))
        }
    }

    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        let expected = Self::total_blocks_for(self.total_bytes, self.block_size)?;
        if self.total_blocks != expected {
            return Err(LosslessSessionValidationError::InconsistentTotalBlocks {
                expected,
                actual: self.total_blocks,
            });
        }

        if let LosslessSessionMode::Fec(fec) = &self.mode {
            if fec.scheme_kind().is_none() {
                return Err(LosslessSessionValidationError::UnknownFecScheme {
                    scheme: fec.scheme,
                });
            }
            if fec.symbols_per_block == 0 {
                return Err(LosslessSessionValidationError::ZeroSymbolsPerBlock);
            }
            if fec.tree_ids.is_empty() {
                return Err(LosslessSessionValidationError::FecTreeIdsEmpty);
            }
            if fec.tree_ids.len() > MAX_MANIFEST_TREE_IDS {
                return Err(LosslessSessionValidationError::TooManyTreeIds {
                    configured: fec.tree_ids.len(),
                    max: MAX_MANIFEST_TREE_IDS,
                });
            }
            if !fec.tree_ids.windows(2).all(|pair| pair[0] < pair[1]) {
                return Err(LosslessSessionValidationError::TreeIdsMustBeSortedUnique);
            }
        }

        Ok(())
    }

    pub fn validate_block_id(&self, block_id: u64) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if block_id >= self.total_blocks {
            return Err(LosslessSessionValidationError::BlockIdOutOfRange {
                block_id,
                total_blocks: self.total_blocks,
            });
        }
        Ok(())
    }

    pub fn expected_block_payload_len(
        &self,
        block_id: u64,
    ) -> Result<u32, LosslessSessionValidationError> {
        self.validate_block_id(block_id)?;
        if self.total_blocks == 0 {
            return Ok(0);
        }
        if block_id + 1 == self.total_blocks {
            let rem = (self.total_bytes % u64::from(self.block_size)) as u32;
            if rem == 0 {
                Ok(self.block_size)
            } else {
                Ok(rem)
            }
        } else {
            Ok(self.block_size)
        }
    }

    pub fn validate_block_data(
        &self,
        data: &LosslessSessionBlockData,
    ) -> Result<(), LosslessSessionValidationError> {
        if self.mode.is_fec() {
            return Err(LosslessSessionValidationError::BlockDataRequiresPlainMode);
        }
        let expected = self.expected_block_payload_len(data.block_id)?;
        if data.payload_len != expected {
            return Err(LosslessSessionValidationError::BlockDataLenMismatch {
                block_id: data.block_id,
                expected,
                actual: data.payload_len,
            });
        }
        Ok(())
    }

    pub fn validate_block_symbol(
        &self,
        symbol: &LosslessSessionBlockSymbol,
    ) -> Result<(), LosslessSessionValidationError> {
        let LosslessSessionMode::Fec(fec) = &self.mode else {
            return Err(LosslessSessionValidationError::BlockSymbolRequiresFecMode);
        };
        self.validate_block_id(symbol.block_id)?;
        if !fec.tree_ids.contains(&symbol.tree_id) {
            return Err(
                LosslessSessionValidationError::BlockSymbolTreeIdNotAdvertised {
                    tree_id: symbol.tree_id,
                },
            );
        }
        Ok(())
    }

    pub fn validate_plain_status(
        &self,
        status: &PlainStatus,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if self.mode.is_fec() {
            return Err(LosslessSessionValidationError::PlainStatusRequiresPlainMode);
        }
        status.validate_against_total_blocks(self.total_blocks)
    }

    pub fn validate_fec_status(
        &self,
        status: &FecStatus,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if !self.mode.is_fec() {
            return Err(LosslessSessionValidationError::FecStatusRequiresFecMode);
        }
        status.validate_against_total_blocks(self.total_blocks)
    }

    pub fn validate_control(
        &self,
        control: &LosslessSessionControl,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        control.validate()?;
        match control {
            LosslessSessionControl::Manifest { manifest } => manifest.validate(),
            LosslessSessionControl::Ready { .. } | LosslessSessionControl::Eot => Ok(()),
            LosslessSessionControl::BlockAck { block_id } => self.validate_block_id(*block_id),
            LosslessSessionControl::BlockStatus { status } => {
                if !self.mode.is_fec() {
                    return Err(LosslessSessionValidationError::BlockStatusRequiresFecMode);
                }
                self.validate_block_id(status.block_id)
            }
            LosslessSessionControl::PlainStatus { status } => self.validate_plain_status(status),
            LosslessSessionControl::FecStatus { status } => self.validate_fec_status(status),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionBlockData {
    pub block_id: u64,
    pub payload_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionBlockSymbol {
    pub block_id: u64,
    pub symbol_id: u32,
    pub tree_id: u16,
    pub payload_len: u32,
}

/// Fixed header for both block and control frames.
///
/// Layout (big-endian):
/// - magic:      u32  (LOSSLESS_SESSION_MAGIC)
/// - version:    u8   (LOSSLESS_SESSION_VERSION)
/// - kind:       u8   (1=BlockData, 2=BlockSymbol, 3=Control)
/// - ctrl_kind:  u8   (LosslessSessionCtrlKind value when kind=Control, else 0)
/// - reserved:   u8   (0; alignment/padding)
/// - session_id: u64  (flow/session demux)
/// - body_len:   u32  (number of bytes following the header)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LosslessSessionHeader {
    pub magic: u32,
    pub version: u8,
    pub kind: LosslessSessionKind,
    pub ctrl_kind: u8,
    pub session_id: u64,
    pub body_len: u32,
}

impl LosslessSessionHeader {
    pub const LEN: usize = 4 + 1 + 1 + 1 + 1 + 8 + 4;

    #[inline]
    pub fn encode_into(&self, out: &mut [u8]) {
        debug_assert!(out.len() >= Self::LEN);
        out[0..4].copy_from_slice(&self.magic.to_be_bytes());
        out[4] = self.version;
        out[5] = self.kind as u8;
        out[6] = self.ctrl_kind;
        out[7] = 0;
        out[8..16].copy_from_slice(&self.session_id.to_be_bytes());
        out[16..20].copy_from_slice(&self.body_len.to_be_bytes());
    }

    #[inline]
    pub fn decode_from(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < Self::LEN {
            return None;
        }
        let magic = u32::from_be_bytes(buf[0..4].try_into().ok()?);
        if magic != LOSSLESS_SESSION_MAGIC {
            return None;
        }
        let version = buf[4];
        if version != LOSSLESS_SESSION_VERSION {
            return None;
        }
        let kind = match buf[5] {
            1 => LosslessSessionKind::BlockData,
            2 => LosslessSessionKind::BlockSymbol,
            3 => LosslessSessionKind::Control,
            _ => return None,
        };
        let ctrl_kind = buf[6];
        let session_id = u64::from_be_bytes(buf[8..16].try_into().ok()?);
        let body_len = u32::from_be_bytes(buf[16..20].try_into().ok()?);
        Some((
            Self {
                magic,
                version,
                kind,
                ctrl_kind,
                session_id,
                body_len,
            },
            Self::LEN,
        ))
    }
}

/// CONTROL payload variants (follows `LosslessSessionHeader` when kind == Control).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionControl {
    Manifest { manifest: LosslessSessionManifest },
    Ready { node_id: u64 },
    // Retained during the plain-mode cutover; later issues switch plain mode to
    // PlainStatus while FEC continues to use per-block feedback.
    BlockAck { block_id: u64 },
    BlockStatus { status: BlockStatus },
    Eot,
    PlainStatus { status: PlainStatus },
    FecStatus { status: FecStatus },
}

impl LosslessSessionControl {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        match self {
            Self::Manifest { manifest } => manifest.validate(),
            Self::Ready { .. } | Self::BlockAck { .. } | Self::Eot => Ok(()),
            Self::BlockStatus { status } => {
                if status.deficit_symbols == 0 {
                    return Err(LosslessSessionValidationError::ZeroDeficitSymbols);
                }
                Ok(())
            }
            Self::PlainStatus { status } => status.validate(),
            Self::FecStatus { status } => status.validate(),
        }
    }
}

/// Encode a `BlockData` frame into a fresh `Vec<u8>`.
pub fn encode_block_data(session_id: u64, block_id: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 4 + payload.len() as u32;
    let mut out = vec![0u8; LosslessSessionHeader::LEN + body_len as usize];
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::BlockData,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..LosslessSessionHeader::LEN]);
    let mut pos = LosslessSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&block_id.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

const BLOCK_SYMBOL_FIXED_BODY_LEN: usize = 8 + 4 + 2 + 2 + 4;
const BLOCK_SYMBOL_TREE_ID_OFFSET: usize = LosslessSessionHeader::LEN + 8 + 4;

/// Encode a `BlockSymbol` frame into a fresh `Vec<u8>`.
pub fn encode_block_symbol(
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    encode_block_symbol_into(&mut out, session_id, block_id, symbol_id, tree_id, payload);
    out
}

/// Encode a `BlockSymbol` frame into the provided reusable buffer.
pub fn encode_block_symbol_into<'a>(
    buf: &'a mut Vec<u8>,
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> &'a [u8] {
    let body_len = BLOCK_SYMBOL_FIXED_BODY_LEN + payload.len();
    let frame_len = LosslessSessionHeader::LEN + body_len;
    buf.resize(frame_len, 0);
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::BlockSymbol,
        ctrl_kind: 0,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..LosslessSessionHeader::LEN]);

    let mut pos = LosslessSessionHeader::LEN;
    buf[pos..pos + 8].copy_from_slice(&block_id.to_be_bytes());
    pos += 8;
    buf[pos..pos + 4].copy_from_slice(&symbol_id.to_be_bytes());
    pos += 4;
    buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
    pos += 2;
    buf[pos..pos + 2].copy_from_slice(&0u16.to_be_bytes());
    pos += 2;
    buf[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    buf[pos..pos + payload.len()].copy_from_slice(payload);
    &buf[..frame_len]
}

/// Update the tree id for an already-encoded `BlockSymbol` frame.
pub fn set_block_symbol_tree_id(buf: &mut [u8], tree_id: u16) -> Option<()> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockSymbol || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < BLOCK_SYMBOL_FIXED_BODY_LEN as u32 || buf.len() < off + hdr.body_len as usize
    {
        return None;
    }

    let pos = off + (BLOCK_SYMBOL_TREE_ID_OFFSET - LosslessSessionHeader::LEN);
    buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
    Some(())
}

/// Try to decode a `BlockData` frame; returns (header, block metadata, payload slice).
pub fn decode_block_data(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionBlockData, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockData || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < 12 || buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let mut pos = off;
    let block_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let payload_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    if hdr.body_len as usize != 8 + 4 + payload_len as usize {
        return None;
    }
    let payload_end = pos + payload_len as usize;
    if payload_end > buf.len() {
        return None;
    }
    Some((
        hdr,
        LosslessSessionBlockData {
            block_id,
            payload_len,
        },
        &buf[pos..payload_end],
    ))
}

/// Try to decode a `BlockSymbol` frame; returns (header, block metadata, payload slice).
pub fn decode_block_symbol(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionBlockSymbol, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockSymbol || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < 20 || buf.len() < off + hdr.body_len as usize {
        return None;
    }

    let mut pos = off;
    let block_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let symbol_id = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let tree_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?);
    pos += 2;
    pos += 2;
    let payload_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;

    if hdr.body_len as usize != 8 + 4 + 2 + 2 + 4 + payload_len as usize {
        return None;
    }

    let payload_end = pos + payload_len as usize;
    if payload_end > buf.len() {
        return None;
    }

    Some((
        hdr,
        LosslessSessionBlockSymbol {
            block_id,
            symbol_id,
            tree_id,
            payload_len,
        },
        &buf[pos..payload_end],
    ))
}

const MANIFEST_FIXED_BODY_LEN: usize = 1 + 1 + 1 + 1 + 4 + 8 + 8 + 2 + 2;
const PLAIN_STATUS_FIXED_BODY_LEN: usize = 1 + 1 + 2;
const PLAIN_STATUS_RANGE_LEN: usize = 8 + 8;
const FEC_STATUS_FIXED_BODY_LEN: usize = 1 + 1 + 2;
const FEC_STATUS_BLOCK_LEN: usize = 8 + 2 + 2;

/// Stack-friendly scratch size for common control frames.
///
/// Large `FecStatus::MissingBlocks` reports can exceed this bound; callers that
/// need to encode arbitrarily large FEC status payloads should use
/// [`encode_control`], which allocates an exact-size `Vec<u8>`.
pub const MAX_CONTROL_FRAME_SIZE: usize = LosslessSessionHeader::LEN
    + max_control_body_len(
        MANIFEST_FIXED_BODY_LEN + (MAX_MANIFEST_TREE_IDS * 2),
        PLAIN_STATUS_FIXED_BODY_LEN + (MAX_MISSING_BLOCK_RANGES * PLAIN_STATUS_RANGE_LEN),
        FEC_STATUS_FIXED_BODY_LEN + ((u8::MAX as usize) * FEC_STATUS_BLOCK_LEN),
    );

const fn max_control_body_len(lhs: usize, mid: usize, rhs: usize) -> usize {
    let first = if lhs > mid { lhs } else { mid };
    if first > rhs { first } else { rhs }
}

fn manifest_tree_ids(mode: &LosslessSessionMode) -> &[u16] {
    match mode {
        LosslessSessionMode::Plain => &[],
        LosslessSessionMode::Fec(fec) => &fec.tree_ids,
    }
}

fn control_body_len(control: &LosslessSessionControl) -> usize {
    match control {
        LosslessSessionControl::Manifest { manifest } => {
            MANIFEST_FIXED_BODY_LEN + (manifest_tree_ids(&manifest.mode).len() * 2)
        }
        LosslessSessionControl::Ready { .. } => 8,
        LosslessSessionControl::BlockAck { .. } => 8,
        LosslessSessionControl::BlockStatus { .. } => 12,
        LosslessSessionControl::Eot => 0,
        LosslessSessionControl::PlainStatus { status } => match status {
            PlainStatus::Complete => PLAIN_STATUS_FIXED_BODY_LEN,
            PlainStatus::MissingBlocks { ranges } => {
                PLAIN_STATUS_FIXED_BODY_LEN + (ranges.len() * PLAIN_STATUS_RANGE_LEN)
            }
        },
        LosslessSessionControl::FecStatus { status } => match status {
            FecStatus::Complete => FEC_STATUS_FIXED_BODY_LEN,
            FecStatus::MissingBlocks { blocks } => {
                FEC_STATUS_FIXED_BODY_LEN + (blocks.len() * FEC_STATUS_BLOCK_LEN)
            }
        },
    }
}

/// Encode a CONTROL frame into the provided buffer.
///
/// The buffer must be at least `LosslessSessionHeader::LEN + control_body_len(control)` bytes.
pub fn encode_control_into<'a>(
    buf: &'a mut [u8],
    session_id: u64,
    control: &LosslessSessionControl,
) -> &'a [u8] {
    control
        .validate()
        .expect("lossless control must validate before encoding");
    let body_len = control_body_len(control);
    assert!(
        buf.len() >= LosslessSessionHeader::LEN + body_len,
        "buffer too small for encoded control frame"
    );

    let ctrl_kind = match control {
        LosslessSessionControl::Manifest { manifest } => {
            let body_start = LosslessSessionHeader::LEN;
            let (scheme, symbols_per_block, tree_ids) = match &manifest.mode {
                LosslessSessionMode::Plain => (0u8, 0u16, &[][..]),
                LosslessSessionMode::Fec(fec) => {
                    (fec.scheme, fec.symbols_per_block, fec.tree_ids.as_slice())
                }
            };
            assert!(
                tree_ids.len() <= MAX_MANIFEST_TREE_IDS,
                "manifest tree set exceeds wire capacity"
            );

            buf[body_start] = manifest.mode.kind() as u8;
            buf[body_start + 1] = scheme;
            buf[body_start + 2] = tree_ids.len() as u8;
            buf[body_start + 3] = 0;
            buf[body_start + 4..body_start + 8].copy_from_slice(&manifest.block_size.to_be_bytes());
            buf[body_start + 8..body_start + 16]
                .copy_from_slice(&manifest.total_bytes.to_be_bytes());
            buf[body_start + 16..body_start + 24]
                .copy_from_slice(&manifest.total_blocks.to_be_bytes());
            buf[body_start + 24..body_start + 26].copy_from_slice(&symbols_per_block.to_be_bytes());
            buf[body_start + 26..body_start + 28].copy_from_slice(&0u16.to_be_bytes());

            let mut pos = body_start + MANIFEST_FIXED_BODY_LEN;
            for tree_id in tree_ids {
                buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
                pos += 2;
            }
            LosslessSessionCtrlKind::Manifest as u8
        }
        LosslessSessionControl::Ready { node_id } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&node_id.to_be_bytes());
            LosslessSessionCtrlKind::Ready as u8
        }
        LosslessSessionControl::BlockAck { block_id } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&block_id.to_be_bytes());
            LosslessSessionCtrlKind::BlockAck as u8
        }
        LosslessSessionControl::BlockStatus { status } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&status.block_id.to_be_bytes());
            buf[body_start + 8..body_start + 10]
                .copy_from_slice(&status.deficit_symbols.to_be_bytes());
            buf[body_start + 10..body_start + 12].copy_from_slice(&0u16.to_be_bytes());
            LosslessSessionCtrlKind::BlockStatus as u8
        }
        LosslessSessionControl::Eot => LosslessSessionCtrlKind::Eot as u8,
        LosslessSessionControl::PlainStatus { status } => {
            let body_start = LosslessSessionHeader::LEN;
            match status {
                PlainStatus::Complete => {
                    buf[body_start] = 0;
                    buf[body_start + 1] = 0;
                    buf[body_start + 2..body_start + 4].copy_from_slice(&0u16.to_be_bytes());
                }
                PlainStatus::MissingBlocks { ranges } => {
                    assert!(
                        ranges.len() <= MAX_MISSING_BLOCK_RANGES,
                        "missing block ranges exceed wire capacity"
                    );
                    buf[body_start] = 1;
                    buf[body_start + 1] = ranges.len() as u8;
                    buf[body_start + 2..body_start + 4].copy_from_slice(&0u16.to_be_bytes());
                    let mut pos = body_start + PLAIN_STATUS_FIXED_BODY_LEN;
                    for range in ranges {
                        buf[pos..pos + 8].copy_from_slice(&range.start_block_id.to_be_bytes());
                        buf[pos + 8..pos + 16].copy_from_slice(&range.end_block_id.to_be_bytes());
                        pos += PLAIN_STATUS_RANGE_LEN;
                    }
                }
            }
            LosslessSessionCtrlKind::PlainStatus as u8
        }
        LosslessSessionControl::FecStatus { status } => {
            let body_start = LosslessSessionHeader::LEN;
            match status {
                FecStatus::Complete => {
                    buf[body_start] = 0;
                    buf[body_start + 1] = 0;
                    buf[body_start + 2..body_start + 4].copy_from_slice(&0u16.to_be_bytes());
                }
                FecStatus::MissingBlocks { blocks } => {
                    assert!(
                        blocks.len() <= MAX_FEC_STATUS_BLOCKS,
                        "fec status blocks exceed wire capacity"
                    );
                    buf[body_start] = 1;
                    buf[body_start + 1] = (blocks.len() & 0xff) as u8;
                    buf[body_start + 2] = ((blocks.len() >> 8) & 0xff) as u8;
                    buf[body_start + 3] = 0;
                    let mut pos = body_start + FEC_STATUS_FIXED_BODY_LEN;
                    for block in blocks {
                        buf[pos..pos + 8].copy_from_slice(&block.block_id.to_be_bytes());
                        buf[pos + 8..pos + 10]
                            .copy_from_slice(&block.deficit_symbols.to_be_bytes());
                        buf[pos + 10..pos + 12].copy_from_slice(&0u16.to_be_bytes());
                        pos += FEC_STATUS_BLOCK_LEN;
                    }
                }
            }
            LosslessSessionCtrlKind::FecStatus as u8
        }
    };

    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::Control,
        ctrl_kind,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..LosslessSessionHeader::LEN]);

    &buf[..LosslessSessionHeader::LEN + body_len]
}

/// Encode a CONTROL frame (header + control body) into a fresh `Vec<u8>`.
pub fn encode_control(session_id: u64, control: &LosslessSessionControl) -> Vec<u8> {
    let mut buf = vec![0u8; LosslessSessionHeader::LEN + control_body_len(control)];
    encode_control_into(&mut buf, session_id, control);
    buf
}

/// Try to decode a CONTROL frame; returns (header, parsed control).
pub fn decode_control(buf: &[u8]) -> Option<(LosslessSessionHeader, LosslessSessionControl)> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::Control {
        return None;
    }
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let body = &buf[off..off + hdr.body_len as usize];
    let ctrl = match hdr.ctrl_kind {
        x if x == LosslessSessionCtrlKind::Manifest as u8 => {
            if body.len() < MANIFEST_FIXED_BODY_LEN {
                return None;
            }
            let mode_kind = LosslessSessionModeKind::from_wire(body[0])?;
            let scheme = body[1];
            let tree_count = body[2] as usize;
            let block_size = u32::from_be_bytes(body[4..8].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[8..16].try_into().ok()?);
            let total_blocks = u64::from_be_bytes(body[16..24].try_into().ok()?);
            let symbols_per_block = u16::from_be_bytes(body[24..26].try_into().ok()?);

            if body.len() != MANIFEST_FIXED_BODY_LEN + (tree_count * 2) {
                return None;
            }

            let mut tree_ids = Vec::with_capacity(tree_count);
            let mut pos = MANIFEST_FIXED_BODY_LEN;
            for _ in 0..tree_count {
                tree_ids.push(u16::from_be_bytes(body[pos..pos + 2].try_into().ok()?));
                pos += 2;
            }

            let mode = match mode_kind {
                LosslessSessionModeKind::Plain => {
                    if scheme != 0 || symbols_per_block != 0 || !tree_ids.is_empty() {
                        return None;
                    }
                    LosslessSessionMode::Plain
                }
                LosslessSessionModeKind::Fec => LosslessSessionMode::Fec(LosslessSessionFecMode {
                    scheme,
                    symbols_per_block,
                    tree_ids,
                }),
            };
            let manifest = LosslessSessionManifest {
                block_size,
                total_bytes,
                total_blocks,
                mode,
            };
            manifest.validate().ok()?;
            LosslessSessionControl::Manifest { manifest }
        }
        x if x == LosslessSessionCtrlKind::Ready as u8 => {
            if body.len() != 8 {
                return None;
            }
            let node_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            LosslessSessionControl::Ready { node_id }
        }
        x if x == LosslessSessionCtrlKind::BlockAck as u8 => {
            if body.len() != 8 {
                return None;
            }
            let block_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            LosslessSessionControl::BlockAck { block_id }
        }
        x if x == LosslessSessionCtrlKind::BlockStatus as u8 => {
            if body.len() != 12 {
                return None;
            }
            let block_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let deficit_symbols = u16::from_be_bytes(body[8..10].try_into().ok()?);
            let status = BlockStatus {
                block_id,
                deficit_symbols,
            };
            LosslessSessionControl::BlockStatus { status }
        }
        x if x == LosslessSessionCtrlKind::Eot as u8 => {
            if !body.is_empty() {
                return None;
            }
            LosslessSessionControl::Eot
        }
        x if x == LosslessSessionCtrlKind::PlainStatus as u8 => {
            if body.len() < PLAIN_STATUS_FIXED_BODY_LEN {
                return None;
            }

            let report_kind = body[0];
            let range_count = body[1] as usize;
            if body.len() != PLAIN_STATUS_FIXED_BODY_LEN + (range_count * PLAIN_STATUS_RANGE_LEN) {
                return None;
            }

            let status = match report_kind {
                0 if range_count == 0 => PlainStatus::Complete,
                1 => {
                    let mut ranges = Vec::with_capacity(range_count);
                    let mut pos = PLAIN_STATUS_FIXED_BODY_LEN;
                    for _ in 0..range_count {
                        ranges.push(MissingBlockRange {
                            start_block_id: u64::from_be_bytes(body[pos..pos + 8].try_into().ok()?),
                            end_block_id: u64::from_be_bytes(
                                body[pos + 8..pos + 16].try_into().ok()?,
                            ),
                        });
                        pos += PLAIN_STATUS_RANGE_LEN;
                    }
                    PlainStatus::MissingBlocks { ranges }
                }
                _ => return None,
            };
            LosslessSessionControl::PlainStatus { status }
        }
        x if x == LosslessSessionCtrlKind::FecStatus as u8 => {
            if body.len() < FEC_STATUS_FIXED_BODY_LEN {
                return None;
            }

            let report_kind = body[0];
            let block_count = usize::from(body[1]) | (usize::from(body[2]) << 8);
            if body.len() != FEC_STATUS_FIXED_BODY_LEN + (block_count * FEC_STATUS_BLOCK_LEN) {
                return None;
            }

            let status = match report_kind {
                0 if block_count == 0 => FecStatus::Complete,
                1 => {
                    let mut blocks = Vec::with_capacity(block_count);
                    let mut pos = FEC_STATUS_FIXED_BODY_LEN;
                    for _ in 0..block_count {
                        blocks.push(BlockStatus {
                            block_id: u64::from_be_bytes(body[pos..pos + 8].try_into().ok()?),
                            deficit_symbols: u16::from_be_bytes(
                                body[pos + 8..pos + 10].try_into().ok()?,
                            ),
                        });
                        pos += FEC_STATUS_BLOCK_LEN;
                    }
                    FecStatus::MissingBlocks { blocks }
                }
                _ => return None,
            };
            LosslessSessionControl::FecStatus { status }
        }
        _ => return None,
    };
    ctrl.validate().ok()?;
    Some((hdr, ctrl))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_manifest() -> LosslessSessionManifest {
        LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2500,
            total_blocks: 3,
            mode: LosslessSessionMode::Plain,
        }
    }

    fn fec_manifest() -> LosslessSessionManifest {
        LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2500,
            total_blocks: 3,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(8, vec![1, 3, 5])),
        }
    }

    #[test]
    fn roundtrip_block_data() {
        let payload = b"plain block";
        let buf = encode_block_data(42, 7, payload);
        let (hdr, data, body) = decode_block_data(&buf).expect("decode block data");
        assert_eq!(hdr.magic, LOSSLESS_SESSION_MAGIC);
        assert_eq!(hdr.version, LOSSLESS_SESSION_VERSION);
        assert_eq!(hdr.kind, LosslessSessionKind::BlockData);
        assert_eq!(hdr.session_id, 42);
        assert_eq!(data.block_id, 7);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
    }

    #[test]
    fn roundtrip_block_symbol() {
        let payload = b"fec symbol";
        let mut buf = Vec::new();
        encode_block_symbol_into(&mut buf, 42, 9, 3, 5, payload);
        let (hdr, data, body) = decode_block_symbol(&buf).expect("decode block symbol");
        assert_eq!(hdr.session_id, 42);
        assert_eq!(hdr.kind, LosslessSessionKind::BlockSymbol);
        assert_eq!(data.block_id, 9);
        assert_eq!(data.symbol_id, 3);
        assert_eq!(data.tree_id, 5);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
        assert!(
            decode_block_data(&buf).is_none(),
            "wrong decoder must reject block symbol"
        );
    }

    #[test]
    fn roundtrip_controls() {
        let manifest_plain = LosslessSessionControl::Manifest {
            manifest: plain_manifest(),
        };
        let manifest_fec = LosslessSessionControl::Manifest {
            manifest: fec_manifest(),
        };
        let ctrls = vec![
            manifest_plain,
            manifest_fec,
            LosslessSessionControl::Ready { node_id: 99 },
            LosslessSessionControl::BlockAck { block_id: 2 },
            LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            },
            LosslessSessionControl::PlainStatus {
                status: PlainStatus::MissingBlocks {
                    ranges: vec![
                        MissingBlockRange {
                            start_block_id: 0,
                            end_block_id: 1,
                        },
                        MissingBlockRange {
                            start_block_id: 2,
                            end_block_id: 3,
                        },
                    ],
                },
            },
            LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id: 2,
                    deficit_symbols: 3,
                },
            },
            LosslessSessionControl::FecStatus {
                status: FecStatus::Complete,
            },
            LosslessSessionControl::FecStatus {
                status: FecStatus::MissingBlocks {
                    blocks: vec![
                        BlockStatus {
                            block_id: 0,
                            deficit_symbols: 2,
                        },
                        BlockStatus {
                            block_id: 2,
                            deficit_symbols: 1,
                        },
                    ],
                },
            },
            LosslessSessionControl::Eot,
        ];

        for ctrl in ctrls {
            let buf = encode_control(77, &ctrl);
            let (hdr, decoded) = decode_control(&buf).expect("decode control");
            assert_eq!(hdr.session_id, 77);
            assert_eq!(hdr.version, LOSSLESS_SESSION_VERSION);
            assert_eq!(decoded, ctrl);
        }
    }

    #[test]
    fn encode_control_into_matches_encode_control() {
        let ctrls = vec![
            LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
            LosslessSessionControl::Manifest {
                manifest: fec_manifest(),
            },
            LosslessSessionControl::Ready { node_id: 11 },
            LosslessSessionControl::BlockAck { block_id: 1 },
            LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            },
            LosslessSessionControl::PlainStatus {
                status: PlainStatus::MissingBlocks {
                    ranges: vec![MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    }],
                },
            },
            LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id: 1,
                    deficit_symbols: 2,
                },
            },
            LosslessSessionControl::FecStatus {
                status: FecStatus::Complete,
            },
            LosslessSessionControl::FecStatus {
                status: FecStatus::MissingBlocks {
                    blocks: vec![BlockStatus {
                        block_id: 1,
                        deficit_symbols: 4,
                    }],
                },
            },
            LosslessSessionControl::Eot,
        ];

        for ctrl in ctrls {
            let heap_encoded = encode_control(42, &ctrl);
            let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
            let stack_encoded = encode_control_into(&mut buf, 42, &ctrl);
            assert_eq!(heap_encoded.as_slice(), stack_encoded);
            let (_, decoded_heap) = decode_control(&heap_encoded).expect("decode heap");
            let (_, decoded_stack) = decode_control(stack_encoded).expect("decode stack");
            assert_eq!(decoded_heap, ctrl);
            assert_eq!(decoded_stack, ctrl);
        }
    }

    #[test]
    fn encode_block_symbol_into_supports_tree_id_patch() {
        let mut frame = Vec::new();
        let encoded = encode_block_symbol_into(&mut frame, 42, 7, 3, 5, b"payload");
        let (_, symbol, body) = decode_block_symbol(encoded).expect("decode symbol");
        assert_eq!(symbol.block_id, 7);
        assert_eq!(symbol.symbol_id, 3);
        assert_eq!(symbol.tree_id, 5);
        assert_eq!(body, b"payload");

        set_block_symbol_tree_id(&mut frame, 9).expect("patch tree id");
        let (_, patched, patched_body) = decode_block_symbol(&frame).expect("decode patched");
        assert_eq!(patched.tree_id, 9);
        assert_eq!(patched_body, b"payload");
    }

    #[test]
    fn manifest_validation_rejects_bad_shapes() {
        let zero_block = LosslessSessionManifest {
            block_size: 0,
            total_bytes: 1,
            total_blocks: 1,
            mode: LosslessSessionMode::Plain,
        };
        assert_eq!(
            zero_block.validate(),
            Err(LosslessSessionValidationError::ZeroBlockSize)
        );

        let bad_total_blocks = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2049,
            total_blocks: 2,
            mode: LosslessSessionMode::Plain,
        };
        assert_eq!(
            bad_total_blocks.validate(),
            Err(LosslessSessionValidationError::InconsistentTotalBlocks {
                expected: 3,
                actual: 2,
            })
        );

        let bad_fec = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 1024,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode {
                scheme: 99,
                symbols_per_block: 0,
                tree_ids: vec![],
            }),
        };
        assert_eq!(
            bad_fec.validate(),
            Err(LosslessSessionValidationError::UnknownFecScheme { scheme: 99 })
        );
    }

    #[test]
    fn plain_mode_rejects_fec_only_frames() {
        let manifest = plain_manifest();
        let symbol = LosslessSessionBlockSymbol {
            block_id: 0,
            symbol_id: 0,
            tree_id: 1,
            payload_len: 128,
        };
        assert_eq!(
            manifest.validate_block_symbol(&symbol),
            Err(LosslessSessionValidationError::BlockSymbolRequiresFecMode)
        );
        assert_eq!(
            manifest.validate_control(&LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id: 0,
                    deficit_symbols: 1,
                },
            }),
            Err(LosslessSessionValidationError::BlockStatusRequiresFecMode)
        );
        assert_eq!(
            manifest.validate_control(&LosslessSessionControl::FecStatus {
                status: FecStatus::Complete,
            }),
            Err(LosslessSessionValidationError::FecStatusRequiresFecMode)
        );
        manifest
            .validate_control(&LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            })
            .expect("plain manifests should accept plain status reports");
    }

    #[test]
    fn fec_mode_rejects_plain_only_frames_and_unknown_tree_ids() {
        let manifest = fec_manifest();
        let data = LosslessSessionBlockData {
            block_id: 0,
            payload_len: 1024,
        };
        assert_eq!(
            manifest.validate_block_data(&data),
            Err(LosslessSessionValidationError::BlockDataRequiresPlainMode)
        );

        let bad_symbol = LosslessSessionBlockSymbol {
            block_id: 0,
            symbol_id: 5,
            tree_id: 99,
            payload_len: 128,
        };
        assert_eq!(
            manifest.validate_block_symbol(&bad_symbol),
            Err(LosslessSessionValidationError::BlockSymbolTreeIdNotAdvertised { tree_id: 99 })
        );
        assert_eq!(
            manifest.validate_control(&LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            }),
            Err(LosslessSessionValidationError::PlainStatusRequiresPlainMode)
        );
        manifest
            .validate_control(&LosslessSessionControl::FecStatus {
                status: FecStatus::Complete,
            })
            .expect("fec manifests should accept fec status reports");
    }

    #[test]
    fn plain_manifest_validates_block_lengths() {
        let manifest = plain_manifest();
        let full_block = LosslessSessionBlockData {
            block_id: 0,
            payload_len: 1024,
        };
        manifest
            .validate_block_data(&full_block)
            .expect("first block should use full block size");

        let tail_block = LosslessSessionBlockData {
            block_id: 2,
            payload_len: 452,
        };
        manifest
            .validate_block_data(&tail_block)
            .expect("tail block should use the remainder");

        let bad_tail = LosslessSessionBlockData {
            block_id: 2,
            payload_len: 1024,
        };
        assert_eq!(
            manifest.validate_block_data(&bad_tail),
            Err(LosslessSessionValidationError::BlockDataLenMismatch {
                block_id: 2,
                expected: 452,
                actual: 1024,
            })
        );
    }

    #[test]
    fn decode_control_rejects_invalid_manifest_and_short_bodies() {
        let good = encode_control(
            9,
            &LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
        );
        let mut truncated = good.clone();
        truncated.truncate(LosslessSessionHeader::LEN);
        assert!(decode_control(&truncated).is_none());

        let ready = encode_control(1, &LosslessSessionControl::Ready { node_id: 7 });
        let mut bad_ready = ready.clone();
        bad_ready.truncate(LosslessSessionHeader::LEN + 4);
        assert!(decode_control(&bad_ready).is_none());

        let plain_status = encode_control(
            1,
            &LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            },
        );
        let mut bad_plain_status = plain_status.clone();
        bad_plain_status.truncate(LosslessSessionHeader::LEN + 1);
        assert!(decode_control(&bad_plain_status).is_none());

        let mut bad_mode = good.clone();
        bad_mode[LosslessSessionHeader::LEN] = 9;
        assert!(decode_control(&bad_mode).is_none());

        let mut bad_tree_count = encode_control(
            10,
            &LosslessSessionControl::Manifest {
                manifest: fec_manifest(),
            },
        );
        bad_tree_count[LosslessSessionHeader::LEN + 2] = 7;
        assert!(decode_control(&bad_tree_count).is_none());
    }

    #[test]
    fn decode_control_rejects_invalid_plain_status_payloads() {
        let mut bad_kind = encode_control(
            12,
            &LosslessSessionControl::PlainStatus {
                status: PlainStatus::Complete,
            },
        );
        bad_kind[LosslessSessionHeader::LEN] = 9;
        assert!(
            decode_control(&bad_kind).is_none(),
            "plain-status decode must reject unsupported report kinds"
        );

        let mut malformed_ranges = encode_control(
            13,
            &LosslessSessionControl::PlainStatus {
                status: PlainStatus::MissingBlocks {
                    ranges: vec![MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    }],
                },
            },
        );
        let body_start = LosslessSessionHeader::LEN;
        malformed_ranges[body_start + PLAIN_STATUS_FIXED_BODY_LEN + 8
            ..body_start + PLAIN_STATUS_FIXED_BODY_LEN + 16]
            .copy_from_slice(&1u64.to_be_bytes());
        assert!(
            decode_control(&malformed_ranges).is_none(),
            "plain-status decode must reject malformed missing ranges"
        );
    }

    #[test]
    fn decode_control_rejects_invalid_fec_status_payloads() {
        let mut bad_kind = encode_control(
            14,
            &LosslessSessionControl::FecStatus {
                status: FecStatus::Complete,
            },
        );
        bad_kind[LosslessSessionHeader::LEN] = 9;
        assert!(
            decode_control(&bad_kind).is_none(),
            "fec-status decode must reject unsupported report kinds"
        );

        let mut duplicate_blocks = encode_control(
            15,
            &LosslessSessionControl::FecStatus {
                status: FecStatus::MissingBlocks {
                    blocks: vec![
                        BlockStatus {
                            block_id: 1,
                            deficit_symbols: 2,
                        },
                        BlockStatus {
                            block_id: 2,
                            deficit_symbols: 1,
                        },
                    ],
                },
            },
        );
        let body_start = LosslessSessionHeader::LEN;
        duplicate_blocks[body_start + FEC_STATUS_FIXED_BODY_LEN + FEC_STATUS_BLOCK_LEN
            ..body_start + FEC_STATUS_FIXED_BODY_LEN + FEC_STATUS_BLOCK_LEN + 8]
            .copy_from_slice(&1u64.to_be_bytes());
        assert!(
            decode_control(&duplicate_blocks).is_none(),
            "fec-status decode must reject duplicate or unsorted block entries"
        );
    }

    #[test]
    fn fec_status_roundtrips_at_255_block_limit() {
        let control = LosslessSessionControl::FecStatus {
            status: FecStatus::MissingBlocks {
                blocks: (0..MAX_FEC_STATUS_BLOCKS as u64)
                    .map(|block_id| BlockStatus {
                        block_id,
                        deficit_symbols: 4,
                    })
                    .collect(),
            },
        };

        let encoded = encode_control(16, &control);
        let (_, decoded) = decode_control(&encoded).expect("decode max-size fec status");
        assert_eq!(decoded, control);
    }

    #[test]
    fn decode_control_rejects_plain_manifest_with_fec_fields() {
        let mut encoded = encode_control(
            11,
            &LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
        );
        let body_start = LosslessSessionHeader::LEN;

        encoded[body_start + 1] = FecScheme::RaptorQ as u8;
        encoded[body_start + 2] = 1;
        encoded[body_start + 24..body_start + 26].copy_from_slice(&4u16.to_be_bytes());
        encoded.extend_from_slice(&7u16.to_be_bytes());

        let body_len = MANIFEST_FIXED_BODY_LEN + 2;
        encoded[16..20].copy_from_slice(&(body_len as u32).to_be_bytes());

        assert!(
            decode_control(&encoded).is_none(),
            "plain manifests must not carry FEC scheme, symbol, or tree-id fields"
        );
    }

    #[test]
    fn decode_rejects_bad_magic_and_wrong_kinds() {
        let mut buf = encode_block_data(1, 1, b"x");
        buf[0] = 0;
        assert!(decode_block_data(&buf).is_none());

        let mut symbol = Vec::new();
        encode_block_symbol_into(&mut symbol, 1, 0, 0, 1, b"y");
        assert!(decode_block_data(&symbol).is_none());
    }

    #[test]
    fn zero_deficit_block_status_is_rejected() {
        let control = LosslessSessionControl::BlockStatus {
            status: BlockStatus {
                block_id: 1,
                deficit_symbols: 0,
            },
        };
        assert_eq!(
            control.validate(),
            Err(LosslessSessionValidationError::ZeroDeficitSymbols)
        );
    }

    #[test]
    fn fec_status_validation_rejects_bad_blocks() {
        let manifest = fec_manifest();

        manifest
            .validate_fec_status(&FecStatus::Complete)
            .expect("complete fec status should validate");
        manifest
            .validate_fec_status(&FecStatus::MissingBlocks {
                blocks: vec![
                    BlockStatus {
                        block_id: 0,
                        deficit_symbols: 2,
                    },
                    BlockStatus {
                        block_id: 2,
                        deficit_symbols: 1,
                    },
                ],
            })
            .expect("sorted in-range fec status blocks should validate");

        assert_eq!(
            FecStatus::MissingBlocks { blocks: vec![] }.validate(),
            Err(LosslessSessionValidationError::EmptyFecStatusBlocks)
        );
        assert_eq!(
            FecStatus::MissingBlocks {
                blocks: vec![BlockStatus {
                    block_id: 0,
                    deficit_symbols: 0,
                }],
            }
            .validate(),
            Err(LosslessSessionValidationError::ZeroDeficitSymbols)
        );
        assert_eq!(
            FecStatus::MissingBlocks {
                blocks: vec![
                    BlockStatus {
                        block_id: 1,
                        deficit_symbols: 2,
                    },
                    BlockStatus {
                        block_id: 1,
                        deficit_symbols: 3,
                    },
                ],
            }
            .validate(),
            Err(LosslessSessionValidationError::FecStatusBlocksMustBeSortedUnique)
        );
        assert_eq!(
            manifest.validate_fec_status(&FecStatus::MissingBlocks {
                blocks: vec![BlockStatus {
                    block_id: 3,
                    deficit_symbols: 1,
                }],
            }),
            Err(LosslessSessionValidationError::BlockIdOutOfRange {
                block_id: 3,
                total_blocks: 3,
            })
        );

        assert_eq!(
            FecStatus::MissingBlocks {
                blocks: (0..(MAX_FEC_STATUS_BLOCKS as u64 + 1))
                    .map(|block_id| BlockStatus {
                        block_id,
                        deficit_symbols: 1,
                    })
                    .collect(),
            }
            .validate(),
            Err(LosslessSessionValidationError::TooManyFecStatusBlocks {
                configured: MAX_FEC_STATUS_BLOCKS + 1,
                max: MAX_FEC_STATUS_BLOCKS,
            })
        );
    }

    #[test]
    fn plain_status_validation_rejects_bad_ranges() {
        let manifest = plain_manifest();

        manifest
            .validate_plain_status(&PlainStatus::Complete)
            .expect("complete status should validate");
        manifest
            .validate_plain_status(&PlainStatus::MissingBlocks {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 0,
                        end_block_id: 1,
                    },
                    MissingBlockRange {
                        start_block_id: 2,
                        end_block_id: 3,
                    },
                ],
            })
            .expect("sorted disjoint missing ranges should validate");

        assert_eq!(
            PlainStatus::MissingBlocks { ranges: vec![] }.validate(),
            Err(LosslessSessionValidationError::MissingBlockRangesEmpty)
        );
        assert_eq!(
            manifest.validate_plain_status(&PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 2,
                    end_block_id: 2,
                }],
            }),
            Err(LosslessSessionValidationError::MissingBlockRangeInvalid {
                start_block_id: 2,
                end_block_id: 2,
            })
        );
        assert_eq!(
            manifest.validate_plain_status(&PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 2,
                    end_block_id: 4,
                }],
            }),
            Err(
                LosslessSessionValidationError::MissingBlockRangeOutOfRange {
                    end_block_id: 4,
                    total_blocks: 3,
                }
            )
        );
        assert_eq!(
            manifest.validate_plain_status(&PlainStatus::MissingBlocks {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 3,
                    },
                    MissingBlockRange {
                        start_block_id: 2,
                        end_block_id: 3,
                    },
                ],
            }),
            Err(LosslessSessionValidationError::MissingBlockRangesMustBeSortedMerged)
        );
    }
}
