use serde::{Deserialize, Serialize};

/// Magic constant (legacy "RLM1" ASCII) used by lossless session frames.
pub const LOSSLESS_SESSION_MAGIC: u32 = 0x524C_4D31;
pub const LOSSLESS_SESSION_BASE_VERSION: u8 = 1;
pub const LOSSLESS_SESSION_FEC_VERSION: u8 = 2;
/// Legacy alias kept for compatibility with existing non-FEC call sites.
pub const LOSSLESS_SESSION_VERSION: u8 = LOSSLESS_SESSION_BASE_VERSION;

/// Top-level frame kind carried in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionKind {
    Data = 1,
    Control = 2,
}

/// Control sub-kind (only meaningful when kind == Control).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionCtrlKind {
    Manifest = 1,
    Ready = 2,
    Ack = 3,
    Eot = 4,
    FecManifest = 5,
    FecCapabilities = 6,
    FecStatus = 7,
    FecCancel = 8,
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
            x if x == FecScheme::RaptorQ as u8 => Some(FecScheme::RaptorQ),
            _ => None,
        }
    }

    #[inline]
    pub const fn bit(self) -> u32 {
        1u32 << ((self as u8) - 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FecManifest {
    pub protocol_version: u8,
    /// Raw wire scheme identifier to preserve clean handling for unknown schemes.
    pub scheme: u8,
    pub symbols_per_block: u16,
    pub symbol_size: u16,
}

impl FecManifest {
    pub fn new_raptorq(symbols_per_block: u16, symbol_size: u16) -> Self {
        Self {
            protocol_version: LOSSLESS_SESSION_FEC_VERSION,
            scheme: FecScheme::RaptorQ.to_wire(),
            symbols_per_block,
            symbol_size,
        }
    }

    #[inline]
    pub fn scheme_kind(&self) -> Option<FecScheme> {
        FecScheme::from_wire(self.scheme)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FecCapabilities {
    pub protocol_version: u8,
    /// Bitset of supported schemes. Bit 0 => scheme id 1, etc.
    pub supported_schemes: u32,
}

impl FecCapabilities {
    pub fn empty() -> Self {
        Self {
            protocol_version: LOSSLESS_SESSION_FEC_VERSION,
            supported_schemes: 0,
        }
    }

    pub fn with_scheme(mut self, scheme: FecScheme) -> Self {
        self.supported_schemes |= scheme.bit();
        self
    }

    #[inline]
    pub fn supports_scheme_wire(&self, scheme: u8) -> bool {
        if scheme == 0 || scheme > 32 {
            return false;
        }
        (self.supported_schemes & (1u32 << (scheme - 1))) != 0
    }

    #[inline]
    pub fn supports_manifest(&self, manifest: &FecManifest) -> bool {
        self.protocol_version >= manifest.protocol_version
            && self.supports_scheme_wire(manifest.scheme)
    }
}

impl Default for FecCapabilities {
    fn default() -> Self {
        Self::empty().with_scheme(FecScheme::RaptorQ)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FecStatus {
    pub block_id: u64,
    pub deficit_symbols: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionFecData {
    pub block_id: u64,
    pub symbol_id: u32,
    pub tree_id: u16,
    pub payload_len: u32,
}

impl LosslessSessionFecData {
    pub const DEFAULT_TREE_ID: u16 = 0;
}

/// Fixed header for both DATA and CONTROL frames.
///
/// Layout (big-endian):
/// - magic:      u32  (LOSSLESS_SESSION_MAGIC)
/// - version:    u8   (LOSSLESS_SESSION_VERSION)
/// - kind:       u8   (1=Data, 2=Control)
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
        out[7] = 0; // reserved
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
        if !matches!(
            version,
            LOSSLESS_SESSION_BASE_VERSION | LOSSLESS_SESSION_FEC_VERSION
        ) {
            return None;
        }
        let kind = match buf[5] {
            1 => LosslessSessionKind::Data,
            2 => LosslessSessionKind::Control,
            _ => return None,
        };
        let ctrl_kind = buf[6];
        // buf[7] reserved
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSessionData {
    pub index: u64,
    pub payload_len: u32,
}

/// CONTROL payload variants (follows `LosslessSessionHeader` when kind == Control).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LosslessSessionControl {
    Manifest {
        chunk_size: u32,
        total_bytes: u64,
    },
    FecManifest {
        chunk_size: u32,
        total_bytes: u64,
        fec: FecManifest,
    },
    Ready {
        node_id: u64,
    },
    FecCapabilities {
        node_id: u64,
        capabilities: FecCapabilities,
    },
    Ack {
        up_to: u64,
    },
    FecStatus {
        status: FecStatus,
    },
    FecCancel {
        cancel_before_block_id: u64,
    },
    /// End-of-transfer marker with the last expected chunk.
    Eot {
        last_index: u64,
    },
}

/// Encode a DATA frame (header + LosslessSessionData + payload) into a fresh Vec<u8>.
pub fn encode_data(session_id: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 4 + payload.len() as u32;
    let mut out = vec![0u8; LosslessSessionHeader::LEN + body_len as usize];
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_BASE_VERSION,
        kind: LosslessSessionKind::Data,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..LosslessSessionHeader::LEN]);
    let mut pos = LosslessSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&index.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

/// Encode a FEC DATA frame (header + LosslessSessionFecData + payload) into a fresh Vec<u8>.
pub fn encode_fec_data(
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> Vec<u8> {
    let body_len = 8 + 4 + 2 + 2 + 4 + payload.len() as u32;
    let mut out = vec![0u8; LosslessSessionHeader::LEN + body_len as usize];
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_FEC_VERSION,
        kind: LosslessSessionKind::Data,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..LosslessSessionHeader::LEN]);

    let mut pos = LosslessSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&block_id.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&symbol_id.to_be_bytes());
    pos += 4;
    out[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
    pos += 2;
    out[pos..pos + 2].copy_from_slice(&0u16.to_be_bytes()); // reserved
    pos += 2;
    out[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

/// Encode a FEC DATA frame with the default tree id (`0`).
pub fn encode_fec_data_default_tree(
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    payload: &[u8],
) -> Vec<u8> {
    encode_fec_data(
        session_id,
        block_id,
        symbol_id,
        LosslessSessionFecData::DEFAULT_TREE_ID,
        payload,
    )
}

/// Try to decode a DATA frame; returns (header, data header, payload slice).
pub fn decode_data(buf: &[u8]) -> Option<(LosslessSessionHeader, LosslessSessionData, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::Data {
        return None;
    }
    if hdr.version != LOSSLESS_SESSION_BASE_VERSION {
        return None;
    }
    if hdr.body_len < 12 {
        return None;
    }
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let mut pos = off;
    let index = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
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
        LosslessSessionData { index, payload_len },
        &buf[pos..payload_end],
    ))
}

/// Try to decode a FEC DATA frame; returns (header, FEC metadata, payload slice).
pub fn decode_fec_data(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionFecData, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::Data {
        return None;
    }
    if hdr.version != LOSSLESS_SESSION_FEC_VERSION {
        return None;
    }
    if hdr.body_len < 20 {
        return None;
    }
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }

    let mut pos = off;
    let block_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let symbol_id = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let tree_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?);
    pos += 2;
    // reserved
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
        LosslessSessionFecData {
            block_id,
            symbol_id,
            tree_id,
            payload_len,
        },
        &buf[pos..payload_end],
    ))
}

/// Maximum size of a control frame.
///
/// Largest body is currently `FecManifest` (18 bytes).
pub const MAX_CONTROL_FRAME_SIZE: usize = LosslessSessionHeader::LEN + 18;

/// Encode a CONTROL frame into the provided buffer, returning the number of bytes written.
/// The buffer must be at least MAX_CONTROL_FRAME_SIZE bytes.
///
/// Returns the slice of the buffer containing the encoded frame.
pub fn encode_control_into<'a>(
    buf: &'a mut [u8],
    session_id: u64,
    control: &LosslessSessionControl,
) -> &'a [u8] {
    let version = default_control_version(control);
    encode_control_into_with_version(buf, session_id, version, control)
}

/// Encode a CONTROL frame into the provided buffer using an explicit protocol version.
pub fn encode_control_into_with_version<'a>(
    buf: &'a mut [u8],
    session_id: u64,
    version: u8,
    control: &LosslessSessionControl,
) -> &'a [u8] {
    use LosslessSessionControl::*;
    let mut version = match version {
        LOSSLESS_SESSION_BASE_VERSION | LOSSLESS_SESSION_FEC_VERSION => version,
        _ => default_control_version(control),
    };
    if version == LOSSLESS_SESSION_BASE_VERSION
        && matches!(
            control,
            FecManifest { .. } | FecCapabilities { .. } | FecStatus { .. } | FecCancel { .. }
        )
    {
        version = LOSSLESS_SESSION_FEC_VERSION;
    }

    // Encode the body directly into the buffer after the header
    let (ctrl_kind, body_len) = match control {
        Manifest {
            chunk_size,
            total_bytes,
        } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 4].copy_from_slice(&chunk_size.to_be_bytes());
            buf[body_start + 4..body_start + 12].copy_from_slice(&total_bytes.to_be_bytes());
            (LosslessSessionCtrlKind::Manifest as u8, 12)
        }
        FecManifest {
            chunk_size,
            total_bytes,
            fec,
        } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 4].copy_from_slice(&chunk_size.to_be_bytes());
            buf[body_start + 4..body_start + 12].copy_from_slice(&total_bytes.to_be_bytes());
            buf[body_start + 12] = fec.protocol_version;
            buf[body_start + 13] = fec.scheme;
            buf[body_start + 14..body_start + 16]
                .copy_from_slice(&fec.symbols_per_block.to_be_bytes());
            buf[body_start + 16..body_start + 18].copy_from_slice(&fec.symbol_size.to_be_bytes());
            (LosslessSessionCtrlKind::FecManifest as u8, 18)
        }
        Ready { node_id } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&node_id.to_be_bytes());
            (LosslessSessionCtrlKind::Ready as u8, 8)
        }
        FecCapabilities {
            node_id,
            capabilities,
        } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&node_id.to_be_bytes());
            buf[body_start + 8] = capabilities.protocol_version;
            buf[body_start + 9] = 0;
            buf[body_start + 10..body_start + 12].copy_from_slice(&0u16.to_be_bytes());
            buf[body_start + 12..body_start + 16]
                .copy_from_slice(&capabilities.supported_schemes.to_be_bytes());
            (LosslessSessionCtrlKind::FecCapabilities as u8, 16)
        }
        Ack { up_to } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&up_to.to_be_bytes());
            (LosslessSessionCtrlKind::Ack as u8, 8)
        }
        FecStatus { status } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&status.block_id.to_be_bytes());
            buf[body_start + 8..body_start + 10]
                .copy_from_slice(&status.deficit_symbols.to_be_bytes());
            buf[body_start + 10..body_start + 12].copy_from_slice(&0u16.to_be_bytes());
            (LosslessSessionCtrlKind::FecStatus as u8, 12)
        }
        FecCancel {
            cancel_before_block_id,
        } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&cancel_before_block_id.to_be_bytes());
            (LosslessSessionCtrlKind::FecCancel as u8, 8)
        }
        Eot { last_index } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&last_index.to_be_bytes());
            (LosslessSessionCtrlKind::Eot as u8, 8)
        }
    };

    // Encode the header at the start of the buffer
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version,
        kind: LosslessSessionKind::Control,
        ctrl_kind,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..LosslessSessionHeader::LEN]);

    &buf[..LosslessSessionHeader::LEN + body_len]
}

fn default_control_version(control: &LosslessSessionControl) -> u8 {
    match control {
        LosslessSessionControl::FecManifest { .. }
        | LosslessSessionControl::FecCapabilities { .. }
        | LosslessSessionControl::FecStatus { .. }
        | LosslessSessionControl::FecCancel { .. } => LOSSLESS_SESSION_FEC_VERSION,
        LosslessSessionControl::Manifest { .. }
        | LosslessSessionControl::Ready { .. }
        | LosslessSessionControl::Ack { .. }
        | LosslessSessionControl::Eot { .. } => LOSSLESS_SESSION_BASE_VERSION,
    }
}

/// Encode a CONTROL frame (header + control body) into a fresh Vec<u8>.
///
/// Note: Consider using `encode_control_into` with a stack buffer for better performance.
pub fn encode_control(session_id: u64, control: &LosslessSessionControl) -> Vec<u8> {
    let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
    let frame = encode_control_into(&mut buf, session_id, control);
    frame.to_vec()
}

/// Try to decode a CONTROL frame; returns (header, parsed control).
pub fn decode_control(buf: &[u8]) -> Option<(LosslessSessionHeader, LosslessSessionControl)> {
    use LosslessSessionControl::*;
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::Control {
        return None;
    }
    // Guard against out-of-bounds before slicing body to avoid panics.
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let body = &buf[off..off + hdr.body_len as usize];
    let ctrl = match hdr.ctrl_kind {
        x if x == LosslessSessionCtrlKind::Manifest as u8 => {
            if body.len() != 12 {
                return None;
            }
            let chunk_size = u32::from_be_bytes(body[0..4].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[4..12].try_into().ok()?);
            Manifest {
                chunk_size,
                total_bytes,
            }
        }
        x if x == LosslessSessionCtrlKind::FecManifest as u8 => {
            if hdr.version != LOSSLESS_SESSION_FEC_VERSION || body.len() != 18 {
                return None;
            }
            let chunk_size = u32::from_be_bytes(body[0..4].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[4..12].try_into().ok()?);
            let fec = crate::lossless_session::FecManifest {
                protocol_version: body[12],
                scheme: body[13],
                symbols_per_block: u16::from_be_bytes(body[14..16].try_into().ok()?),
                symbol_size: u16::from_be_bytes(body[16..18].try_into().ok()?),
            };
            FecManifest {
                chunk_size,
                total_bytes,
                fec,
            }
        }
        x if x == LosslessSessionCtrlKind::Ready as u8 => {
            if body.len() != 8 {
                return None;
            }
            let node_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ready { node_id }
        }
        x if x == LosslessSessionCtrlKind::FecCapabilities as u8 => {
            if hdr.version != LOSSLESS_SESSION_FEC_VERSION || body.len() != 16 {
                return None;
            }
            let node_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let capabilities = crate::lossless_session::FecCapabilities {
                protocol_version: body[8],
                supported_schemes: u32::from_be_bytes(body[12..16].try_into().ok()?),
            };
            FecCapabilities {
                node_id,
                capabilities,
            }
        }
        x if x == LosslessSessionCtrlKind::Ack as u8 => {
            if body.len() != 8 {
                return None;
            }
            let up_to = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ack { up_to }
        }
        x if x == LosslessSessionCtrlKind::FecStatus as u8 => {
            if hdr.version != LOSSLESS_SESSION_FEC_VERSION || body.len() != 12 {
                return None;
            }
            let block_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let deficit_symbols = u16::from_be_bytes(body[8..10].try_into().ok()?);
            FecStatus {
                status: crate::lossless_session::FecStatus {
                    block_id,
                    deficit_symbols,
                },
            }
        }
        x if x == LosslessSessionCtrlKind::FecCancel as u8 => {
            if hdr.version != LOSSLESS_SESSION_FEC_VERSION || body.len() != 8 {
                return None;
            }
            let cancel_before_block_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            FecCancel {
                cancel_before_block_id,
            }
        }
        x if x == LosslessSessionCtrlKind::Eot as u8 => {
            if body.len() != 8 {
                return None;
            }
            let last_index = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Eot { last_index }
        }
        _ => return None,
    };
    Some((hdr, ctrl))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_data() {
        let payload = b"hello world";
        let buf = encode_data(42, 7, payload);
        let (hdr, data, body) = decode_data(&buf).expect("decode data");
        assert_eq!(hdr.magic, LOSSLESS_SESSION_MAGIC);
        assert_eq!(hdr.version, LOSSLESS_SESSION_VERSION);
        assert_eq!(hdr.kind as u8, LosslessSessionKind::Data as u8);
        assert_eq!(hdr.session_id, 42);
        assert_eq!(data.index, 7);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
    }

    #[test]
    fn roundtrip_fec_data_and_default_tree() {
        let payload = b"fec payload";
        let buf = encode_fec_data_default_tree(42, 9, 3, payload);
        let (hdr, data, body) = decode_fec_data(&buf).expect("decode fec data");
        assert_eq!(hdr.session_id, 42);
        assert_eq!(hdr.version, LOSSLESS_SESSION_FEC_VERSION);
        assert_eq!(data.block_id, 9);
        assert_eq!(data.symbol_id, 3);
        assert_eq!(data.tree_id, 0);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
        assert!(
            decode_data(&buf).is_none(),
            "legacy decoder must reject v2 fec data"
        );
    }

    #[test]
    fn roundtrip_controls() {
        let ctrls = vec![
            LosslessSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123456,
            },
            LosslessSessionControl::FecManifest {
                chunk_size: 4096,
                total_bytes: 123456,
                fec: FecManifest::new_raptorq(64, 1400),
            },
            LosslessSessionControl::Ready { node_id: 99 },
            LosslessSessionControl::FecCapabilities {
                node_id: 99,
                capabilities: FecCapabilities::default(),
            },
            LosslessSessionControl::Ack { up_to: 77 },
            LosslessSessionControl::FecStatus {
                status: FecStatus {
                    block_id: 3,
                    deficit_symbols: 2,
                },
            },
            LosslessSessionControl::Eot { last_index: 15 },
        ];
        for ctrl in ctrls {
            let buf = encode_control(77, &ctrl);
            let (hdr, decoded) = decode_control(&buf).expect("decode control");
            assert_eq!(hdr.session_id, 77);
            let expected_version = match ctrl {
                LosslessSessionControl::FecManifest { .. }
                | LosslessSessionControl::FecCapabilities { .. }
                | LosslessSessionControl::FecStatus { .. } => LOSSLESS_SESSION_FEC_VERSION,
                _ => LOSSLESS_SESSION_BASE_VERSION,
            };
            assert_eq!(hdr.version, expected_version);
            assert_eq!(decoded, ctrl);
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = encode_data(1, 1, b"x");
        buf[0] = 0; // break magic
        assert!(decode_data(&buf).is_none());
    }

    #[test]
    fn decode_data_rejects_truncated_payload() {
        let buf = encode_data(1, 1, b"abc");
        // Corrupt payload_len to be larger than actual bytes
        let mut bad = buf.clone();
        // LosslessSessionHeader::LEN + 8 (index) position payload_len (4 bytes)
        let pos = LosslessSessionHeader::LEN + 8;
        bad[pos..pos + 4].copy_from_slice(&(9999u32.to_be_bytes()));
        assert!(decode_data(&bad).is_none());
    }

    #[test]
    fn encode_control_into_matches_encode_control() {
        let ctrls = vec![
            LosslessSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123456,
            },
            LosslessSessionControl::FecManifest {
                chunk_size: 4096,
                total_bytes: 123456,
                fec: FecManifest::new_raptorq(64, 1400),
            },
            LosslessSessionControl::Ready { node_id: 99 },
            LosslessSessionControl::FecCapabilities {
                node_id: 99,
                capabilities: FecCapabilities::default(),
            },
            LosslessSessionControl::Ack { up_to: 77 },
            LosslessSessionControl::FecStatus {
                status: FecStatus {
                    block_id: 3,
                    deficit_symbols: 1,
                },
            },
            LosslessSessionControl::Eot { last_index: 15 },
        ];

        for ctrl in ctrls {
            // Encode using the original heap-allocating version
            let heap_encoded = encode_control(42, &ctrl);

            // Encode using the stack-buffer version
            let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
            let stack_encoded = encode_control_into(&mut buf, 42, &ctrl);

            // Should produce identical output
            assert_eq!(
                heap_encoded.as_slice(),
                stack_encoded,
                "encode_control_into should produce same output as encode_control for {:?}",
                ctrl
            );

            // Both should decode correctly
            let (_, decoded_heap) = decode_control(&heap_encoded).expect("decode heap");
            let (_, decoded_stack) = decode_control(stack_encoded).expect("decode stack");
            assert_eq!(decoded_heap, ctrl);
            assert_eq!(decoded_stack, ctrl);
        }
    }

    #[test]
    fn decode_control_rejects_short_bodies() {
        // Start from a valid manifest and then truncate body bytes
        let good = encode_control(
            9,
            &LosslessSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123,
            },
        );
        let mut bad = good.clone();
        // Truncate to just header (no body)
        bad.truncate(LosslessSessionHeader::LEN);
        assert!(decode_control(&bad).is_none());

        // Ready requires 8 bytes; provide fewer
        let ready = encode_control(1, &LosslessSessionControl::Ready { node_id: 7 });
        let mut bad_ready = ready.clone();
        bad_ready.truncate(LosslessSessionHeader::LEN + 4);
        assert!(decode_control(&bad_ready).is_none());

        // Ack requires 8 bytes
        let ack = encode_control(1, &LosslessSessionControl::Ack { up_to: 1 });
        let mut bad_ack = ack.clone();
        bad_ack.truncate(LosslessSessionHeader::LEN + 6);
        assert!(decode_control(&bad_ack).is_none());

        // EOT requires 8 bytes (index only)
        let eot = encode_control(1, &LosslessSessionControl::Eot { last_index: 42 });
        let mut bad_eot = eot.clone();
        bad_eot.truncate(LosslessSessionHeader::LEN + 4);
        assert!(decode_control(&bad_eot).is_none());

        let fec_manifest = encode_control(
            1,
            &LosslessSessionControl::FecManifest {
                chunk_size: 1024,
                total_bytes: 4096,
                fec: FecManifest::new_raptorq(32, 1400),
            },
        );
        let mut bad_fec_manifest = fec_manifest.clone();
        bad_fec_manifest.truncate(LosslessSessionHeader::LEN + 10);
        assert!(decode_control(&bad_fec_manifest).is_none());

        let fec_caps = encode_control(
            1,
            &LosslessSessionControl::FecCapabilities {
                node_id: 7,
                capabilities: FecCapabilities::default(),
            },
        );
        let mut bad_fec_caps = fec_caps.clone();
        bad_fec_caps.truncate(LosslessSessionHeader::LEN + 12);
        assert!(decode_control(&bad_fec_caps).is_none());

        let fec_status = encode_control(
            1,
            &LosslessSessionControl::FecStatus {
                status: FecStatus {
                    block_id: 1,
                    deficit_symbols: 2,
                },
            },
        );
        let mut bad_fec_status = fec_status.clone();
        bad_fec_status.truncate(LosslessSessionHeader::LEN + 8);
        assert!(decode_control(&bad_fec_status).is_none());
    }

    #[test]
    fn unknown_fec_scheme_is_preserved_for_clean_rejection() {
        let control = LosslessSessionControl::FecManifest {
            chunk_size: 1200,
            total_bytes: 8192,
            fec: FecManifest {
                protocol_version: LOSSLESS_SESSION_FEC_VERSION,
                scheme: 99,
                symbols_per_block: 32,
                symbol_size: 1200,
            },
        };
        let buf = encode_control(3, &control);
        let (_, decoded) = decode_control(&buf).expect("decode control with unknown scheme");
        let LosslessSessionControl::FecManifest { fec, .. } = decoded else {
            panic!("expected fec manifest");
        };
        assert_eq!(fec.scheme, 99);
        assert!(fec.scheme_kind().is_none());
    }

    #[test]
    fn fec_controls_force_v2_header_when_requested_with_v1() {
        let control = LosslessSessionControl::FecStatus {
            status: FecStatus {
                block_id: 10,
                deficit_symbols: 1,
            },
        };
        let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
        let frame =
            encode_control_into_with_version(&mut buf, 11, LOSSLESS_SESSION_BASE_VERSION, &control);
        let (hdr, decoded) = decode_control(frame).expect("decode control");
        assert_eq!(hdr.version, LOSSLESS_SESSION_FEC_VERSION);
        assert_eq!(decoded, control);
    }
}
