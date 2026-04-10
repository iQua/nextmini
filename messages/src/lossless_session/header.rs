use super::{LOSSLESS_SESSION_MAGIC, LOSSLESS_SESSION_VERSION, LosslessSessionKind};

/// Fixed header for both block and control frames.
///
/// Layout (big-endian):
/// - magic:      u32  (LOSSLESS_SESSION_MAGIC)
/// - version:    u8   (LOSSLESS_SESSION_VERSION)
/// - kind:       u8   (1=BlockData, 2=BlockSymbol, 3=Control, 4=MettleSymbol)
/// - ctrl_kind:  u8   (control sub-kind when kind=Control, else 0)
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

/// Raw header view used when callers need session/version diagnostics before
/// full kind-specific decoding succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LosslessSessionRawHeader {
    pub magic: u32,
    pub version: u8,
    pub kind: u8,
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
        let raw = peek_header(buf)?;
        if raw.version != LOSSLESS_SESSION_VERSION {
            return None;
        }
        let kind = match raw.kind {
            1 => LosslessSessionKind::BlockData,
            2 => LosslessSessionKind::BlockSymbol,
            3 => LosslessSessionKind::Control,
            4 => LosslessSessionKind::MettleSymbol,
            _ => return None,
        };
        Some((
            Self {
                magic: raw.magic,
                version: raw.version,
                kind,
                ctrl_kind: raw.ctrl_kind,
                session_id: raw.session_id,
                body_len: raw.body_len,
            },
            Self::LEN,
        ))
    }
}

#[inline]
pub fn peek_header(buf: &[u8]) -> Option<LosslessSessionRawHeader> {
    if buf.len() < LosslessSessionHeader::LEN {
        return None;
    }
    let magic = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    if magic != LOSSLESS_SESSION_MAGIC {
        return None;
    }
    Some(LosslessSessionRawHeader {
        magic,
        version: buf[4],
        kind: buf[5],
        ctrl_kind: buf[6],
        session_id: u64::from_be_bytes(buf[8..16].try_into().ok()?),
        body_len: u32::from_be_bytes(buf[16..20].try_into().ok()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lossless_session::{LosslessSessionControl, decode_control, encode_control};

    #[test]
    fn peek_header_exposes_unsupported_version_for_logging() {
        let mut buf = encode_control(17, &LosslessSessionControl::Ready);
        buf[4] = LOSSLESS_SESSION_VERSION - 1;

        let raw = peek_header(&buf).expect("raw header should still decode");
        assert_eq!(raw.session_id, 17);
        assert_eq!(raw.version, LOSSLESS_SESSION_VERSION - 1);
        assert!(LosslessSessionHeader::decode_from(&buf).is_none());
        assert!(decode_control(&buf).is_none());
    }
}
