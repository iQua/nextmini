use serde::{Deserialize, Serialize};

/// "RLM1" in ASCII.
pub const RLM_MAGIC: u32 = 0x524C_4D31;
pub const RLM_VERSION: u8 = 1;

/// Top-level frame kind carried in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RlmKind {
    Data = 1,
    Control = 2,
}

/// Control sub-kind (only meaningful when kind == Control).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RlmCtrlKind {
    Manifest = 1,
    Ready = 2,
    Ack = 3,
    Eot = 4,
}

/// Fixed header for both DATA and CONTROL frames.
///
/// Layout (big-endian):
/// - magic:      u32  (RLM_MAGIC)
/// - version:    u8   (RLM_VERSION)
/// - kind:       u8   (1=Data, 2=Control)
/// - ctrl_kind:  u8   (RlmCtrlKind value when kind=Control, else 0)
/// - reserved:   u8   (0; alignment/padding)
/// - session_id: u64  (flow/session demux)
/// - body_len:   u32  (number of bytes following the header)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RlmHeader {
    pub magic: u32,
    pub version: u8,
    pub kind: RlmKind,
    pub ctrl_kind: u8,
    pub session_id: u64,
    pub body_len: u32,
}

impl RlmHeader {
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
        if magic != RLM_MAGIC {
            return None;
        }
        let version = buf[4];
        if version != RLM_VERSION {
            return None;
        }
        let kind = match buf[5] {
            1 => RlmKind::Data,
            2 => RlmKind::Control,
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
pub struct RlmData {
    pub index: u64,
    pub payload_len: u32,
}

/// CONTROL payload variants (follows `RlmHeader` when kind == Control).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RlmControl {
    Manifest {
        chunk_size: u32,
        total_bytes: u64,
        checksum_algo: u8, // 0: none, 1: sha256
        options: u32,
    },
    Ready {
        node_id: u64,
    },
    Ack {
        up_to: u64,
    },
    /// End-of-transfer marker with the last expected chunk and optional checksum.
    Eot {
        last_index: u64,
        checksum: Option<[u8; 32]>,
    },
}

/// Encode a DATA frame (header + RlmData + payload) into a fresh Vec<u8>.
pub fn encode_data(session_id: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 4 + payload.len() as u32;
    let mut out = vec![0u8; RlmHeader::LEN + body_len as usize];
    RlmHeader {
        magic: RLM_MAGIC,
        version: RLM_VERSION,
        kind: RlmKind::Data,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..RlmHeader::LEN]);
    let mut pos = RlmHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&index.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

/// Try to decode a DATA frame; returns (header, data header, payload slice).
pub fn decode_data(buf: &[u8]) -> Option<(RlmHeader, RlmData, &[u8])> {
    let (hdr, off) = RlmHeader::decode_from(buf)?;
    if hdr.kind != RlmKind::Data {
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
    Some((hdr, RlmData { index, payload_len }, &buf[pos..payload_end]))
}

/// Encode a CONTROL frame (header + control body) into a fresh Vec<u8>.
pub fn encode_control(session_id: u64, control: &RlmControl) -> Vec<u8> {
    use RlmControl::*;
    let (ctrl_kind, body_bytes) = match control {
        Manifest {
            chunk_size,
            total_bytes,
            checksum_algo,
            options,
        } => {
            let mut b = vec![0u8; 4 + 8 + 1 + 4];
            b[0..4].copy_from_slice(&chunk_size.to_be_bytes());
            b[4..12].copy_from_slice(&total_bytes.to_be_bytes());
            b[12] = *checksum_algo;
            b[13..17].copy_from_slice(&options.to_be_bytes());
            (RlmCtrlKind::Manifest as u8, b)
        }
        Ready { node_id } => {
            let mut b = vec![0u8; 8];
            b[..8].copy_from_slice(&node_id.to_be_bytes());
            (RlmCtrlKind::Ready as u8, b)
        }
        Ack { up_to } => {
            let mut b = vec![0u8; 8];
            b[..8].copy_from_slice(&up_to.to_be_bytes());
            (RlmCtrlKind::Ack as u8, b)
        }
        Eot {
            last_index,
            checksum,
        } => {
            let mut b = Vec::with_capacity(8 + 1 + 32);
            b.extend_from_slice(&last_index.to_be_bytes());
            match checksum {
                Some(arr) => {
                    b.push(1);
                    b.extend_from_slice(arr);
                }
                None => b.push(0),
            }
            (RlmCtrlKind::Eot as u8, b)
        }
    };

    let body_len = body_bytes.len() as u32;
    let mut out = vec![0u8; RlmHeader::LEN + body_len as usize];
    RlmHeader {
        magic: RLM_MAGIC,
        version: RLM_VERSION,
        kind: RlmKind::Control,
        ctrl_kind,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..RlmHeader::LEN]);
    out[RlmHeader::LEN..].copy_from_slice(&body_bytes);
    out
}

/// Try to decode a CONTROL frame; returns (header, parsed control).
pub fn decode_control(buf: &[u8]) -> Option<(RlmHeader, RlmControl)> {
    use RlmControl::*;
    let (hdr, off) = RlmHeader::decode_from(buf)?;
    if hdr.kind != RlmKind::Control {
        return None;
    }
    // Guard against out-of-bounds before slicing body to avoid panics.
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let body = &buf[off..off + hdr.body_len as usize];
    let ctrl = match hdr.ctrl_kind {
        x if x == RlmCtrlKind::Manifest as u8 => {
            if body.len() < 4 + 8 + 1 + 4 {
                return None;
            }
            let chunk_size = u32::from_be_bytes(body[0..4].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[4..12].try_into().ok()?);
            let checksum_algo = body[12];
            let options = u32::from_be_bytes(body[13..17].try_into().ok()?);
            Manifest {
                chunk_size,
                total_bytes,
                checksum_algo,
                options,
            }
        }
        x if x == RlmCtrlKind::Ready as u8 => {
            if body.len() < 8 {
                return None;
            }
            let node_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ready { node_id }
        }
        x if x == RlmCtrlKind::Ack as u8 => {
            if body.len() < 8 {
                return None;
            }
            let up_to = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ack { up_to }
        }
        x if x == RlmCtrlKind::Eot as u8 => {
            if body.len() < 8 + 1 {
                return None;
            }
            let last_index = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let has_sum = body[8];
            let checksum = if has_sum == 1 {
                if body.len() < 8 + 1 + 32 {
                    return None;
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&body[9..9 + 32]);
                Some(arr)
            } else {
                None
            };
            Eot {
                last_index,
                checksum,
            }
        }
        _ => return None,
    };
    Some((hdr, ctrl))
}

/// Ack policy controls sender retirement/commit logic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AckPolicy {
    All,
    KofN(u16),
    Fraction(f32),
}

/// Parse ack policy strings: "all", "k:N" where N>=1, or "frac:P" where 0<P<=1.
pub fn parse_ack_policy(s: &str) -> Option<AckPolicy> {
    let low = s.trim().to_ascii_lowercase();
    if low == "all" {
        return Some(AckPolicy::All);
    }
    if let Some(rest) = low.strip_prefix("k:") {
        let n: u32 = rest.parse().ok()?;
        if n == 0 || n > u16::MAX as u32 {
            return None;
        }
        return Some(AckPolicy::KofN(n as u16));
    }
    if let Some(rest) = low.strip_prefix("frac:") {
        let p: f32 = rest.parse().ok()?;
        if p <= 0.0 || p > 1.0 {
            return None;
        }
        return Some(AckPolicy::Fraction(p));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_data() {
        let payload = b"hello world";
        let buf = encode_data(42, 7, payload);
        let (hdr, data, body) = decode_data(&buf).expect("decode data");
        assert_eq!(hdr.magic, RLM_MAGIC);
        assert_eq!(hdr.version, RLM_VERSION);
        assert_eq!(hdr.kind as u8, RlmKind::Data as u8);
        assert_eq!(hdr.session_id, 42);
        assert_eq!(data.index, 7);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
    }

    #[test]
    fn roundtrip_controls() {
        let ctrls = vec![
            RlmControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123456,
                checksum_algo: 1,
                options: 0,
            },
            RlmControl::Ready { node_id: 99 },
            RlmControl::Ack { up_to: 77 },
            RlmControl::Eot {
                last_index: 15,
                checksum: Some([0xAA; 32]),
            },
        ];
        for ctrl in ctrls {
            let buf = encode_control(77, &ctrl);
            let (hdr, decoded) = decode_control(&buf).expect("decode control");
            assert_eq!(hdr.session_id, 77);
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
    fn parse_ack_policy_variants() {
        assert_eq!(parse_ack_policy("all"), Some(AckPolicy::All));
        assert_eq!(parse_ack_policy("ALL"), Some(AckPolicy::All));
        assert_eq!(parse_ack_policy("k:3"), Some(AckPolicy::KofN(3)));
        assert_eq!(
            parse_ack_policy("frac:0.75"),
            Some(AckPolicy::Fraction(0.75))
        );
        assert_eq!(parse_ack_policy("k:0"), None);
        assert_eq!(parse_ack_policy("k:70000"), None);
        assert_eq!(parse_ack_policy("frac:0"), None);
        assert_eq!(parse_ack_policy("frac:1.2"), None);
        assert_eq!(parse_ack_policy("bogus"), None);
    }

    #[test]
    fn decode_data_rejects_truncated_payload() {
        let buf = encode_data(1, 1, b"abc");
        // Corrupt payload_len to be larger than actual bytes
        let mut bad = buf.clone();
        // RlmHeader::LEN + 8 (index) position payload_len (4 bytes)
        let pos = RlmHeader::LEN + 8;
        bad[pos..pos + 4].copy_from_slice(&(9999u32.to_be_bytes()));
        assert!(decode_data(&bad).is_none());
    }

    #[test]
    fn decode_control_rejects_short_bodies() {
        // Start from a valid manifest and then truncate body bytes
        let good = encode_control(
            9,
            &RlmControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123,
                checksum_algo: 0,
                options: 0,
            },
        );
        let mut bad = good.clone();
        // Truncate to just header (no body)
        bad.truncate(RlmHeader::LEN);
        assert!(decode_control(&bad).is_none());

        // Ready requires 8 bytes; provide fewer
        let ready = encode_control(1, &RlmControl::Ready { node_id: 7 });
        let mut bad_ready = ready.clone();
        bad_ready.truncate(RlmHeader::LEN + 4);
        assert!(decode_control(&bad_ready).is_none());

        // Ack requires 8 bytes
        let ack = encode_control(1, &RlmControl::Ack { up_to: 1 });
        let mut bad_ack = ack.clone();
        bad_ack.truncate(RlmHeader::LEN + 6);
        assert!(decode_control(&bad_ack).is_none());

        // EOT requires 9 bytes minimum (index + flag)
        let eot = encode_control(
            1,
            &RlmControl::Eot {
                last_index: 42,
                checksum: None,
            },
        );
        let mut bad_eot = eot.clone();
        bad_eot.truncate(RlmHeader::LEN + 8);
        assert!(decode_control(&bad_eot).is_none());
    }
}
