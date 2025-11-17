use serde::{Deserialize, Serialize};

/// Magic constant (legacy "RLM1" ASCII) used by reliable session frames.
pub const RELIABLE_SESSION_MAGIC: u32 = 0x524C_4D31;
pub const RELIABLE_SESSION_VERSION: u8 = 1;

/// Top-level frame kind carried in the header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReliableSessionKind {
    Data = 1,
    Control = 2,
}

/// Control sub-kind (only meaningful when kind == Control).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReliableSessionCtrlKind {
    Manifest = 1,
    Ready = 2,
    Ack = 3,
    Eot = 4,
}

/// Fixed header for both DATA and CONTROL frames.
///
/// Layout (big-endian):
/// - magic:      u32  (RELIABLE_SESSION_MAGIC)
/// - version:    u8   (RELIABLE_SESSION_VERSION)
/// - kind:       u8   (1=Data, 2=Control)
/// - ctrl_kind:  u8   (ReliableSessionCtrlKind value when kind=Control, else 0)
/// - reserved:   u8   (0; alignment/padding)
/// - session_id: u64  (flow/session demux)
/// - body_len:   u32  (number of bytes following the header)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReliableSessionHeader {
    pub magic: u32,
    pub version: u8,
    pub kind: ReliableSessionKind,
    pub ctrl_kind: u8,
    pub session_id: u64,
    pub body_len: u32,
}

impl ReliableSessionHeader {
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
        if magic != RELIABLE_SESSION_MAGIC {
            return None;
        }
        let version = buf[4];
        if version != RELIABLE_SESSION_VERSION {
            return None;
        }
        let kind = match buf[5] {
            1 => ReliableSessionKind::Data,
            2 => ReliableSessionKind::Control,
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
pub struct ReliableSessionData {
    pub index: u64,
    pub payload_len: u32,
}

/// CONTROL payload variants (follows `ReliableSessionHeader` when kind == Control).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReliableSessionControl {
    Manifest {
        chunk_size: u32,
        total_bytes: u64,
    },
    Ready {
        node_id: u64,
    },
    Ack {
        up_to: u64,
    },
    /// End-of-transfer marker with the last expected chunk.
    Eot {
        last_index: u64,
    },
}

/// Encode a DATA frame (header + ReliableSessionData + payload) into a fresh Vec<u8>.
pub fn encode_data(session_id: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 4 + payload.len() as u32;
    let mut out = vec![0u8; ReliableSessionHeader::LEN + body_len as usize];
    ReliableSessionHeader {
        magic: RELIABLE_SESSION_MAGIC,
        version: RELIABLE_SESSION_VERSION,
        kind: ReliableSessionKind::Data,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..ReliableSessionHeader::LEN]);
    let mut pos = ReliableSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&index.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    pos += 4;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

/// Try to decode a DATA frame; returns (header, data header, payload slice).
pub fn decode_data(buf: &[u8]) -> Option<(ReliableSessionHeader, ReliableSessionData, &[u8])> {
    let (hdr, off) = ReliableSessionHeader::decode_from(buf)?;
    if hdr.kind != ReliableSessionKind::Data {
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
        ReliableSessionData { index, payload_len },
        &buf[pos..payload_end],
    ))
}

/// Maximum size of a control frame: header (20) + largest body (Manifest: 12) = 32 bytes
pub const MAX_CONTROL_FRAME_SIZE: usize = ReliableSessionHeader::LEN + 12;

/// Encode a CONTROL frame into the provided buffer, returning the number of bytes written.
/// The buffer must be at least MAX_CONTROL_FRAME_SIZE bytes.
///
/// Returns the slice of the buffer containing the encoded frame.
pub fn encode_control_into<'a>(
    buf: &'a mut [u8],
    session_id: u64,
    control: &ReliableSessionControl,
) -> &'a [u8] {
    use ReliableSessionControl::*;

    // Encode the body directly into the buffer after the header
    let (ctrl_kind, body_len) = match control {
        Manifest {
            chunk_size,
            total_bytes,
        } => {
            let body_start = ReliableSessionHeader::LEN;
            buf[body_start..body_start + 4].copy_from_slice(&chunk_size.to_be_bytes());
            buf[body_start + 4..body_start + 12].copy_from_slice(&total_bytes.to_be_bytes());
            (ReliableSessionCtrlKind::Manifest as u8, 12)
        }
        Ready { node_id } => {
            let body_start = ReliableSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&node_id.to_be_bytes());
            (ReliableSessionCtrlKind::Ready as u8, 8)
        }
        Ack { up_to } => {
            let body_start = ReliableSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&up_to.to_be_bytes());
            (ReliableSessionCtrlKind::Ack as u8, 8)
        }
        Eot { last_index } => {
            let body_start = ReliableSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&last_index.to_be_bytes());
            (ReliableSessionCtrlKind::Eot as u8, 8)
        }
    };

    // Encode the header at the start of the buffer
    ReliableSessionHeader {
        magic: RELIABLE_SESSION_MAGIC,
        version: RELIABLE_SESSION_VERSION,
        kind: ReliableSessionKind::Control,
        ctrl_kind,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..ReliableSessionHeader::LEN]);

    &buf[..ReliableSessionHeader::LEN + body_len]
}

/// Encode a CONTROL frame (header + control body) into a fresh Vec<u8>.
///
/// Note: Consider using `encode_control_into` with a stack buffer for better performance.
pub fn encode_control(session_id: u64, control: &ReliableSessionControl) -> Vec<u8> {
    let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
    let frame = encode_control_into(&mut buf, session_id, control);
    frame.to_vec()
}

/// Try to decode a CONTROL frame; returns (header, parsed control).
pub fn decode_control(buf: &[u8]) -> Option<(ReliableSessionHeader, ReliableSessionControl)> {
    use ReliableSessionControl::*;
    let (hdr, off) = ReliableSessionHeader::decode_from(buf)?;
    if hdr.kind != ReliableSessionKind::Control {
        return None;
    }
    // Guard against out-of-bounds before slicing body to avoid panics.
    if buf.len() < off + hdr.body_len as usize {
        return None;
    }
    let body = &buf[off..off + hdr.body_len as usize];
    let ctrl = match hdr.ctrl_kind {
        x if x == ReliableSessionCtrlKind::Manifest as u8 => {
            if body.len() < 4 + 8 {
                return None;
            }
            let chunk_size = u32::from_be_bytes(body[0..4].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[4..12].try_into().ok()?);
            Manifest {
                chunk_size,
                total_bytes,
            }
        }
        x if x == ReliableSessionCtrlKind::Ready as u8 => {
            if body.len() < 8 {
                return None;
            }
            let node_id = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ready { node_id }
        }
        x if x == ReliableSessionCtrlKind::Ack as u8 => {
            if body.len() < 8 {
                return None;
            }
            let up_to = u64::from_be_bytes(body[0..8].try_into().ok()?);
            Ack { up_to }
        }
        x if x == ReliableSessionCtrlKind::Eot as u8 => {
            if body.len() < 8 {
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
        assert_eq!(hdr.magic, RELIABLE_SESSION_MAGIC);
        assert_eq!(hdr.version, RELIABLE_SESSION_VERSION);
        assert_eq!(hdr.kind as u8, ReliableSessionKind::Data as u8);
        assert_eq!(hdr.session_id, 42);
        assert_eq!(data.index, 7);
        assert_eq!(data.payload_len as usize, payload.len());
        assert_eq!(body, payload);
    }

    #[test]
    fn roundtrip_controls() {
        let ctrls = vec![
            ReliableSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123456,
            },
            ReliableSessionControl::Ready { node_id: 99 },
            ReliableSessionControl::Ack { up_to: 77 },
            ReliableSessionControl::Eot { last_index: 15 },
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
    fn decode_data_rejects_truncated_payload() {
        let buf = encode_data(1, 1, b"abc");
        // Corrupt payload_len to be larger than actual bytes
        let mut bad = buf.clone();
        // ReliableSessionHeader::LEN + 8 (index) position payload_len (4 bytes)
        let pos = ReliableSessionHeader::LEN + 8;
        bad[pos..pos + 4].copy_from_slice(&(9999u32.to_be_bytes()));
        assert!(decode_data(&bad).is_none());
    }

    #[test]
    fn encode_control_into_matches_encode_control() {
        let ctrls = vec![
            ReliableSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123456,
            },
            ReliableSessionControl::Ready { node_id: 99 },
            ReliableSessionControl::Ack { up_to: 77 },
            ReliableSessionControl::Eot { last_index: 15 },
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
            &ReliableSessionControl::Manifest {
                chunk_size: 4096,
                total_bytes: 123,
            },
        );
        let mut bad = good.clone();
        // Truncate to just header (no body)
        bad.truncate(ReliableSessionHeader::LEN);
        assert!(decode_control(&bad).is_none());

        // Ready requires 8 bytes; provide fewer
        let ready = encode_control(1, &ReliableSessionControl::Ready { node_id: 7 });
        let mut bad_ready = ready.clone();
        bad_ready.truncate(ReliableSessionHeader::LEN + 4);
        assert!(decode_control(&bad_ready).is_none());

        // Ack requires 8 bytes
        let ack = encode_control(1, &ReliableSessionControl::Ack { up_to: 1 });
        let mut bad_ack = ack.clone();
        bad_ack.truncate(ReliableSessionHeader::LEN + 6);
        assert!(decode_control(&bad_ack).is_none());

        // EOT requires 8 bytes (index only)
        let eot = encode_control(1, &ReliableSessionControl::Eot { last_index: 42 });
        let mut bad_eot = eot.clone();
        bad_eot.truncate(ReliableSessionHeader::LEN + 4);
        assert!(decode_control(&bad_eot).is_none());
    }
}
