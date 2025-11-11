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
    Sack = 4,
    Repair = 5,
    Eot = 6,
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

/// DATA payload header (follows `RlmHeader` when kind == Data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RlmData {
    pub index: u64,
    pub payload_len: u32,
    // followed by payload bytes
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
    /// SACK encodes missing ranges beyond the cumulative `base`.
    /// Each run is `(start_delta_from_base, len)`, both u16.
    Sack {
        base: u64,
        runs: Vec<(u16, u16)>,
    },
    /// Targeted repair requests for specific chunk indices.
    Repair {
        indices: Vec<u64>,
    },
    /// End-of-transfer marker with the last expected chunk and optional checksum.
    Eot {
        last_index: u64,
        checksum: Option<[u8; 32]>,
    },
}

/// Encode a DATA frame (header + RlmData + payload) into a fresh Vec<u8>.
pub fn encode_data(session_id: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 4 + payload.len() as u32; // RlmData
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
    // RlmData
    out[RlmHeader::LEN..RlmHeader::LEN + 8].copy_from_slice(&index.to_be_bytes());
    out[RlmHeader::LEN + 8..RlmHeader::LEN + 12]
        .copy_from_slice(&(payload.len() as u32).to_be_bytes());
    out[RlmHeader::LEN + 12..].copy_from_slice(payload);
    out
}

/// Try to decode a DATA frame; returns (header, data header, payload slice).
pub fn decode_data(buf: &[u8]) -> Option<(RlmHeader, RlmData, &[u8])> {
    let (hdr, off) = RlmHeader::decode_from(buf)?;
    if hdr.kind != RlmKind::Data {
        return None;
    }
    if buf.len() < off + hdr.body_len as usize || hdr.body_len < 12 {
        return None;
    }
    let index = u64::from_be_bytes(buf[off..off + 8].try_into().ok()?);
    let payload_len = u32::from_be_bytes(buf[off + 8..off + 12].try_into().ok()?);
    let start = off + 12;
    let end = start + payload_len as usize;
    if end > buf.len() {
        return None;
    }
    Some((
        hdr,
        RlmData { index, payload_len },
        &buf[start..end],
    ))
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
        Sack { base, runs } => {
            let mut b = Vec::with_capacity(8 + 2 + runs.len() * 4);
            b.extend_from_slice(&base.to_be_bytes());
            let n: u16 = runs.len().min(u16::MAX as usize) as u16;
            b.extend_from_slice(&n.to_be_bytes());
            for (start_delta, len) in runs.iter().take(n as usize) {
                b.extend_from_slice(&start_delta.to_be_bytes());
                b.extend_from_slice(&len.to_be_bytes());
            }
            (RlmCtrlKind::Sack as u8, b)
        }
        Repair { indices } => {
            let n: u16 = indices.len().min(u16::MAX as usize) as u16;
            let mut b = Vec::with_capacity(2 + (n as usize) * 8);
            b.extend_from_slice(&n.to_be_bytes());
            for idx in indices.iter().take(n as usize) {
                b.extend_from_slice(&idx.to_be_bytes());
            }
            (RlmCtrlKind::Repair as u8, b)
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
        x if x == RlmCtrlKind::Sack as u8 => {
            if body.len() < 10 {
                return None;
            }
            let base = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let n = u16::from_be_bytes(body[8..10].try_into().ok()?);
            let mut runs = Vec::with_capacity(n as usize);
            let mut i = 10usize;
            for _ in 0..n {
                if i + 4 > body.len() {
                    return None;
                }
                let start_delta = u16::from_be_bytes(body[i..i + 2].try_into().ok()?);
                let len = u16::from_be_bytes(body[i + 2..i + 4].try_into().ok()?);
                runs.push((start_delta, len));
                i += 4;
            }
            Sack { base, runs }
        }
        x if x == RlmCtrlKind::Repair as u8 => {
            if body.len() < 2 {
                return None;
            }
            let n = u16::from_be_bytes(body[0..2].try_into().ok()?);
            let mut i = 2usize;
            let mut indices = Vec::with_capacity(n as usize);
            for _ in 0..n {
                if i + 8 > body.len() {
                    return None;
                }
                let idx = u64::from_be_bytes(body[i..i + 8].try_into().ok()?);
                indices.push(idx);
                i += 8;
            }
            Repair { indices }
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
            Eot { last_index, checksum }
        }
        _ => return None,
    };
    Some((hdr, ctrl))
}

/// Merge and normalize SACK gap runs encoded as `(start_delta_from_base, len)` pairs.
/// Input may contain overlapping or adjacent ranges; output is sorted and coalesced.
pub fn coalesce_sack_runs(mut runs: Vec<(u16, u16)>) -> Vec<(u16, u16)> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|r| r.0);
    let mut out: Vec<(u16, u16)> = Vec::with_capacity(runs.len());
    let mut cur = runs[0];
    for (s, l) in runs.into_iter().skip(1) {
        let cur_end = cur.0.saturating_add(cur.1);
        if s <= cur_end { // overlap or adjacent
            let new_end = cur_end.max(s.saturating_add(l));
            cur.1 = new_end.saturating_sub(cur.0);
        } else {
            out.push(cur);
            cur = (s, l);
        }
    }
    out.push(cur);
    out
}

use std::collections::BTreeSet;

/// Build SACK gap runs given a cumulative base and the set of received chunk indices in (base, high].
pub fn build_gap_runs(base: u64, highest_seen: u64, received: &BTreeSet<u64>) -> Vec<(u16, u16)> {
    if highest_seen <= base {
        return Vec::new();
    }
    let mut runs: Vec<(u16, u16)> = Vec::new();
    let mut cur_start: Option<u64> = None;
    // Iterate inclusive range (base+1 ..= highest_seen) and emit gaps
    for idx in (base + 1)..=highest_seen {
        let have = received.contains(&idx);
        if !have {
            if cur_start.is_none() {
                cur_start = Some(idx);
            }
        } else if let Some(start) = cur_start.take() {
            // Encode the gap (start..idx) relative to base, but constrain into u16 window.
            let full_len = idx - start; // >0 by construction
            let full_delta = start - base; // >=1
            if full_delta <= u16::MAX as u64 {
                let mut remaining = full_len;
                let mut seg_delta_u64 = full_delta;
                while remaining > 0 {
                    let seg_len_u64 = remaining.min(u16::MAX as u64);
                    let seg_delta = seg_delta_u64 as u16;
                    let seg_len = seg_len_u64 as u16;
                    runs.push((seg_delta, seg_len));
                    remaining -= seg_len_u64;
                    // advance delta by the emitted segment; if it exceeds u16, we stop emitting
                    seg_delta_u64 = match seg_delta_u64.checked_add(seg_len_u64) {
                        Some(v) if v <= u16::MAX as u64 => v,
                        _ => break,
                    };
                }
            }
        }
    }
    if let Some(start) = cur_start {
        let full_len = (highest_seen + 1).saturating_sub(start);
        let full_delta = start.saturating_sub(base);
        if full_len > 0 && full_delta <= u16::MAX as u64 {
            let mut remaining = full_len;
            let mut seg_delta_u64 = full_delta;
            while remaining > 0 {
                let seg_len_u64 = remaining.min(u16::MAX as u64);
                runs.push((seg_delta_u64 as u16, seg_len_u64 as u16));
                remaining -= seg_len_u64;
                seg_delta_u64 = match seg_delta_u64.checked_add(seg_len_u64) {
                    Some(v) if v <= u16::MAX as u64 => v,
                    _ => break,
                };
            }
        }
    }
    // Runs are already bounded; coalescing keeps adjacent segments together without overflow.
    coalesce_sack_runs(runs)
}

/// Compute cumulative ACK base (`expected-1`) and SACK gap runs.
pub fn build_ack_and_sack(expected: u64, received: &BTreeSet<u64>, highest_seen: u64) -> (u64, Vec<(u16, u16)>) {
    let base = expected.saturating_sub(1);
    let runs = build_gap_runs(base, highest_seen, received);
    (base, runs)
}

/// Choose minimal REPAIR indices for a timeout on `expected`.
pub fn choose_repair_indices(expected: u64) -> Vec<u64> { vec![expected] }

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
    if low == "all" { return Some(AckPolicy::All); }
    if let Some(rest) = low.strip_prefix("k:") {
        let n: u32 = rest.parse().ok()?;
        if n == 0 || n > u16::MAX as u32 { return None; }
        return Some(AckPolicy::KofN(n as u16));
    }
    if let Some(rest) = low.strip_prefix("frac:") {
        let p: f32 = rest.parse().ok()?;
        if p <= 0.0 || p > 1.0 { return None; }
        return Some(AckPolicy::Fraction(p));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

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
            RlmControl::Manifest { chunk_size: 4096, total_bytes: 123456, checksum_algo: 1, options: 0 },
            RlmControl::Ready { node_id: 99 },
            RlmControl::Ack { up_to: 77 },
            RlmControl::Sack { base: 10, runs: vec![(1,3), (10,2)] },
            RlmControl::Repair { indices: vec![2,4,6,8] },
            RlmControl::Eot { last_index: 1024, checksum: None },
        ];

        for c in ctrls {
            let buf = encode_control(7, &c);
            let (hdr, parsed) = decode_control(&buf).expect("decode ctrl");
            assert_eq!(hdr.kind as u8, RlmKind::Control as u8);
            assert_eq!(hdr.session_id, 7);
            assert_eq!(parsed, c);
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = encode_data(1, 1, b"x");
        buf[0] = 0; // break magic
        assert!(decode_data(&buf).is_none());
    }

    #[test]
    fn coalesce_sack_runs_merges_and_sorts() {
        let runs = vec![(5,2), (1,3), (3,2), (8,1)];
        // (1..3)->(1,3) and (3..5)->(3,2) merge into (1,4); then (5,2) adjacent → (1,6)
        let out = coalesce_sack_runs(runs);
        assert_eq!(out, vec![(1,6), (8,1)]);
    }

    #[test]
    fn build_gap_runs_from_received_set() {
        let base = 10u64;
        let highest = 20u64;
        let mut recvd = BTreeSet::new();
        // receive 11, 12, 15, 19, 20 → gaps (13,2) and (16,3)
        for v in [11, 12, 15, 19, 20] { recvd.insert(v); }
        let gaps = build_gap_runs(base, highest, &recvd);
        assert_eq!(gaps, vec![(3,2), (6,3)]);
    }

    #[test]
    fn build_ack_and_sack_base_and_runs() {
        let mut recvd = BTreeSet::new();
        for v in [2u64, 4, 5, 7] { recvd.insert(v); }
        let (base, runs) = build_ack_and_sack(2, &recvd, 7);
        assert_eq!(base, 1);
        // missing 3 then 6
        assert_eq!(runs, vec![(2,1), (5,1)]);
    }

    #[test]
    fn build_gap_runs_bounds_large_ranges() {
        let base = 0u64;
        let highest = 70_000u64; // exceeds u16::MAX
        let recvd = BTreeSet::new(); // nothing received → one giant gap
        let gaps = build_gap_runs(base, highest, &recvd);
        assert!(!gaps.is_empty());
        for (d, l) in gaps {
            assert!(d <= u16::MAX);
            assert!(l <= u16::MAX);
        }
    }

    #[test]
    fn parse_ack_policy_variants() {
        assert_eq!(parse_ack_policy("all"), Some(AckPolicy::All));
        assert_eq!(parse_ack_policy("ALL"), Some(AckPolicy::All));
        assert_eq!(parse_ack_policy("k:3"), Some(AckPolicy::KofN(3)));
        assert_eq!(parse_ack_policy("frac:0.75"), Some(AckPolicy::Fraction(0.75)));
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
            &RlmControl::Manifest { chunk_size: 4096, total_bytes: 123, checksum_algo: 0, options: 0 },
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

        // Sack requires at least 10 bytes (base + count)
        let sack = encode_control(1, &RlmControl::Sack { base: 10, runs: vec![(1,1)] });
        let mut bad_sack = sack.clone();
        bad_sack.truncate(RlmHeader::LEN + 9);
        assert!(decode_control(&bad_sack).is_none());

        // Repair requires at least 2 bytes (count)
        let repair = encode_control(1, &RlmControl::Repair { indices: vec![1,2] });
        let mut bad_repair = repair.clone();
        bad_repair.truncate(RlmHeader::LEN + 1);
        assert!(decode_control(&bad_repair).is_none());

        // EOT requires 9 bytes minimum (index + flag)
        let eot = encode_control(1, &RlmControl::Eot { last_index: 42, checksum: None });
        let mut bad_eot = eot.clone();
        bad_eot.truncate(RlmHeader::LEN + 8);
        assert!(decode_control(&bad_eot).is_none());
    }
}
