File: messages/src/rlm.rs
Change: **Add RLM v1 wire format (headers + control/data encode/decode)**

```rust
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
    /// SACK encodes *missing ranges* beyond the cumulative `base`.
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
            let mut indices = Vec::with_capacity(n as usize);
            let mut i = 2usize;
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
            if body.len() < 9 {
                return None;
            }
            let last_index = u64::from_be_bytes(body[0..8].try_into().ok()?);
            let flag = body[8];
            let checksum = if flag == 1 {
                if body.len() < 9 + 32 {
                    return None;
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&body[9..41]);
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
```

---

File: messages/src/lib.rs
Change: **Export the RLM module**

```rust
pub mod rlm;
```

---

File: dataplane/src/node/reliable/mod.rs
Change: **Introduce RLM engine façade, session IDs, params & policy**

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub type SessionId = u64;

#[derive(Clone, Debug)]
pub enum CompletionPolicy {
    All,
    Threshold(usize),
    Leader(usize),
}

impl CompletionPolicy {
    #[inline]
    pub fn should_retire(&self, acked_by: &std::collections::HashSet<usize>, receiver_count: usize) -> bool {
        match *self {
            CompletionPolicy::All => acked_by.len() == receiver_count,
            CompletionPolicy::Threshold(t) => acked_by.len() >= t.min(receiver_count),
            CompletionPolicy::Leader(id) => acked_by.contains(&id),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SenderParams {
    pub session_id: Option<SessionId>,
    pub group_ip: std::net::Ipv4Addr,
    pub receiver_ids: Vec<usize>,
    pub chunk_size: usize,
    pub window: usize,
    pub completion: CompletionPolicy,
}

#[derive(Clone, Debug)]
pub struct ReceiverParams {
    pub session_id: Option<SessionId>,
    pub group_ip: std::net::Ipv4Addr,
    pub ack_interval_ms: u64,
    pub nack_min_interval_ms: u64,
    pub jitter_ms: u64,
}

pub fn new_session_id() -> SessionId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub mod control;
pub mod sender;
pub mod receiver;
```

---

File: dataplane/src/node/reliable/control.rs
Change: **Add SACK utilities and NACK throttling helper**

```rust
use std::time::{Duration, Instant};

/// Merge and normalize SACK *gap* runs encoded as `(start_delta_from_base, len)`.
/// Input may contain overlapping or adjacent ranges; output is coalesced & sorted.
pub fn coalesce_sack_runs(mut runs: Vec<(u16, u16)>) -> Vec<(u16, u16)> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|r| r.0);
    let mut out: Vec<(u16, u16)> = Vec::with_capacity(runs.len());
    let mut cur = runs[0];
    for (s, l) in runs.into_iter().skip(1) {
        let cur_end = cur.0.saturating_add(cur.1);
        if s <= cur_end {
            // overlap or adjacency; extend
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

/// Build SACK *gap* runs given a cumulative base and the set of *received* chunk indices
/// in (base, high] (excludes everything <= base). Returns a compact set of missing runs.
pub fn build_gap_runs(
    base: u64,
    highest_seen: u64,
    received: &std::collections::BTreeSet<u64>,
) -> Vec<(u16, u16)> {
    if highest_seen <= base {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut cur_start: Option<u64> = None;
    for idx in base + 1..=highest_seen {
        let have = received.contains(&idx);
        if !have {
            if cur_start.is_none() {
                cur_start = Some(idx);
            }
        } else if let Some(start) = cur_start.take() {
            let len = (idx - start) as u16;
            let delta = (start - base) as u16;
            runs.push((delta, len));
        }
    }
    // tail gap?
    if let Some(start) = cur_start {
        let len = (highest_seen + 1 - start) as u16;
        let delta = (start - base) as u16;
        runs.push((delta, len));
    }
    coalesce_sack_runs(runs)
}

/// Simple per-chunk NACK limiter to avoid rapid repeats.
pub struct NackLimiter {
    last_for: Option<u64>,
    last_at: Instant,
    min_interval: Duration,
}

impl NackLimiter {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            last_for: None,
            last_at: Instant::now().saturating_sub(min_interval),
            min_interval,
        }
    }

    /// Returns true if a NACK for `chunk` is allowed at `now`.
    pub fn should_send(&mut self, chunk: u64, now: Instant) -> bool {
        match (self.last_for, now.saturating_duration_since(self.last_at)) {
            (Some(c), dt) if c == chunk && dt < self.min_interval => false,
            _ => {
                self.last_for = Some(chunk);
                self.last_at = now;
                true
            }
        }
    }

    /// Deterministic jitter in 0..=max_ms derived from (node_id, chunk).
    pub fn jitter_ms(node_id: usize, chunk: u64, max_ms: u64) -> u64 {
        if max_ms == 0 {
            return 0;
        }
        let salt = (node_id as u64)
            .wrapping_mul(1103515245)
            .wrapping_add(12345)
            .wrapping_add(chunk.rotate_left(13));
        salt % max_ms
    }
}
```

---

File: dataplane/src/node/reliable/sender.rs
Change: **Implement control processing for M1/M2 (ACK/SACK/REPAIR) and retirement**

```rust
use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::{CompletionPolicy};
use nextmini_messages::rlm::RlmControl;

/// Process a single control event from `from_node` and update inflight/repairs.
/// Returns a list of chunk indices that should be retired after this event.
pub fn process_control_event(
    from_node: usize,
    ctrl: &RlmControl,
    inflight: &mut BTreeMap<u64, HashSet<usize>>,
    resend_queue: &mut BTreeSet<u64>,
    receiver_count: usize,
    policy: &CompletionPolicy,
) -> Vec<u64> {
    match ctrl {
        RlmControl::Ack { up_to } => {
            let up = *up_to;
            // mark all <= up acknowledged by from_node
            for (_idx, acked_by) in inflight.range_mut(..=up) {
                acked_by.insert(from_node);
            }
            // collect retire candidates
            let mut completed = Vec::new();
            for (idx, acked_by) in inflight.range(..=up) {
                if policy.should_retire(acked_by, receiver_count) {
                    completed.push(*idx);
                }
            }
            completed
        }
        RlmControl::Sack { base, runs } => {
            // SACK encodes gaps (missing ranges) beyond base. Schedule resends for those in inflight.
            let mut scheduled = 0usize;
            for (delta, len) in runs {
                let start = *base + (*delta as u64);
                let end = start + (*len as u64);
                for idx in start..end {
                    if inflight.contains_key(&idx) {
                        resend_queue.insert(idx);
                        scheduled += 1;
                    }
                }
            }
            // no direct retire from SACK; wait for ACK
            Vec::new()
        }
        RlmControl::Repair { indices } => {
            let mut scheduled = 0usize;
            for idx in indices {
                if inflight.contains_key(idx) {
                    resend_queue.insert(*idx);
                    scheduled += 1;
                }
            }
            Vec::new()
        }
        RlmControl::Manifest { .. } | RlmControl::Ready { .. } | RlmControl::Eot { .. } => {
            // no-op for sender-side control path in M1/M2
            Vec::new()
        }
    }
}

/// Helper to apply retirement (removes from inflight and any pending resends).
pub fn retire_chunks(
    to_retire: &[u64],
    inflight: &mut BTreeMap<u64, HashSet<usize>>,
    resend_queue: &mut BTreeSet<u64>,
) {
    for idx in to_retire {
        inflight.remove(idx);
        resend_queue.remove(idx);
    }
}
```

---

File: dataplane/src/node/reliable/receiver.rs
Change: **SACK construction from receiver state and targeted REPAIR selection**

```rust
use std::collections::{BTreeMap, BTreeSet};

use super::control::build_gap_runs;

/// Compute cumulative ACK base (`expected-1`) and SACK *gap* runs given:
/// - `expected`: next missing chunk (1-based indexing)
/// - `received`: map of chunks already present beyond expected
/// - `highest_seen`: largest index seen so far (for bounding)
pub fn build_ack_and_sack(
    expected: u64,
    received: &BTreeSet<u64>,
    highest_seen: u64,
) -> (u64, Vec<(u16, u16)>) {
    let base = expected.saturating_sub(1);
    let runs = build_gap_runs(base, highest_seen, received);
    (base, runs)
}

/// Given a timeout on `expected`, choose minimal REPAIR request:
/// - Prefer a single index (expected) to avoid flooding; caller can coalesce.
pub fn choose_repair_indices(expected: u64) -> Vec<u64> {
    vec![expected]
}

/// Insert a newly arrived chunk into pending map and advance `expected` if possible.
pub fn on_chunk(
    idx: u64,
    bytes: bytes::Bytes,
    pending: &mut BTreeMap<u64, bytes::Bytes>,
    expected: &mut u64,
) {
    if idx < *expected {
        return;
    }
    pending.insert(idx, bytes);
    // advance expected while we have a contiguous prefix
    while pending.contains_key(expected) {
        pending.remove(expected);
        *expected += 1;
    }
}
```

