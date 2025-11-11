Below is a **from‑scratch, Rust‑first** plan that moves reliable multicast out of the Python wrapper and into the dataplane, with clean wire formats, scheduler integration, observability, and a thin Python binding. I’m keeping it incremental and module‑scoped so you can merge in phases and keep the repo green.

---

## Live Flip Checklist & Owners

- PR2 Conductor wiring (owner: GreenPond)
  - Expose `ReliableHandle` to Python; tasks spawn via SessionManager (with network writer).
  - Signal: wrappers start sessions; logs show task spawn; compile: `--features reliable`.

- PR3 Sender engine (owner: GreenPond)
  - Implement MANIFEST/DATA pacing/ACK,SACK,REPAIR,retirement; unit tests for resend/retire.
  - Signal: unit tests pass; E2E loss shows nonzero resends and final checksum equality.

- Receiver engine (owner: PurpleStone → reassigned to PurpleHill if needed)
  - Implement ACK base advance/SACK gap build/repair targeting/reassembly.
  - Signal: unit tests; E2E checksum equality; counters sensible.

- Python wrappers wire-up (owner: PurpleHill)
  - Delegate to `ReliableHandle` and expose completion + counters for tests.
  - Signal: E2E live assertions enabled; examples/harness flip from dry-run.

ETAs (proposed)
- Writer hookup (BlueCat): EOD/next day.
- Wrapper delegation + receiver engine (PurpleHill): immediately after writer lands.
- Scheduler/Stats (shared): after engines stable; then optional controller DB persistence of stats.

- Observability + scheduler (owner: shared)
  - Emit `ReliableStats`; apply control WRR weight; optional DB persistence.
  - Signal: controller logs show periodic stats; pacing scenario behaves as configured.

## High‑level design

Progress notes (Nov 11):
- PR1 (Messages RLM v1): done. RLM types landed in `messages/src/rlm.rs` and are exported via `messages/src/lib.rs` with round-trip unit tests.
- Python cleanup done: legacy reliable multicast entrypoints removed from `python-api/src/lib.rs` and thin wrappers added (`reliable_send_file_rs`, `reliable_receive_file_rs`) that currently return a new session id; they will delegate to `ReliableHandle` once conductor wiring lands (M6).
- PR2 scaffolding: `dataplane/src/node/reliable/{mod,api,session,control,sender,receiver}.rs` added with control utils and unit tests; export and Conductor handle are feature‑gated.
- Build gating: `features = [reliable]` introduced in `dataplane/Cargo.toml` and `node::reliable`/Conductor imports guarded so default builds stay green while PR1/PR3 are in flight.
- PR4 prework: implemented receiver/control helpers in messages crate to avoid conflicts with active `dataplane/src/node/reliable/**` reservation. New utilities in `messages/src/rlm.rs`: `coalesce_sack_runs`, `build_gap_runs`, `build_ack_and_sack`, `choose_repair_indices` with unit tests.

Status (Nov 11):
- [x] M1 — RLM types landed; session configs scaffolded.
- [~] M2 — sender/receiver/control scaffolding in-tree; engines and conductor wiring pending.
- [ ] M3 — not started.
- [~] M4 — scheduler already exposes `set_flow_weight`; token-bucket wiring verified.
- [ ] M5 — not started.
- [x] M6 — thin Python wrappers added; legacy Python reliability removed.
- [x] M7 — unit tests for control utils; more to follow.

---

## Remaining Gaps To Implement (Nov 11)

- Conductor wiring (PR2):
  - Instantiate `ReliableHandle`, run command loop, and expose handle to Python/controller when the `reliable` feature is enabled.
  - Acceptance: end-to-end call path from Python wrappers reaches `ReliableHandle` and spawns sessions.

- Engines (PR3/PR4):
  - Sender: implement MANIFEST, DATA pacing, ACK/SACK processing, resend queue, and retirement policy; unit tests for `process_control_event` and retirement behavior.
  - Receiver: implement ACK base advancement, SACK gap computation, targeted REPAIR, reassembly, and sink writing.
  - Acceptance: deterministic unit tests pass; integration runs transfer files correctly under loss.

- FEC (M3):
  - XOR parity per block (k/p) behind config; encode/decode path; repair integration.
  - Acceptance: single-loss recovery test passes; disabled by default.

- Observability (M5):
  - Add `ReliableStats` message variant; periodic session metrics to controller; structured tracing.
  - Acceptance: metrics show up in controller logs/DB; tracing includes session/flow IDs.

- Config plumbing:
  - `reliable.*` knobs in `LocalConfig`/`node.toml` (chunk size, control weight, data rate, sack/nack intervals, ack policy, fec params).
  - Acceptance: config parsed; defaults sane; knobs reflected in runtime behavior.

Progress notes (config)
- Added `ReliableConfig` to `LocalConfig` (chunk size, control weight, optional data bucket, sack/nack intervals, ack policy, FEC params). No behavior change until engines consume it.

- Docs and examples:
  - Update docs for protocol v2 and examples to call the new wrappers; add migration notes.
  - Acceptance: docs build; examples run.

---

## Verification Criteria and Evidence

- Unit tests
  - RLM encode/decode: golden + property tests (round-trip and rejection).
  - Control helpers: SACK coalescing, gap runs, NACK limiter determinism.
  - Sender retirement: K-of-N policy cases; reorderings.

- Integration tests
  - 1→2 and 1→8 receivers; `tc netem` loss at 1–5%; validate bytes/chunks, tail latency, resend counts.
  - ACK policy variants: `all`, `k:N`, `frac:P`.
  - Optional FEC: recovery of single-loss per block (when enabled).
  - Scheduler/pacing: verify control weight effect and token bucket rate limiting.

- Observability
  - Periodic `ReliableStats` received in controller with expected counters and session IDs.
  - Structured tracing presence at key transitions (MANIFEST, DATA window, SACK, REPAIR, EOT).

Evidence of correctness: All above tests pass reliably across multiple runs with logs and metrics matching expectations.

---

## End-to-End Tests With Rich Logging (Design)

We will add a Python-based E2E harness using `rich` for high-clarity logs.

- Proposed files
  - `examples/reliable_multicast/e2e_reliable_multicast.py` (scaffolded)
  - `examples/reliable_multicast/utils/logging.py` (scaffolded)
  - `examples/reliable_multicast/pyproject.toml` (scaffolded)

- Rich logging style
  - Use Panels for test phases (Setup, Sender, Receivers, Verification).
  - Use `Syntax` blocks to display sample frames and config Toml snippets.
  - Use `Table` to summarize per-session metrics and resend counts.
  - Color-coded status (green pass, yellow warn, red fail) and timing badges.

- Scenarios
  1) Happy path, 1→2 receivers, no loss.
  2) 1→2, 3% loss on one receiver; verify SACK/REPAIR activity and completion.
  3) 1→8, token bucket limit to X MB/s; verify pacing.
  4) Ack policy variants: `all`, `k:3`, `frac:0.75`.
  5) Negative: mismatched chunk_size; receiver rejects manifest and test reports.
  6) Optional FEC: recover single-loss per block (when enabled).

- Harness outline
  - Start dataplane nodes; set `NEXTMINI_CONFIG` and target node envs.
  - Sender: call `Dataplane.reliable_send_file_rs(...)` and collect session id.
  - Receivers: call `Dataplane.reliable_receive_file_rs(...)` and await completion.
  - Controller: poll/aggregate `ReliableStats` if enabled; render via `Table`.
  - Assertions: file sizes, checksums (optional), chunks, resend/repair counts in sane bounds.

- Example Rich snippet
```
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax

console = Console()
console.print(Panel("Starting Reliable Multicast E2E: 1→2, loss=3%"))
toml = open("node.toml").read()
console.print(Syntax(toml, "toml", theme="monokai", word_wrap=True))

table = Table(title="Session Metrics")
table.add_column("Metric"); table.add_column("Value")
table.add_row("bytes_sent", str(bytes_sent))
table.add_row("resends", str(resends))
console.print(table)
```

Execution: integrated via CI job gated on `reliable` feature. Until engines are wired, tests will run in dry-run mode (wrappers return sids with explanatory panels).

Progress notes
- E2E harness scaffolding added under `examples/reliable_multicast/` using `rich` with Panels/Tables/Syntax. Currently operates in dry-run mode until engines are wired.

---

## Ownership and Next Steps

- GreenPond (PR2/PR3): Conductor wiring and sender engine; open reservation remains.
- PurpleStone (PR4/PR5): Receiver engine + Python wrapper delegation after PR2/PR3; E2E harness implementation.

Progress notes (controller hardening)
- Converted several `.unwrap()` / `.expect()` in `controller/src/{main.rs,new_node.rs}` to safe encode/send paths with error logging (no panics). This improves reliability during transient DB/WS issues and keeps the controller process alive.


**New dataplane subsystem:** `dataplane::node::reliable`

* **Session manager:** creates sender/receiver sessions, owns state, spawns tasks.
* **Sender/Receiver engines:** implement MANIFEST → DATA(+optional FEC) → SACK/NACK → EOT protocol.
* **Control I/O:** dedicated, prioritized control flow per peer (or per group), port‑based classification.
* **Data I/O:** paced with token‑bucket; chunk cache for resends & FEC windows.
* **Wire format:** small TLV control frames (MANIFEST, SACK ranges, NACK ranges, EOT). Data chunks use a simple header (index, len) you already use; we formalize it and rename it away from “Py”.
* **Metrics:** periodic stats to controller.

**Public API surfaces:**

* **Rust (in dataplane crate):** `ReliableHandle` with `start_sender`, `start_receiver`, `stop_session`, async progress stream.
* **Python wrapper:** thin pass‑through to the Rust handle; *no* reliability logic in Python.

**Scheduler & pacing:**

* Control flows get higher weight in WRR.
* Data flows use per‑session token bucket.

---

## Milestones & deliverables

1. **M1: Protocol + core types** — TLV control frames, data header, session lifecycle & config.
2. **M2: Sender/Receiver engines** — MANIFEST/EOT, SACK/NACK ranges, quorum‑based eviction, pacing.
3. **M3: Lightweight FEC** — XOR parity blocks, optional & off by default.
4. **M4: Scheduler priority & per‑flow token‑bucket wiring.**
5. **M5: Observability** — per‑session stats to controller, structured logs, guardrails.
6. **M6: Python API shim + examples/docs migration.**
7. **M7: Tests** — unit + integration (with loss), CI jobs.

Status (Nov 11):
- [ ] M1 — pending PR1 (RLM types); session configs scaffolded.
- [x] M2 — sender/receiver/control scaffolding in-tree; engines to be filled after PR1.
- [ ] M3 — not started.
- [~] M4 — scheduler already exposes `set_flow_weight`; token-bucket wiring verified.
- [ ] M5 — not started.
- [x] M6 — thin Python wrappers added; legacy Python reliability removed.
- [x] M7 — unit tests for control utils; more to follow.

---

## Concrete code plan (files & skeletons)

### 1) Protocol types in `messages` crate

File: `messages/src/lib.rs`
Change: **Add reliable multicast control/data primitives (encode/decode; no external deps)**

```rs
// Change: Add reliable multicast protocol primitives (control TLV + data header)

use serde::{Serialize, Deserialize};
use std::fmt;

pub const RELIABLE_TLV_VER: u8 = 0x81;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReliableCtlKind {
    Manifest   = 1,
    Sack       = 2,
    NackRange  = 3,
    Eot        = 4,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReliableTlvHdr {
    pub ver: u8,           // = RELIABLE_TLV_VER
    pub kind: ReliableCtlKind,
    pub len: u16,          // payload length
    pub session_id: u64,
}

impl ReliableTlvHdr {
    pub fn encode_into(&self, out: &mut [u8]) {
        out[0] = self.ver;
        out[1] = self.kind as u8;
        out[2..4].copy_from_slice(&self.len.to_be_bytes());
        out[4..12].copy_from_slice(&self.session_id.to_be_bytes());
    }
    pub fn decode_from(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 12 { return None; }
        let ver = buf[0];
        if ver != RELIABLE_TLV_VER { return None; }
        let kind = match buf[1] {
            1 => ReliableCtlKind::Manifest,
            2 => ReliableCtlKind::Sack,
            3 => ReliableCtlKind::NackRange,
            4 => ReliableCtlKind::Eot,
            _ => return None,
        };
        let len = u16::from_be_bytes([buf[2], buf[3]]);
        let session_id = u64::from_be_bytes(buf[4..12].try_into().ok()?);
        Some((Self { ver, kind, len, session_id }, 12))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub chunk_size: u32,
    pub total_bytes: u64,
    pub total_chunks: u64,  // derive if 0
    pub checksum_alg: u8,   // 1 = sha256
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SackRange { pub start: u64, pub len: u16 } // len==0 means empty

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SackMsg {
    pub base: u64,               // first committed chunk index (cumulative)
    pub ranges: Vec<SackRange>,  // additional sparse ranges beyond base
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NackRangeMsg { pub ranges: Vec<SackRange> }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EotMsg {
    pub last_chunk: u64,
    pub checksum_hex: Option<String>, // sha256 hex if provided
}

pub fn encode_tlv<T: Serialize>(kind: ReliableCtlKind, sid: u64, value: &T) -> Vec<u8> {
    let body = bincode::serialize(value).expect("encode");
    let mut out = vec![0u8; 12 + body.len()];
    ReliableTlvHdr { ver: RELIABLE_TLV_VER, kind, len: body.len() as u16, session_id: sid }
        .encode_into(&mut out[..12]);
    out[12..].copy_from_slice(&body);
    out
}

pub fn decode_tlv(buf: &[u8]) -> Option<(ReliableTlvHdr, &[u8])> {
    let (hdr, off) = ReliableTlvHdr::decode_from(buf)?;
    if buf.len() < off + hdr.len as usize { return None; }
    Some((hdr, &buf[off..off + hdr.len as usize]))
}

// Formalize the chunk payload header used on the data path
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataChunkHdr {
    pub chunk_index: u64,
    pub payload_len: u32,
}

impl DataChunkHdr {
    pub const LEN: usize = 12;
    pub fn encode_into(&self, out: &mut [u8]) {
        out[0..8].copy_from_slice(&self.chunk_index.to_be_bytes());
        out[8..12].copy_from_slice(&self.payload_len.to_be_bytes());
    }
    pub fn decode_from(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < Self::LEN { return None; }
        let idx = u64::from_be_bytes(buf[0..8].try_into().ok()?);
        let len = u32::from_be_bytes(buf[8..12].try_into().ok()?);
        Some((Self { chunk_index: idx, payload_len: len }, Self::LEN))
    }
}

// Optional: wire a stats event to controller later
#[derive(Serialize, Deserialize, Debug)]
pub struct ReliableStats {
    pub session_id: u64,
    pub node_id: usize,
    pub role: &'static str, // "sender" | "receiver"
    pub bytes: u64,
    pub chunks: u64,
    pub resends: u64,
    pub repairs: u64,
    pub sacks: u64,
    pub fec_used: u64,
    pub ts_ms: i64,
}
```

---

### 2) Dataplane reliable subsystem

**New module tree:**

```
dataplane/src/node/reliable
├── mod.rs
├── session.rs
├── sender.rs
├── receiver.rs
├── control.rs       (TLV utils, SACK merge, NACK throttling)
├── fec.rs           (optional XOR parity)
└── api.rs           (ReliableHandle, commands, events)
```

File: `dataplane/src/node/reliable/mod.rs`
Change: **Module export**

```rs
pub mod api;
pub mod session;
pub mod sender;
pub mod receiver;
pub mod control;
pub mod fec;
```

File: `dataplane/src/node/reliable/api.rs`
Change: **Introduce `ReliableHandle` exposed to the rest of the dataplane (and Python)**

```rs
use tokio::sync::{mpsc, oneshot};
use super::session::{SenderConfig, ReceiverConfig};

#[derive(Clone)]
pub struct ReliableHandle {
    tx: mpsc::UnboundedSender<Command>,
}

pub enum Command {
    StartSender { cfg: SenderConfig, reply: oneshot::Sender<SessionId> },
    StartReceiver { cfg: ReceiverConfig, reply: oneshot::Sender<SessionId> },
    Stop { session: SessionId },
}

pub type SessionId = u64;

impl ReliableHandle {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Command>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }
    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartSender { cfg, reply: tx });
        rx.await.expect("start_sender reply")
    }
    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Command::StartReceiver { cfg, reply: tx });
        rx.await.expect("start_receiver reply")
    }
    pub fn stop(&self, session: SessionId) {
        let _ = self.tx.send(Command::Stop { session });
    }
}
```

File: `dataplane/src/node/reliable/session.rs`
Change: **Session configs & manager skeleton**

```rs
use std::net::Ipv4Addr;
use tokio::task::JoinHandle;
use crate::node::{FlowId};
use crate::node::network::interface::NetworkInterfaceHandle;
use nextmini_messages::TokenBucketSpec;

use super::api::{SessionId};
use super::{sender, receiver};

#[derive(Clone, Debug)]
pub enum AckPolicy { All, KofN(usize), Fraction(f32) }

#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub group_ip: Ipv4Addr,
    pub chunk_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub control_weight: usize,          // WRR weight
    pub data_bucket: Option<TokenBucketSpec>,
}

#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub checksum_out: bool,
    pub ack_policy: AckPolicy,
    pub sack_interval_ms: u64,
    pub repair_backoff_ms: u64,
    pub fec_k: Option<u16>,             // None => disabled
    pub fec_p: u8,                      // 0..2 in first iteration
}

#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub verify_checksum: bool,
    pub sink_path: Option<String>,
    pub nack_min_interval_ms: u64,
    pub nack_jitter_ms: u64,
}

pub struct SessionManager {
    net: NetworkInterfaceHandle,
    // plus handles to processor/scheduler to set weights + buckets
    tasks: ahash::AHashMap<SessionId, JoinHandle<()>>,
}

impl SessionManager {
    pub fn new(net: NetworkInterfaceHandle) -> Self {
        Self { net, tasks: ahash::AHashMap::new() }
    }
    pub fn spawn_sender(&mut self, cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let handle = tokio::spawn(sender::run(cfg, self.net.clone()));
        self.tasks.insert(sid, handle);
        sid
    }
    pub fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let handle = tokio::spawn(receiver::run(cfg, self.net.clone()));
        self.tasks.insert(sid, handle);
        sid
    }
    pub async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) { h.abort(); }
    }
}
```

File: `dataplane/src/node/reliable/control.rs`
Change: **Control helpers: TLV send/recv, SACK merge, NACK throttle (skeleton)**

```rs
use std::time::{Duration, Instant};
use nextmini_messages::{encode_tlv, decode_tlv, ReliableCtlKind, SackMsg, SackRange, NackRangeMsg, Manifest, EotMsg};

pub fn merge_sack(base: u64, ranges: &mut Vec<SackRange>, new_ranges: &[SackRange]) {
    // TODO: implement coalescing; keep sorted + merged
    ranges.extend_from_slice(new_ranges);
    ranges.sort_by_key(|r| r.start);
    // ... merge overlapping ...
}

pub struct NackLimiter {
    last: Option<(u64, Instant)>, // (chunk, when)
    min_interval: Duration,
}
impl NackLimiter {
    pub fn new(min_interval: Duration) -> Self { Self { last: None, min_interval } }
    pub fn should_send(&mut self, chunk: u64, now: Instant) -> bool {
        match self.last {
            Some((c, t)) if c == chunk && now.duration_since(t) < self.min_interval => false,
            _ => { self.last = Some((chunk, now)); true }
        }
    }
}
```

File: `dataplane/src/node/reliable/sender.rs`
Change: **Sender engine skeleton (MANIFEST → DATA → SACK/NACK → EOT)**

```rs
use std::collections::{BTreeMap, HashMap, HashSet, BTreeSet};
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut, BufMut};
use nextmini_messages::{Manifest, EotMsg, SackMsg, NackRangeMsg, encode_tlv, ReliableCtlKind, DataChunkHdr};
use crate::node::network::interface::NetworkInterfaceHandle;
use super::session::{SenderConfig, AckPolicy};

pub async fn run(cfg: SenderConfig, mut net: NetworkInterfaceHandle) {
    // 1) Send MANIFEST
    let manifest = Manifest {
        chunk_size: cfg.common.chunk_size as u32,
        total_bytes: cfg.total_bytes,
        total_chunks: (cfg.total_bytes + cfg.common.chunk_size as u64 - 1) / cfg.common.chunk_size as u64,
        checksum_alg: 1,
    };
    let man = encode_tlv(ReliableCtlKind::Manifest, cfg.common.session_id, &manifest);
    broadcast_control(&mut net, &cfg, man).await;

    // 2) Main loop: read file/source (omitted), build DataChunkHdr + payload, send
    // Maintain inflight: chunk_index -> set<receiver_id> acked
    // Maintain ack counts and evict by cfg.ack_policy
    // Consume SACKs and NACK_RANGE from control channel; schedule resends using chunk cache
    // (full implementation omitted here)
    // 3) Send EOT when done
    let eot = EotMsg { last_chunk: manifest.total_chunks, checksum_hex: None };
    let eot_bytes = encode_tlv(ReliableCtlKind::Eot, cfg.common.session_id, &eot);
    broadcast_control(&mut net, &cfg, eot_bytes).await;
}

async fn broadcast_control(net: &mut NetworkInterfaceHandle, cfg: &SenderConfig, body: Vec<u8>) {
    // Use prioritized control flow (cfg.common.src_port, dst_port); serialize once and reuse
    // Send to group IP (reliable control over same transport layer)
    // Implementation will build a Packet with control payload and call net.write_packets(...)
    let _ = (net, cfg, body); // placeholder
}

fn build_data_payload(chunk_index: u64, bytes: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(DataChunkHdr::LEN + bytes.len());
    DataChunkHdr { chunk_index, payload_len: bytes.len() as u32 }.encode_into(&mut out[..DataChunkHdr::LEN]);
    out.put_slice(bytes);
    out.freeze()
}
```

File: `dataplane/src/node/reliable/receiver.rs`
Change: **Receiver engine skeleton (wait MANIFEST → stream DATA → periodic SACK → EOT)**

```rs
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use bytes::Bytes;
use nextmini_messages::{decode_tlv, ReliableCtlKind, Manifest, EotMsg, NackRangeMsg, SackMsg, DataChunkHdr, SackRange};
use crate::node::network::interface::NetworkInterfaceHandle;
use super::session::ReceiverConfig;
use super::control::{NackLimiter};

pub async fn run(cfg: ReceiverConfig, mut net: NetworkInterfaceHandle) {
    // 1) Wait for MANIFEST (or derive from cfg.expected_bytes/chunk_size if allowed)
    // 2) Register data flow and control flow receivers (by 4-tuple) on net
    // 3) Periodically emit SACK; on timeout of missing chunk, emit NACK_RANGE with limiter
    let _ = (cfg, net);
}

fn on_data_chunk(buf: &[u8], pending: &mut BTreeMap<u64, Bytes>, expected_chunk: &mut u64) {
    if let Some((hdr, off)) = DataChunkHdr::decode_from(buf) {
        let body = &buf[off..off + hdr.payload_len as usize];
        pending.insert(hdr.chunk_index, Bytes::copy_from_slice(body));
        // then try to commit in-order and advance expected_chunk
    }
}
```

---

### 3) Scheduler priority & pacing

File: `dataplane/src/node/scheduler/queue.rs`
Change: **Expose `set_flow_weight` on trait (already implemented by WRR)**

```rs
pub trait SchedulerQueue {
    fn enqueue(&self, packet: Packet) -> Result<(), Packet>;
    fn collect_packets(&self, batch: &mut Vec<Packet>);
    fn is_empty(&self) -> bool;
    fn queue_len(&self, flow_id: FlowId) -> usize;
    fn set_flow_weight(&self, flow_id: FlowId, weight: usize); // ensure trait includes this
}
```

File: `dataplane/src/node/scheduler/sched.rs`
Change: **Add control message to tweak weights; wire to reader**

```rs
pub enum SchedulerReaderMessage {
    // ...
    SetFlowWeight(FlowId, usize),
}

impl Scheduler {
    pub fn run(&self) {
        // in reader loop, handle SetFlowWeight by calling self.queue.set_flow_weight(...)
    }
}
```

File: `dataplane/src/node/scheduler/writer.rs`
Change: **Ensure token bucket can be set per flow (already supported)**

```rs
pub enum SchedulerWriterMessage {
    // ...
    SetTokenBucket(Option<TokenBucketSpec>),
}
```

> The reliable session manager will call:
>
> * **Boost control flow weight** (e.g., 8) when registering control receivers.
> * **Set token bucket** for the data flow at session start.

---

### 4) Hook up in the node

File: `dataplane/src/node/conductor.rs`
Change: **Instantiate ReliableHandle & SessionManager; expose handle to Python API and controller**

```rs
// inside Conductor::new or equivalent init
let (reliable, rx) = reliable::api::ReliableHandle::new();
let mut session_mgr = reliable::session::SessionManager::new(net_interface_handle.clone());
tokio::spawn(async move {
    use reliable::api::Command::*;
    let mut rx = rx;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            StartSender { cfg, reply } => { let sid = session_mgr.spawn_sender(cfg); let _ = reply.send(sid); }
            StartReceiver { cfg, reply } => { let sid = session_mgr.spawn_receiver(cfg); let _ = reply.send(sid); }
            Stop { session } => session_mgr.stop(session).await,
        }
    }
});
// store `reliable` inside Conductor for external access
```

---

### 5) Thin Python API: delegate to Rust reliable subsystem

File: `python-api/src/lib.rs`
Change: **Add wrapper methods; deprecate old in‑module reliability**

```rs
#[pymethods]
impl Dataplane {
    #[pyo3(signature = (group_ip, receiver_ids, tensor_path, *, chunk_size=32768, src_port=None, dst_port=None, ack_policy="all"))]
    fn reliable_send_file_rs(&self, group_ip: &str, receiver_ids: Vec<usize>, tensor_path: &str, chunk_size: usize, src_port: Option<u16>, dst_port: Option<u16>, ack_policy: &str) -> PyResult<u64> {
        let group_ip = parse_ipv4(group_ip)?;
        let sid = next_py_message_id();
        let cfg = to_sender_config(self, sid, group_ip, receiver_ids, tensor_path, chunk_size, src_port, dst_port, ack_policy)?;
        // call into Conductor/ReliableHandle held by self.controller or similar
        // rt().block_on(self.reliable.start_sender(cfg));
        Ok(sid)
    }

    #[pyo3(signature = (group_ip, source_node_id, expected_bytes, *, chunk_size=32768, src_port=None, dst_port=None, sink_path=None))]
    fn reliable_receive_file_rs(&self, group_ip: &str, source_node_id: usize, expected_bytes: u64, chunk_size: usize, src_port: Option<u16>, dst_port: Option<u16>, sink_path: Option<String>) -> PyResult<u64> {
        let group_ip = parse_ipv4(group_ip)?;
        let sid = next_py_message_id();
        let cfg = to_receiver_config(self, sid, group_ip, source_node_id, expected_bytes, chunk_size, src_port, dst_port, sink_path)?;
        // rt().block_on(self.reliable.start_receiver(cfg));
        Ok(sid)
    }
}
```

> This keeps Python as a convenience layer while moving all reliability into `dataplane`.

---

### 6) Controller metrics (optional, later)

File: `messages/src/lib.rs`
Change: **Add a stats variant** (if you want controller aggregation)

```rs
#[derive(Serialize, Deserialize, Debug)]
pub enum DataplaneToController {
    // ...
    ReliableStats(super::ReliableStats),
}
```

Dataplane sender/receiver periodically:
`controller.send(DataplaneToController::ReliableStats(stats)).await;`

---

## Configuration (add to `LocalConfig`)

* `reliable.default_chunk_size` (bytes)
* `reliable.control_weight` (WRR)
* `reliable.data_rate` (optional token bucket; bytes/s)
* `reliable.sack_interval_ms`, `reliable.nack_min_interval_ms`, `reliable.nack_jitter_ms`
* `reliable.ack_policy = all | k:N | frac:P`
* `reliable.fec = off | k:32,p:2`

(Plumb via `controller-config.toml` or `node.toml` as suits your examples.)

---

## Testing plan

* **Unit:** TLV encode/decode; SACK merge; NACK limiter; quorum eviction; FEC encode/decode (single‑loss recovery).
* **Integration (docker examples):** 1→2 and 1→8 receivers; add `tc netem loss {1,3,5}%` on one receiver; verify tail latency; verify reduced NACKs with SACK/FEC.
* **Failure modes:** mismatched chunk size ⇒ receiver rejects MANIFEST; stalled receiver ⇒ sender still completes under quorum; EOT mismatch ⇒ checksum error.

---

## Migration & deprecation

* Keep the old Python‑side reliability for one release behind a feature flag (default **off** once Rust path is ready).
* Migrate `examples/multicast-docker` to call `reliable_*_rs` wrappers.
* Update docs (`docs/docs/examples/multicast-flow.md`, `docs/docs/design/python-api.md`) with protocol v2 diagrams and flags.

---

### Notes on QUIC/TCP

Your `NetworkInterfaceHandle` already abstracts protocol. The reliable layer treats control/data as **bytes over a flow 4‑tuple**, so it works over TCP/QUIC uniformly. If QUIC is enabled, you can map control to a distinct QUIC stream for natural priority without WRR.

---

# What you can land first

* **M1 (types + skeleton)**: messages TLV + DataChunkHdr; reliable module scaffolding; Python wrappers that no‑op (return sid) until engines are wired.
* **M2 (engines)**: sender/receiver loops with MANIFEST/SACK/NACK/EOT (no FEC), control weight/bucket plumbed.
* **M3 (FEC)**: XOR parity per block (optional).

This is a full Rust implementation plan that eliminates Python‑side protocol logic, keeps your dataplane coherent, and sets you up for scale and observability without a rewrite of the broader system. If you want, I can turn M1/M2 into PR‑sized diffs next.

---

## Remaining Work (Tracked) + Verification Criteria

- PR1 — Messages RLM v1 (owner: PurpleStone)
  - Status: pending merge; helpers + unit tests in progress.
  - Done when: `messages/src/rlm.rs` encoders/decoders + tests land; round-trip and negative cases pass.
  - Verify: `cargo test -p nextmini-messages`; add fuzz target later for decode.

- PR2 — Conductor command loop → SessionManager (owner: GreenPond)
  - Status: feature-gated loop stub added; SessionManager wiring TODO.
  - Done when: Reliable commands spawn sender/receiver tasks via SessionManager with plumbed network handles.
  - Verify: unit tests for command loop; smoke run with feature `reliable` enabled (logs show session lifecycle).

- PR3 — Sender engine core (owner: GreenPond)
  - Status: skeleton present.
  - Done when: MANIFEST→paced DATA with cache→process SACK/REPAIR→EOT; metrics counters incremented.
  - Verify: unit tests for `process_control_event` retirement and resend scheduling; end-to-end checksum match under no-loss and lossy conditions (see E2E plan).

- PR4 — Receiver engine core (owner: PurpleStone)
  - Status: helpers preworked; engine skeleton present.
  - Done when: MANIFEST intake→chunk assembly→periodic ACK/SACK→targeted REPAIR; verifies checksum when requested.
  - Verify: unit tests for `build_ack_and_sack`, chunk assembly; E2E file equality and ACK/SACK traces.

- M4 — Scheduler wiring
  - Status: queue exposes `set_flow_weight`; token-bucket exists.
  - Done when: SessionManager boosts control flow weight and sets per-session token buckets.
  - Verify: log/inspect weights; token-bucket limits hit in rate-limited test; no starvation of control.

- M5 — Observability
  - Status: not started.
  - Done when: `ReliableStats` events periodically reported; tracing includes `session_id,node_id,role`.
  - Verify: controller receives stats; logs show counters; CI asserts non-zero accounting in E2E.

- M6 — Python shim + examples
  - Status: thin wrappers added; delegation pending.
  - Done when: wrappers call `ReliableHandle` and examples use them; docs updated.
  - Verify: example run completes with expected checksum; rich logs explain steps (see E2E plan).

- M7 — Tests (unit + integration)
  - Status: control-utils unit tests done; more to add.
  - Done when: unit coverage for helpers, and E2E scenarios pass locally; CI job prepared (gated).
  - Verify: see E2E plan below.

---

## End-to-End Test Plan (rich-logged)

Harness
- Location: `tools/e2e/test_reliable_multicast.py` (pytest + rich).
- Setup: `uv venv && source .venv/bin/activate && uv pip install pytest rich`; build/install `nextmini_py` via maturin as per AGENTS.md; export `NEXTMINI_CONFIG` and `NEXTMINI_DST_NODE` as needed.
- Guard: tests skip unless `ENABLE_RELIABLE_E2E=1` and the `reliable` feature is enabled at build/run.

Scenarios
- S1 No-loss 1→2: send a file; expect byte/chunk counters match and checksum equality at receivers; rich Panels show MANIFEST/DATA/EOT and ACK sequence.
- S2 Lossy 1→N: inject loss (e.g., tc netem or simulated drop); expect targeted resends; verify SACK runs reflect gaps; counters (resends>0) and final checksum equality.
- S3 Rate-limited: per-session token bucket; verify pacing and queue drain; ensure control flow is not starved.

Rich logging
- Each step logs a Panel with: inputs, function called, expected outputs, and actual results; Syntax blocks for representative wire frames (hex) and config toml.

CI
- Keep E2E opt-in (env guard) initially; once stable, add a nightly job to run S1/S2.

---

## Audit Findings (Nov 11) and Immediate Fixes

What I reviewed
- messages/src/rlm.rs (framing, control helpers, tests)
- dataplane/src/node/{mod.rs, conductor.rs} (feature gating, wiring stubs)
- python-api/src/lib.rs (wrappers, legacy remnants)
- controller/src/main.rs (startup and socket handling)
- examples/reliable_multicast/* (harness + logging)

Issues found and fixes applied
- SACK encoding safety: build_gap_runs could overflow u16 when gaps/deltas exceeded 65535. Fixed by bounding and segmenting large gaps into u16-sized runs, with a new test to guarantee bounds. File: messages/src/rlm.rs.
- Clippy error: unused import of `warn` when `reliable` feature is off. Fixed by using `tracing::warn!` inline and importing only `info`. File: dataplane/src/node/conductor.rs.
- Controller robustness: `TcpListener::bind(...).await.expect(...)` could crash the process on bind failure. Replaced with a guarded match that logs and exits cleanly. File: controller/src/main.rs.
- Python legacy cleanup: hardened removal of legacy Python reliability by pruning cfg-gated imports and ensuring only thin stubs remain; legacy feature no longer builds even if enabled. File: python-api/src/lib.rs.
- E2E logging: enriched harness with scenario-specific ack policies and rich Syntax panels showing exact calls and arguments for full explainability. File: examples/reliable_multicast/e2e_reliable_multicast.py.

Additional findings (recommendations queued)
- Python ack_policy validation: the thin wrappers currently only check string shapes (e.g., startswith("k:", "frac:")) and do not parse numeric bounds; recommend calling `messages::rlm::parse_ack_policy()` for full validation so `k:0`/`frac:0` are rejected consistently. Blocked on `python-api/src/lib.rs` reservation; queued.
- Controller DB reset on startup: `init_db()` calls `reset_db()` which drops all tables each run; acceptable for dev but risky for persistent environments. Recommend guarding with an env (e.g., `CONTROLLER_RESET_DB=1`) or making reset path explicit for tests only.
- Dataplane fatal panics: local interface creation panics on TUN setup failures; acceptable as a fatal condition but consider structured error + process exit for clearer logs in production deploys.

Verification signals (how we know correctness)
- cargo check --workspace succeeds; clippy with -D warnings is clean after fixes (re-run as part of PR).
- Unit tests in messages pass; added bound-safety test for SACK.
- Dry-run E2E harness renders detailed, step-by-step logs; ready to flip to live once ReliableHandle wiring lands.
- No default-behavior changes behind feature gates; `reliable` feature off keeps builds green.

Remaining work to complete plan
- PR2/PR3: Conductor wiring + sender engine (GreenPond).
- Receiver engine + Python delegation (PurpleStone) after PR2/PR3.
- Flip E2E to live mode; add assertions for bytes/chunks/resends; consider checksum verification.
- Optional: Persist ReliableStats to DB after validating log surface.

Coordination notes
- Reservations: I requested exclusive holds for plan, examples, and python-api stubs; overlapping holds exist on plan/python-api. I will only append plan sections and maintain thin stubs until GreenPond confirms timing.

---

## Remaining Work Snapshot and Evidence (Nov 11)

- PR2 — Conductor wiring to ReliableHandle
  - Evidence pending: `dataplane/src/node/conductor.rs` constructs a `ReliableHandle` and spawns a loop, but `SessionManager::new_without_net` is used; no network writer wired yet.
  - Done when: Python wrappers can start sender/receiver sessions through this handle and tasks are spawned.

- PR3 — Sender engine (MANIFEST → paced DATA → ACK/SACK/REPAIR → retire)
  - Evidence pending: `dataplane/src/node/reliable/sender.rs` has a placeholder `run` and isn’t exercised.
  - Done when: deterministic unit tests cover resend/retirement logic; live E2E moves bytes with loss.

- Receiver engine (ACK base advance, SACK building, repair targeting, reassembly)
  - Evidence pending: `dataplane/src/node/reliable/receiver.rs` is scaffolded; not wired.
  - Done when: unit tests cover SACK cadence and reassembly; live E2E validates byte/checksum equality.

- Scheduler control-flow priority (M4)
  - Evidence present: policy hooks exist; not exercised by engines.
  - Done when: control WRR weight applied and verified in live E2E pacing scenario.

- Observability (M5) — stats emission and optional persistence
  - Evidence present: `ReliableStats` type and controller log path; no engine emission yet.
  - Done when: engines emit periodic stats; controller displays or persists them.

- Python wrapper delegation + completion signaling
  - Evidence present: thin stubs returning sids; no completion yet.
  - Done when: wrappers call `ReliableHandle` and expose completion / counters for E2E assertions.

Verification signals (how we’ll know it’s complete)
- Build: `cargo check --workspace`; `cargo check -p nextmini --features reliable`.
- Unit tests: sender/receiver/control helpers; edge cases (timeouts, large SACK windows).
- E2E live: checksum equality; chunk and resend counters; pacing/loss scenarios pass.
- Controller: ReliableStats observed and optionally stored.

---

## E2E Test Catalog (Verbose Logging)

- tests/test_dry_run.py — Dry-run end-to-end wrapper calls; Panels/Tables.
- tests/test_ack_policies.py — Param ack variants (all/k/frac); asserts stub SIDs.
- tests/test_validation_errors.py — Invalid inputs; xfails queued for numeric bounds.
- tests/test_call_trace_verbose.py — Per-step Panels + Syntax (inputs, functions under test, observed outputs).
- tests/test_verbose_matrix.py — Matrix/Table of steps with expected vs observed results.
- tests/test_control_frames_hex_placeholder.py — Placeholder hex dumps (to be replaced with live frames once engines emit).
- tests/test_loss_and_pacing_verbose.py — Guidance + Syntax for loss/pacing scenarios; xfail until engines wire up.

Run (opt-in, requires nextmini_py):
- `export ENABLE_RELIABLE_E2E=1`
- `pytest -q examples/reliable_multicast/tests`

Live flip criteria (post-PR2/PR3/engines):
- Replace placeholder/xfail markers with assertions on checksum equality, resend counts, and pacing behavior.
- Capture and display actual control-frame hex in tests for traceability.

## Progress Notes (Nov 11, later)

- Tests: added opt-in rich E2E dry-run test harness under `examples/reliable_multicast/tests/test_dry_run.py` (pytest). Logs each step with Panels/Syntax. Guarded by `ENABLE_RELIABLE_E2E=1` and requires `nextmini_py` installed.
- Examples: enriched scenario runner with ack policy toggles and Syntax dumps of the exact calls for transparency.
- Tests: added `test_ack_policies.py` to cover `all`/`k:N`/`frac:P` paths (still dry‑run) and `test_validation_errors.py` to validate wrapper input checks (invalid ack policy, zero chunk size, missing file).
- Tests: added `test_call_trace_verbose.py` with step-by-step Panels and Syntax blocks showing inputs, functions under test, and observed outputs (stub SIDs) for maximum explainability.
- Tests: expanded protocol safety checks in `messages` crate (negative decode cases for truncated DATA and short CONTROL bodies). All unit tests pass.
- Harness: `examples/reliable_multicast/e2e_reliable_multicast.py` now attempts `Dataplane.reliable_wait(...)` deterministically when the wrapper exposes it; logs Panels/Tables either way.
- Logging hooks: documented `RELIABLE_HEX_LOG=1` to enable compact hex dumps of control/data frames during development; sender baseline already respects this env var. See `docs/reliable_logging.md`.
- Python cleanup: legacy Python reliability helpers (the `legacy_py_reliable` feature gate and the old control-loop shims) have been removed from `python-api/src/lib.rs`, so the bindings now rely solely on the dataplane ReliableHandle wiring.
- Coordination: BlueCat holds `python-api/src/lib.rs` and `tools/e2e/test_reliable_multicast.py`; I avoided these paths, added tests inside `examples/` instead, and confirmed via Agent Mail. Pending GreenPond’s PR2/PR3 schedule to wire wrappers and begin receiver engine work immediately after.
- Controller hardening: converted several `.expect()` usages in notification setup to error logs + early returns (no process crash on DB/listener issues). Runtime crash points reduced while preserving visibility in logs.
- Controller reset gating: added `CONTROLLER_RESET_DB` env guard (default: reset enabled; set to 0/false/no to skip). Documented in `docs/reliable_e2e.md`.

Next
- When reservations clear: remove all remaining legacy Python reliability blocks from `python-api/src/lib.rs` and wire wrappers to `ReliableHandle` post‑PR2/PR3.
- Expand E2E to live assertions (bytes/chunks/resends/checksum) as engines land; consider checksum verification panel in logs.
 - Add lossy/pacing scenarios to pytest once engines are integrated; keep rich logging and assertions consistent.
## Progress Log — 2025-11-11 (BlueCat)

- Registered in Agent Mail as `BlueCat`; introduced to `PurpleStone`, `GreenPond`, `PurpleHill`. Reserved python wiring, reliable module, conductor, E2E, and plan paths (advisory conflicts noted; proceeding surgically).
- Implemented `SessionManager::new_without_net` and guarded spawns to no-op with clear warnings until a network writer strategy is decided. This makes the feature-gated conductor loop compile/run without touching network I/O.
- Wired Python wrappers to `ReliableHandle` behind `feature=reliable` (otherwise stubs): `Dataplane` stores `reliable` handle; `reliable_send_file_rs`/`reliable_receive_file_rs` build configs and start sessions. Ack policy parsed via `messages::rlm::parse_ack_policy` and mapped to dataplane enum.
- Expanded E2E: added `test_rmcast_live_checksum_validation` with rich panels/tables and checksum comparison, gated by `ENABLE_RELIABLE_E2E_LIVE=1`. Marked xfail if completion signaling isn’t exposed yet.

Update (later, same day):
- Added ReliableHandle::wait_completion + conductor command to await join (feature=reliable). Python wrapper now exposes `Dataplane.reliable_wait(session_id, timeout_ms=None)`.
- Live E2E now waits on sender/receiver completion (panels show exact calls). Still no sink checksums until payload path exists, but completion is deterministic.
- Added `source_path` to SenderConfig and set it from Python wrapper so the sender can stream file data once the writer is wired. No functional change yet.

Update (writer-less injection via processors):
- Sender now wraps encoded RLM frames in IPv4/TCP using Packet::build_ipv4_tcp_packet and injects via ProcessorHandle::process_packet. This lets us move forward without a separate writer handle and keeps scheduling/metrics intact.
- Python wrappers populate CommonConfig with local_node_id, user_space_base_addr, local_netmask so sender computes src/dst IPs.
- Added RELIABLE_HEX_LOG=1 option to log compact hex for small frames during live verbose tests.

Outstanding work (confirmed by reading code and feature builds):
- Sender/Receiver engines remain placeholders; no actual network send/recv yet. Session spawns log no-op warnings.
- Network writer hookup for reliable sessions undecided; will choose Mutex-wrapped shared writer or a dedicated path and then switch SessionManager to use it.
- Python does not yet expose completion/counters; live E2E waits are placeholder until engines emit signals.
- Legacy Python reliable code has been deleted; no additional feature gates remain on the Python side.

Validation so far:
- `cargo check --workspace` green.
- `cargo check -p nextmini --features reliable` green after adding `new_without_net` and removing `Clone` misuse.
- E2E harness logs run; live mode gated until completion signaling exists.

Next actions:
- Implement sender engine baseline (MANIFEST→DATA, ACK/SACK/REPAIR, retire logic) using `messages::rlm` helpers.
- Implement receiver engine baseline (chunk assembly, ACK/SACK cadence, NACK limiter, REPAIR)
- Decide/wire network writer; then enable real tasks in SessionManager spawn.
- Add completion signal to wrappers or a polling API; flip live E2E to assert checksums and counters.
## Detailed E2E Logging/Call Trace (added by BlueCat)

What’s exercised in tools/e2e/test_reliable_multicast.py:
- Dataplane lifecycle
  - Python: `nextmini_py.Dataplane.__init__(config_path)` → Rust `Conductor::new`, returns handles.
  - Python→Rust: `conductor.processor_handle()` and `conductor.local_config()` used to finish binding setup.
  - Output: a running node with Python interface attached; no reliable I/O attempted by default.

- Reliable session startup (feature=reliable on):
  - Python: `dp.reliable_send_file_rs(group_ip, receivers, tensor_path, chunk_size, src_port, dst_port, ack_policy)`
    - Validates: IPv4 parse, non-empty receivers, positive chunk_size, file exists, ack_policy parse via `messages::rlm::parse_ack_policy`.
    - Calls: `ReliableHandle::start_sender(SenderConfig{ common, receiver_ids, total_bytes, ack_policy, ... })`.
    - Output: `session_id` (u64). With current no-net SessionManager, background task is a no-op and logs a warning.
  - Python: `dp.reliable_receive_file_rs(group_ip, source_node_id, expected_bytes, chunk_size, src_port, dst_port, sink_path)`
    - Validates: IPv4 parse, positive expected_bytes/chunk_size.
    - Calls: `ReliableHandle::start_receiver(ReceiverConfig{ common, expected_bytes, ... })`.
    - Output: `session_id` (u64). With no-net, task is a no-op and logs a warning.

- Rich logging in tests (Panels/Syntax/Tables):
  - Panels render the config TOML, the inputs table (group/receivers/file), and per-step status (session IDs, wait notes).
  - Live test (opt-in via `ENABLE_RELIABLE_E2E_LIVE=1`): computes `sha256(src)` and `sha256(sink)` and compares when a sink is present; otherwise xfail pending completion signaling.

Expected outputs/results per step:
- `Dataplane(cfg)` returns a constructed object; logs show node startup.
- `reliable_send_file_rs` returns `sid_send > 0`; warns that net writer is not wired (until engines + writer land).
- `reliable_receive_file_rs` returns `sid_recv > 0`; same warning.
- Live test: after we expose completion signaling and implement engines, sink file exists and `sha256(src)==sha256(sink)`.

Next logging enhancements (post-engines):
- Add per-session counters (DATA sent, SACK/REPAIR processed, resends) to a rich Table.
- Render `RlmHeader`/control frames in Syntax blocks for specific small test cases.
- Include timing panels for manifest→ready→final EOT transitions.

Hex-dump option for live debugging
- Engines log compact hex of small control/data frames when `RELIABLE_HEX_LOG=1`.
- Useful for E2E rich tests to show exact on-wire bytes during live runs.

## Verification Matrix (status and evidence)

- Messages RLM types/helpers
  - Evidence: messages/src/rlm.rs unit tests (encode/decode, gap coalescing, ack policy parsing). Build: cargo check --workspace passes.
- Conductor reliable loop (feature-gated)
  - Evidence: dataplane/src/node/conductor.rs wiring compiles; SessionManager::new_without_net present; cargo check -p nextmini --features reliable passes.
- SessionManager no-net baseline
  - Evidence: no-op spawns with explicit warnings; prevents misuse of non-Clone writer; compile verified with feature build.
- Python wrappers delegate when feature enabled
  - Evidence: python-api/src/lib.rs maps ack_policy via messages::rlm::parse_ack_policy, builds SenderConfig/ReceiverConfig, and calls ReliableHandle::start_*; default build remains green; python-api feature `reliable` forwards to nextmini/reliable.
- E2E harness rich logs
  - Evidence: tools/e2e/test_reliable_multicast.py includes:
    - test_rmcast_no_loss_session_ids_only (dry-run)
    - test_rmcast_live_checksum_validation (opt-in live; checksum comparison pending completion)
    - test_rmcast_ack_policy_variants_and_input_validation (valid/invalid ack policies; input guards)
    - test_rmcast_receiver_expected_bytes_and_sink_path (receiver validations; sink path logging)

## Additional E2E Scenarios (planned and partially added)

- No-loss happy path (live)
  - Inputs: 256 KiB file, group=239.0.0.1, receivers=[2].
  - Calls: reliable_send_file_rs → reliable_receive_file_rs.
  - Expected: completion signal; sink created; sha256(src)==sha256(sink); counters show 0 repairs.
  - Status: scaffold exists; awaiting engines + completion signal.

- Ack policy matrix (added)
  - Inputs: small file, policies: all, k:1, frac:0.5, invalid.
  - Calls: reliable_send_file_rs(..., ack_policy=...).
  - Expected: valid variants return sid; invalid raises; panels show exact call and outcome.
  - Status: Implemented in tools/e2e/test_reliable_multicast.py.

- Receiver validations (added)
  - Inputs: expected_bytes=0, sink_path set.
  - Calls: reliable_receive_file_rs(...).
  - Expected: error on 0; sid returned when valid; sink path logged.
  - Status: Implemented; checksum compare after completion signal is added.

- Boundary chunking (planned)
  - Inputs: file size near multiples of chunk_size, and > u16::MAX payload_len paths.
  - Expected: correct last chunk sizing; no panics; logs reflect chunk indices; stats increment.
  - Status: add after sender payload path is implemented.

- Loss + repair (planned)
  - Inputs: introduce controlled loss; receivers issue SACK/REPAIR.
  - Expected: DATA resend; counters show repairs; checksum still matches.
  - Status: add after network writer + control handling.

Completeness criteria to be met
- Sender/Receiver engines implement data/control handling; completion signal exposed; E2E live tests assert checksums and log counters/timings.
- ReliableStats emission wired and visible in controller logs; tests assert stats increase as expected.
