Here’s a concrete, file-by-file plan to bolt PGMCC onto the reliable multicast stack you’ve got, with minimal disruption and a clean feature gate.

I’ll break it into phases, but everything is concrete enough that you can start wiring code from this.

---

## 0. Design goals & constraints

**Goals**

* Make the **sender’s window (`SenderState::window`) and pacing (`DataPacer`) follow a PGMCC-style ACKer**, i.e. the “slowest” receiver in TCP-friendly terms (RTT + loss).
* **Do not break existing RLM reliability semantics** (ACK/SACK/Repair, Manifest/EOT).
* Keep it **feature/config gated**, so you can turn PGMCC on per-session and fall back to the current static window + token bucket behavior.

**Key choices**

* **No change to data frame layout.** We keep `RlmData` as `{index, payload_len}`; we infer RTT at the sender using the time between *data send* and *control receive* that references that index.
* **Add new control frames for PGMCC feedback (optional).** But the core algorithm can work off existing `Ack` + `Sack` + `Repair`. New control frames just make it nicer and more explicit.
* **PGMCC logic lives primarily on the sender side.** Receivers still generate ACK/SACK/Repair as today; we treat “ACKer” as the receiver that yields the lowest TCP-friendly rate estimate, computed at the sender from per-receiver stats.
* Existing **`AckPolicy` / `CompletionPolicy` stay in charge of reliability**, independent of the congestion window. PGMCC decides how fast we *attempt* to send; `CompletionPolicy` decides when chunks are retired.

---

## 1. Configuration & feature gating (session.rs)

### 1.1 Add a congestion control mode

In `dataplane/src/node/reliable/session.rs`:

```rs
/// Sender-side congestion control mode.
#[derive(Clone, Debug)]
pub enum CongestionControl {
    /// Current behavior: static window + optional token bucket.
    Static,
    /// PGMCC: sender window & pacing driven by a designated ACKer.
    Pgmcc(PgmccConfig),
}

#[derive(Clone, Debug)]
pub struct PgmccConfig {
    /// Minimum congestion window in chunks.
    pub min_cwnd_chunks: usize,
    /// Maximum congestion window in chunks (upper bound; also capped by static window).
    pub max_cwnd_chunks: usize,
    /// Initial cwnd in chunks (slow-start equivalent).
    pub init_cwnd_chunks: usize,
    /// EWMA smoothing for RTT [0,1].
    pub rtt_alpha: f64,
    /// EWMA smoothing for loss probability [0,1].
    pub loss_alpha: f64,
    /// Minimum RTT in ms to clamp wild samples.
    pub min_rtt_ms: u64,
    /// Feedback / recompute interval for PGMCC decisions (ms).
    pub feedback_interval_ms: u64,
    /// Hysteresis when changing ACKer (percentage drop required in rate).
    pub acker_hysteresis_pct: f64,
}
```

Extend `SenderConfig`:

```rs
#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_path: Option<String>,
    pub checksum_out: bool,
    pub ack_policy: AckPolicy,
    pub repair_backoff_ms: u64,
    pub fec_k: Option<u16>,
    pub fec_p: u8,
    pub ready_grace_ms: u64,

    // NEW:
    pub cc: CongestionControl,
}
```

Optionally allow receiver-side toggles (mostly for future optimization, not strictly needed for correctness):

```rs
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub verify_checksum: bool,
    pub sink_path: Option<String>,
    pub nack_min_interval_ms: u64,
    pub nack_jitter_ms: u64,
    pub sack_interval_ms: u64,

    // NEW:
    pub pgmcc_enabled: bool,
}
```

**Control-plane impact:** wherever `SenderConfig` and `ReceiverConfig` are constructed (controller side), add knobs to enable `CongestionControl::Pgmcc(...)` on a per-session basis. If not supplied, default to `Static`.

---

## 2. Wire format: optional PGMCC control frames (rlm.rs)

You *can* implement an MVP without new control frames, but it’s useful long-term to carry explicit PGMCC feedback. Plan for it now.

### 2.1 Extend `RlmCtrlKind`

In `messages/src/rlm.rs`:

```rs
pub enum RlmCtrlKind {
    Data = 1,
    Control = 2,
    // ... these are internal, used as `ctrl_kind` u8 in the header
}
```

You don’t expose this enum directly, but you map to it in `encode_control` / `decode_control`. Extend it conceptually with:

```rs
pub enum RlmCtrlKind {
    Manifest = 1,
    Ready = 2,
    Ack = 3,
    Sack = 4,
    Repair = 5,
    Eot = 6,
    PgmccFeedback = 7,
    PgmccAcker = 8,
}
```

(You don’t have to expose this enum publicly; just use these numeric constants in `encode_control` / `decode_control`.)

### 2.2 Extend `RlmControl` enum

Add two variants:

```rs
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RlmControl {
    Manifest { ... },
    Ready { node_id: u64 },
    Ack { up_to: u64 },
    Sack { base: u64, runs: Vec<(u16,u16)> },
    Repair { indices: Vec<u64> },
    Eot { last_index: u64, checksum: Option<[u8; 32]> },

    /// Receiver → sender: PGMCC congestion feedback derived from this receiver’s path.
    PgmccFeedback {
        node_id: u64,
        /// Highest contiguous chunk the receiver considers delivered.
        acked_upto: u64,
        /// Smoothed RTT in milliseconds, scaled (e.g. x8 or x16) for fractional precision.
        rtt_ms_x8: u32,
        /// Loss event probability p, scaled as fixed-point (e.g. p * 1e6).
        loss_event_rate_x1e6: u32,
    },

    /// Sender → all receivers: notify which node is currently the ACKer.
    PgmccAcker {
        node_id: u64,
    },
}
```

### 2.3 Update `encode_control` / `decode_control`

Add match arms:

* In `encode_control`:

  * For `PgmccFeedback`:

    * Body layout: `[node_id: u64][acked_upto: u64][rtt_ms_x8: u32][loss_event_rate_x1e6: u32]` ⇒ 8+8+4+4 = 24 bytes.
    * `ctrl_kind = RlmCtrlKind::PgmccFeedback as u8`.

  * For `PgmccAcker`:

    * Body layout: `[node_id: u64]` ⇒ 8 bytes.
    * `ctrl_kind = RlmCtrlKind::PgmccAcker as u8`.

* In `decode_control`:

  * Add cases for those `ctrl_kind`s, validating `body_len` and slicing fields accordingly.
  * Existing variants keep working as-is; you only accept the new kinds when encountered.

### 2.4 Tests

Extend `roundtrip_controls()` with a couple of PGMCC variants and assert equality after encode/decode.

---

## 3. Sender-side: PGMCC controller & dynamic window (sender.rs)

This is the heart of the change.

### 3.1 Track first-send timestamps per chunk

In `SenderState` add a map:

```rs
use std::collections::BTreeMap;
use std::time::Instant;

struct SenderState {
    // existing fields...

    base_window: usize,          // replaces current `window` as static upper bound
    window: usize,               // current dynamic window (cwnd in chunks)
    pgmcc: Option<PgmccController>,

    // First-send timestamps for chunks, used for RTT estimation.
    first_send_times: BTreeMap<u64, Instant>,
}
```

In `SenderState::new`:

* Compute `base_window` instead of `window`:

```rs
let base_window = compute_window(&cfg);
let (window, pgmcc) = match &cfg.cc {
    CongestionControl::Static => (base_window, None),
    CongestionControl::Pgmcc(pcfg) => {
        let mut controller = PgmccController::new(pcfg.clone(), cfg.receiver_ids.clone());
        let init_cwnd = pcfg.init_cwnd_chunks.clamp(pcfg.min_cwnd_chunks, pcfg.max_cwnd_chunks);
        controller.set_cwnd(init_cwnd as f64);
        (init_cwnd, Some(controller))
    }
};
```

* Initialize `first_send_times` as empty.

In `send_data_chunk`:

```rs
fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
    let frame = rlm::encode_data(self.session_id, chunk.index, &chunk.data);
    let fragments = self.fragment_frame(frame);

    // record first-send time for RTT
    let now = Instant::now();
    self.first_send_times.entry(chunk.index).or_insert(now);

    self.enqueue_frame(chunk.index, fragments.clone());
    // ... existing accounting ...

    for bytes in fragments.iter() {
        self.send_frame(bytes, processors);
    }
}
```

In `send_resend` you **do not** overwrite `first_send_times` — RTT should be based on first transmission.

### 3.2 Make `DataPacer` dynamic-rate aware

Right now, `DataPacer` takes a static `TokenBucketSpec` and never changes its rate.

Change `DataPacer` to store a simple “effective” rate:

```rs
struct DataPacer {
    base_spec: Option<nextmini_messages::TokenBucketSpec>,
    /// Effective rate (bytes/s). For static mode, derived from base_spec.rate.
    rate_bytes_per_s: f64,
    tokens: f64,
    last: Instant,
}
```

* In `new(spec)`:

  * If `spec` is `Some`, set `rate_bytes_per_s = spec.rate as f64`.
  * If `None`, set `rate_bytes_per_s = 0.0` and treat that as “no pacing” (current behavior).

* Add a method:

```rs
impl DataPacer {
    fn set_target_rate(&mut self, bytes_per_s: f64) {
        if bytes_per_s <= 0.0 {
            // fallback: keep static/base rate
            return;
        }
        self.rate_bytes_per_s = bytes_per_s;
    }

    fn refill(&mut self, spec: &nextmini_messages::TokenBucketSpec) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        let rate = if self.rate_bytes_per_s > 0.0 {
            self.rate_bytes_per_s
        } else {
            spec.rate as f64
        };
        self.tokens = (self.tokens + elapsed * rate).min(spec.bucket_size as f64);
        self.last = now;
    }

    async fn wait_for(&mut self, bytes: usize) {
        let Some(base_spec) = self.base_spec.clone() else {
            // Pacing disabled
            return;
        };
        // ... use rate_bytes_per_s in refill and sleep calculation ...
    }
}
```

Now PGMCC can call `pacer.set_target_rate(...)` without touching `TokenBucketSpec`.

### 3.3 Add `PgmccController`

At the top of `sender.rs` (or in a new `pgmcc.rs` module re-exported from `mod.rs`):

```rs
use ahash::AHashMap;

struct PerReceiverStats {
    last_ack_index: u64,
    last_ack_time: Option<Instant>,
    /// Smoothed RTT in seconds.
    rtt: f64,
    /// Smoothed loss event probability p \in (0,1).
    loss_p: f64,
    /// Total new chunks ACKed (for this receiver).
    acked_chunks: u64,
    /// Unique chunks this receiver has reported missing.
    lost_chunks: BTreeSet<u64>,
}

pub struct PgmccController {
    cfg: PgmccConfig,
    per_receiver: AHashMap<usize, PerReceiverStats>,
    /// Node id currently treated as ACKer.
    acker: Option<usize>,
    /// Global congestion window (chunks).
    cwnd_chunks: f64,
    /// Last time we recomputed cwnd.
    last_update: Instant,
}
```

Core methods:

```rs
impl PgmccController {
    pub fn new(cfg: PgmccConfig, receivers: Vec<usize>) -> Self { ... }

    pub fn set_cwnd(&mut self, cwnd: f64) { ... }

    pub fn on_ack(
        &mut self,
        from_node: usize,
        ack_up_to: u64,
        send_times: &BTreeMap<u64, Instant>,
        now: Instant,
        chunk_size: usize,
    ) {
        // 1. RTT sample: now - first_send_times[ack_up_to]
        // 2. Update PerReceiverStats.rtt with EWMA(cfg.rtt_alpha)
        // 3. Update acked_chunks based on delta of last_ack_index
        // 4. Maybe store last_ack_index, last_ack_time
    }

    pub fn on_sack(
        &mut self,
        from_node: usize,
        base: u64,
        runs: &[(u16, u16)],
    ) {
        // Convert runs to absolute chunk indices.
        // For each new gap index, mark in lost_chunks and update loss_p using EWMA(cfg.loss_alpha).
    }

    pub fn on_repair(
        &mut self,
        from_node: usize,
        indices: &[u64],
    ) {
        // Optionally treat each index as a loss event too (depending on design).
    }

    pub fn maybe_recompute(&mut self, now: Instant, base_window: usize) -> Option<(usize, usize, f64)> {
        // Only recompute every cfg.feedback_interval_ms
        // 1. For each receiver with valid RTT, loss_p, compute TCP-friendly window estimate:
        //    cwnd_i = tcp_friendly_cwnd(rtt_i, loss_p_i, chunk_size).
        // 2. Pick receiver with smallest cwnd_i (slowest path) as candidate_acker.
        // 3. Apply hysteresis: only switch acker if candidate’s cwnd is
        //    cfg.acker_hysteresis_pct lower than current acker’s cwnd.
        // 4. Set self.acker and self.cwnd_chunks to chosen cwnd, clamped to [min_cwnd, min(max_cwnd, base_window)].
        // 5. Return (acker_id, new_window_chunks, estimated_rate_bytes_per_s) or None.
    }

    pub fn effective_window(&self, base_window: usize) -> usize {
        if self.cwnd_chunks <= 0.0 {
            base_window
        } else {
            self.cwnd_chunks.round() as usize
        }
    }

    pub fn acker(&self) -> Option<usize> { self.acker }
}
```

Define the TCP-friendly window helper (in `pgmcc` module or inline):

```rs
fn tcp_friendly_cwnd(rtt_s: f64, p: f64, chunk_bytes: usize) -> f64 {
    if rtt_s <= 0.0 {
        return f64::INFINITY;
    }
    if p <= 0.0 {
        // no observed loss → don't bound cwnd from above here;
        // caller will clamp to max_cwnd/base_window.
        return f64::INFINITY;
    }

    let s = chunk_bytes as f64;
    let b = 1.0;
    let t_rto = 4.0 * rtt_s;

    // Classic TCP-friendly formula (Padhye et al. / TFRC-style):
    // T(p,R) = s / ( R * (sqrt(2bp/3) + t_RTO * (3 * sqrt(3bp/8) * p * (1 + 32 p^2))) )
    let p_term = (2.0 * b * p / 3.0).sqrt();
    let t_term = t_rto * (3.0 * (p * (0.75 * b * p).sqrt()) * p * (1.0 + 32.0 * p * p));
    let denom = rtt_s * (p_term + t_term);
    if denom <= 0.0 {
        return f64::INFINITY;
    }
    let throughput = s / denom;          // bytes / s
    let cwnd = throughput * rtt_s / s;   // packets in flight
    cwnd
}
```

(Exact algebra can be tuned; the plan is that you implement a Padhye/TFRC-ish function and clamp carefully.)

### 3.4 Hook PGMCC into the sender event loop

In `SenderState`, add helpers:

```rs
impl SenderState {
    fn effective_window(&self) -> usize {
        if let Some(pgmcc) = &self.pgmcc {
            pgmcc.effective_window(self.base_window)
        } else {
            self.base_window
        }
    }

    fn maybe_pgmcc_recompute(&mut self) -> Option<(usize, f64)> {
        let now = Instant::now();
        let chunk_size = self.common.chunk_size;
        if let Some(pgmcc) = &mut self.pgmcc {
            if let Some((acker_id, new_window, rate_bytes_per_s)) =
                pgmcc.maybe_recompute(now, self.base_window)
            {
                self.window = new_window;
                tracing::debug!(
                    session_id = self.session_id,
                    acker = acker_id,
                    cwnd_chunks = new_window,
                    rate_bps = rate_bytes_per_s * 8.0,
                    "RLM PGMCC: updated congestion window from ACKer"
                );
                return Some((new_window, rate_bytes_per_s));
            }
        }
        None
    }
}
```

#### In `run` loop

* After draining control frames and before trying to send new data:

```rs
// Before checking `ready_for_data`
if let Some((_win_chunks, rate_bytes_per_s)) = state.maybe_pgmcc_recompute() {
    if let Some(pacer) = pacer.as_mut() {
        pacer.set_target_rate(rate_bytes_per_s);
    }
}
```

* When gating new data:

```rs
let max_inflight = state.effective_window();
if state.ready_for_data()
    && !chunk_source.finished()
    && state.inflight_len() < max_inflight
{
    pacer.wait_for(state.common.chunk_size).await;
    // send_data_chunk(...)
}
```

* For **retransmissions**, make them obey the same pacer:

```rs
if !progressed && state.should_resend() && last_resend.elapsed() >= state.repair_backoff {
    pacer.wait_for(state.common.chunk_size).await;
    if state.send_resend(&processors) {
        last_resend = Instant::now();
        progressed = true;
    }
}
```

### 3.5 Feed control frames into PGMCC

In `SenderState::handle_control`:

```rs
fn handle_control(&mut self, frame: InboundFrame) {
    let InboundFrame { bytes, peer_id, .. } = frame;
    let Some((_, control)) = rlm::decode_control(&bytes) else { ... };

    let now = Instant::now();
    match &control {
        RlmControl::Ready { node_id } => { ... }

        // Ignore Manifest/Eot loops as before.
        RlmControl::Manifest { .. } | RlmControl::Eot { .. } => { /* unchanged */ }

        RlmControl::PgmccFeedback {
            node_id,
            acked_upto,
            rtt_ms_x8,
            loss_event_rate_x1e6,
        } => {
            // Optional: alternative feedback path if you decide to compute RTT & p at receivers.
            if let Some(pgmcc) = &mut self.pgmcc {
                let rtt_s = (*rtt_ms_x8 as f64 / 8.0) / 1000.0;
                let p = (*loss_event_rate_x1e6 as f64) / 1e6;
                pgmcc.on_external_feedback(*node_id as usize, *acked_upto, rtt_s, p, now);
            }
        }

        RlmControl::PgmccAcker { .. } => {
            // Optional: if you propagate acker choice to receivers; sender may ignore.
        }

        _ => {
            let Some(from_node) = peer_id else { ...; return; };

            // Feed PGMCC internal estimator before reliability logic
            if let Some(pgmcc) = &mut self.pgmcc {
                match &control {
                    RlmControl::Ack { up_to } => {
                        pgmcc.on_ack(
                            from_node,
                            *up_to,
                            &self.first_send_times,
                            now,
                            self.common.chunk_size,
                        );
                    }
                    RlmControl::Sack { base, runs } => {
                        pgmcc.on_sack(from_node, *base, runs);
                    }
                    RlmControl::Repair { indices } => {
                        pgmcc.on_repair(from_node, indices);
                    }
                    _ => {}
                }
            }

            // Existing reliability semantics:
            let retired = control::process_control_event(
                from_node,
                &control,
                &mut self.inflight,
                &mut self.resend_queue,
                self.receiver_count.max(1),
                &self.completion_policy,
            );
            // ... retire chunks + logging as today ...
        }
    }
}
```

PGMCC uses a superset of the existing control stream; reliability behavior is unchanged.

---

## 4. Receiver-side adjustments (receiver.rs)

You can get a working PGMCC sender **without touching the receiver** beyond understanding new control frames, but we can plan some optional improvements.

### 4.1 Optional PGMCC per-receiver estimator

If you want receivers to compute loss/RTT and emit explicit `PgmccFeedback`:

* Add small state inside `receiver::run`:

```rs
struct ReceiverPgmcc {
    enabled: bool,
    last_feedback: Instant,
    last_feedback_index: u64,
    rtt_ms_x8: u32,
    loss_event_rate_x1e6: u32,
    // Optionally more fine-grained data.
}
```

* As `handle_data_frame` advances `expected` / `highest_seen`, and as `handle_control_frame` sees Manifest / EOT, update local counters (e.g., count loss events when calling `NackLimiter` / emitting `Repair`).

* Periodically (e.g., every `ReceiverConfig::sack_interval_ms` or a dedicated `pgmcc_feedback_interval_ms`), emit:

```rs
if pgmcc.should_emit_feedback(now, expected.saturating_sub(1)) {
    control_io.send(&RlmControl::PgmccFeedback {
        node_id: cfg.common.local_node_id as u64,
        acked_upto: expected.saturating_sub(1),
        rtt_ms_x8: pgmcc.rtt_ms_x8,
        loss_event_rate_x1e6: pgmcc.loss_event_rate_x1e6,
    });
}
```

How you compute those two values is flexible; you can start simple (e.g., approximating RTT based on data inter-arrival and a configured baseline) and let the sender-side estimator dominate.

### 4.2 Handling PgmccAcker (optional)

If you want to reduce ACK implosion once PGMCC is mature:

* In `handle_control_frame`, add a branch:

```rs
RlmControl::PgmccAcker { node_id } => {
    let is_acker = node_id == cfg.common.local_node_id as u64;
    pgmcc.set_is_acker(is_acker);
    // If !is_acker: down-sample Ack frequency, rely mainly on SACK + Repair.
    return true;
}
```

* Non-ACKers could:

  * Still send ACKs but at a lower rate (e.g., only when `expected` advances by K chunks).
  * Continue sending SACK/Repair so reliability isn’t affected.

This optimization is optional and can be a later phase.

---

## 5. Control utilities (control.rs)

`dataplane/src/node/reliable/control.rs` currently:

* Encodes SACK semantics (gaps are missing chunks).
* Applies `CompletionPolicy` and manages inflight/resend queues.

You **don’t need to change its APIs** for PGMCC, but:

* Extend tests to ensure `process_control_event` correctly handles SACK / Ack patterns that PGMCC will generate more frequently.
* If you find yourself duplicating SACK-gap → indices logic in `PgmccController::on_sack`, you can factor a helper in this module:

```rs
pub fn expand_gap_runs(base: u64, runs: &[(u16, u16)]) -> impl Iterator<Item = u64> + '_ {
    runs.iter().flat_map(move |(delta, len)| {
        let start = base + (*delta as u64);
        let end = start + (*len as u64);
        start..end
    })
}
```

Then reuse that both in `process_control_event` and in PGMCC’s per-receiver loss accounting.

---

## 6. Processor & reliable runtime wiring

### 6.1 Processor / ReliableHandle (processor.rs, api.rs, session.rs)

No structural changes needed:

* `Processor::try_deliver_reliable` already delivers frames into `ReliableHandle::deliver(...)`.
* `ReliableInboundFrame` simply wraps the bytes + peer_id; PGMCC control frames are just new `RlmControl` variants parsed by `sender::handle_control`.

The only thing to keep in mind: **PGMCC is entirely within the `reliable` feature-gated path**, so all new structs and methods should be under `#[cfg(feature = "reliable")]` when appropriate (mirroring existing code).

### 6.2 Trace helpers (trace.rs)

Optional:

* You may want a new helper to recognize PGMCC control frames in logs, similar to `manifest_from_bytes`:

```rs
pub fn pgmcc_meta_from_bytes(bytes: &[u8]) -> Option<(u64 /*session_id*/, /*maybe more*/)> {
    let (header, control) = rlm::decode_control(bytes)?;
    match control {
        RlmControl::PgmccFeedback { .. } | RlmControl::PgmccAcker { .. } => {
            Some((header.session_id, /* etc */))
        }
        _ => None,
    }
}
```

Then sprinkle debug logs when such frames pass through `Processor::send_packet` for observability.

---

## 7. Mapping ACKer → window & rate

To make it explicit how the ACKer drives the sender:

1. **Per receiver**, PGMCC estimates:

   * Smoothed RTT `rtt_i`.
   * Smoothed loss probability `p_i`.

2. From these, it computes a **TCP-friendly congestion window** `cwnd_i` (in chunks) using a Padhye/TFRC-style function.

3. Among all receivers with valid samples, the **ACKer candidate** is the one with the **lowest `cwnd_i`** (i.e. lowest sustainable throughput):

   ```rs
   // pseudo
   let (acker, cwnd_min) = per_receiver
       .iter()
       .filter(|(_, st)| st.rtt > 0.0)
       .map(|(id, st)| (*id, tcp_friendly_cwnd(st.rtt, st.loss_p, chunk_bytes)))
       .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal));
   ```

4. Apply **hysteresis**: only switch from current ACKer A to candidate B if

   ```text
   cwnd_B < cwnd_A * (1.0 - acker_hysteresis_pct)
   ```

5. The **global window** is set to `cwnd_acker`, clamped to `[min_cwnd_chunks, min(max_cwnd_chunks, base_window)]`.

6. The **target pacing rate** is

   ```text
   rate_bytes_per_s = (cwnd_acker * chunk_size) / rtt_acker
   ```

`SenderState` then:

* Limits **inflight chunks** to `window = floor(cwnd_acker)`.
* Calls `DataPacer::set_target_rate(rate_bytes_per_s)` so all data/resend transmissions are spaced at that rate.

---

## 8. Interaction with existing AckPolicy / CompletionPolicy

* `AckPolicy` parsed in `messages/src/rlm.rs` and the `CompletionPolicy` in `control.rs` continue to define **when chunks are retired** (e.g., All / K-of-N / leader).
* PGMCC’s cwnd and pacing operate **orthogonally**:

  * You might send only 10 chunks per RTT due to congestion, but still require `K-of-N` ACKs per chunk before retirement.
* For PGMCC sessions, consider defaulting AckPolicy to `All` or `K-of-N` with a reasonably large K to avoid “retiring too early” when ACKer is much slower than others.

No code changes are required to tie them together; just be conscious of config defaults.

---

## 9. Testing & rollout plan

### 9.1 Unit tests

* **RLM control round-trips**:

  * Add tests that `PgmccFeedback` and `PgmccAcker` encode/decode correctly.
* **PgmccController**:

  * Synthetic RTT & loss traces for 1 receiver:

    * Assert that increasing loss or RTT reduces cwnd and rate.
  * Multi-receiver case:

    * Receiver A: low RTT, low loss → larger cwnd.
    * Receiver B: higher RTT or loss → smaller cwnd.
    * Assert ACKer becomes B and `effective_window` equals B’s cwnd.

### 9.2 Integration tests

Extend `examples/reliable_multicast` or `tools/e2e/test_reliable_multicast.py`:

* Single receiver:

  * Run with PGMCC enabled; inject synthetic loss on the path (via iptables / tc).
  * Verify (through logs or metrics) that cwnd shrinks and send rate drops.

* Two receivers with asymmetric loss:

  * Receiver 1: clean path.
  * Receiver 2: lossy path.
  * Assert that the ACKer becomes receiver 2 and global cwnd follows its metrics.

### 9.3 Incremental rollout

1. Implement PGMCC **sender-side only**, using only existing `Ack` / `Sack` / `Repair` (no new control frames yet):

   * `PgmccController::on_ack/on_sack` only.
   * No `PgmccFeedback` / `PgmccAcker`.

2. Once stable:

   * Add optional `PgmccFeedback` from receivers for richer metrics.
   * Optionally reduce non-ACKer ACK volume using `PgmccAcker`.

Because all changes are guarded behind `CongestionControl::Pgmcc`, you can run existing deployments unchanged while gradually enabling the new mode for specific sessions.

---

If you want, next step I can sketch the actual Rust type signatures and a minimal PgmccController skeleton that compiles against your codebase, but this plan should be enough to start implementing PGMCC in a controlled, incremental way.

