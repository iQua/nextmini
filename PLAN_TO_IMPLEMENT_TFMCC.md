At a high level, what you have today is:

* **Reliability**: NACK‑oriented reliable multicast (ACK, SACK, REPAIR) implemented in `control.rs`, `sender.rs`, `receiver.rs`, orchestrated by `SessionManager`.
* **Congestion control**: PGMCC (`pgmcc.rs`), which is **window‑based** and driven entirely from **sender‑side** estimates of RTT and loss rate using ACK/SACK/REPAIR.
* **PGMCC <‑> reliability coupling**: The only signals PGMCC sees are the same ones your reliability code uses (ACK, SACK, REPAIR). SACK and NACK delays therefore directly affect the timeliness and accuracy of the loss estimate, and thus the cwnd and the implicit sending rate.

TFMCC, per RFC 4654, flips that on its head:

* Most congestion‑control logic lives at the **receivers**.
* Each receiver measures RTT and loss event rate, computes a TCP‑friendly rate using the throughput equation, and reports a **desired receive rate** Xᵣ.
* The sender chooses the **current limiting receiver (CLR)** and transmits at (roughly) that rate, using a **rate‑based** pacer.([IETF Datatracker][1])

So the big architectural change is:

> Move congestion‑control measurement to the receivers and make the sender rate‑based, **decoupling congestion control from the SACK/NACK reliability path**.

Below is a concrete, code‑level plan for doing that in your tree.

---

## 1. Goals and constraints

1. Replace PGMCC with **TFMCC** (TCP‑Friendly Multicast Congestion Control) as per RFC 4654.([IETF Datatracker][1])
2. Make the **sender rate‑based** (Bytes/s) rather than purely window‑based.
3. **Minimize coupling** between reliability (ACK/SACK/REPAIR) and congestion control.
4. Preserve:

   * Existing reliable file‑transfer semantics.
   * Python API surface (`send_file`, `receive_file`).
   * The ability to still run with `Static` or PGMCC congestion modes while TFMCC is being brought up.

---

## 2. Where PGMCC is wired today

Relevant bits:

* `reliable/session.rs`

  * `AckPolicy`, `CongestionControl::{Static, Pgmcc(PgmccConfig)}`.
* `reliable/pgmcc.rs`

  * `PgmccController`: per‑receiver state (RTT, loss_p, etc.) and `maybe_recompute(...) -> PgmccUpdate { acker, window_chunks, rate_bytes_per_s }`.
  * Loss estimates are driven by **sender‑side** `on_ack`, `on_sack`, `on_repair`.
* `reliable/sender.rs`

  * Builds a `PgmccController` when `cfg.cc == CongestionControl::Pgmcc`.
  * Each loop iteration calls `state.maybe_pgmcc_recompute()` and then `pacer.set_target_rate(...)` if needed.
  * Uses a window limit (`window_limit`, `base_window`) and `DataPacer` for pacing.
* `reliable/receiver.rs`

  * Emits `RlmControl::Ack`, `Sack`, and `Repair (NACK)` using:

    * `NackLimiter` (per‑chunk min interval).
    * `SackScheduler` (coalescing SACK gap runs on a timer).
* `reliable/control.rs`

  * Translates `RlmControl` into:

    * `CompletionPolicy` decisions (`CompletionPolicy::All`, `Threshold`, `Leader`).
    * Resend queue management.

This means PGMCC’s `loss_p` is only updated when:

* A SACK gap is emitted (potentially delayed by `sack_interval_ms`).
* A REPAIR NACK is emitted (potentially limited by `nack_min_interval_ms`).

So any delay in those control messages directly makes the sender’s loss/RTT view stale.

---

## 3. High‑level architecture for TFMCC

We’ll add a **TFMCC building block** on top of your RLM stack, with two main components:

* A **receiver‑side estimator** (per session, per receiver node):

  * Tracks sequence numbers, loss history, RTT.
  * Implements RFC 4654 Section 5 loss‑event rate calculation and Section 4.3 RTT measurement.([IETF Datatracker][1])
  * Computes desired rate Xᵣ using the TFRC/TFMCC throughput equation.([IETF Datatracker][1])
  * Participates in **feedback suppression** (Section 4.5).([IETF Datatracker][1])
* A **sender‑side controller** (per session on the sender node):

  * Maintains current sending rate X, CLR ID, R_max, feedback round number fb_nr, X_supp, etc.([IETF Datatracker][1])
  * Adjusts sending rate X per the four cases in Section 3.3 (new CLR, CLR leaves, CLR’s rate increases, etc.).([IETF Datatracker][1])
  * Drives `DataPacer` and an optional soft window.

Key principle: **TFMCC does not rely on SACK/REPAIR timing.** Those remain reliability signals only; congestion control uses its own TFMCC feedback messages.

---

## 4. Protocol / message changes

### 4.1 New congestion‑control fields in data packets

Your RLM data header currently (from the receiver code) conveys:

* `session_id`
* `index` (chunk index) – effectively a monotonic sequence number.

We’ll extend the on‑wire format used by `nextmini_messages::rlm::encode_data` / `decode_data` to add TFMCC‑specific fields when TFMCC is enabled for that session:

**Sender‑side fields (per RFC 4654 §2.2.1):**([IETF Datatracker][1])

* `seqno`: use existing `data.index` as the TFMCC sequence number i.
* `X_supp`: suppression rate (bits/s) as a compact fixed‑point or floating‑point.
* `ts_i`: sender timestamp when this packet was sent (ms‑resolution).
* `r_id`: receiver ID whose last feedback timestamp echo is piggybacked.

  * You can use `NodeId` or a stable per‑session receiver index.
* `tr_r'`: timestamp echo of that receiver’s last feedback (for RTT measurement).
* `is_clr`: bool indicating whether `r_id` is the CLR.
* `fb_nr`: 4‑bit feedback round counter.
* `R_max`: max RTT among receivers, as in RFC 4654 (ms, compact float).

Implementation detail:

* Introduce a sub‑header struct in `nextmini_messages::rlm` like:

  ```rust
  pub struct TfmccDataHeader {
      pub seqno: u64,
      pub x_supp_bits_per_s: u32, // or f32
      pub ts_i_ms: u32,
      pub r_id: u32,
      pub tr_r_echo_ms: u32,
      pub is_clr: bool,
      pub fb_nr: u8,
      pub r_max_ms: u16,
  }
  ```

* Add an optional `Option<TfmccDataHeader>` to your on‑wire data header and encode/decode only when `CongestionControl::Tfmcc` is active.

### 4.2 New TFMCC feedback control message

Add a new `RlmControl` variant:

```rust
pub enum RlmControl {
    Ack { up_to: u64 },
    Sack { base: u64, runs: Vec<(u16, u16)> },
    Repair { indices: Vec<u64> },
    Manifest { /* ... */ },
    Ready { node_id: u64 },
    Eot { last_index: u64, /* ... */ },
    // NEW:
    TfmccFeedback {
        receiver_id: u32,
        have_rtt: bool,
        have_loss: bool,
        receiver_leave: bool,
        tr_r_ms: u32,
        ts_i_echo_ms: u32,
        fb_nr_echo: u8,
        x_r_bits_per_s: u32,
    },
}
```

This mirrors RFC 4654 §2.2.2’s feedback contents.([IETF Datatracker][1])

Important: The existing reliability logic in `control.rs` should ignore `TfmccFeedback` (and likewise TFMCC logic should treat Ack/Sack/Repair as inputs only for loss detection, if at all). That keeps the two concerns separated.

---

## 5. CongestionControl enum & config changes

In `reliable/session.rs`:

```rust
#[derive(Clone, Debug)]
pub enum CongestionControl {
    Static,
    Pgmcc(PgmccConfig),
    Tfmcc(TfmccConfig), // NEW
}
```

Add:

```rust
#[derive(Clone, Debug)]
pub struct TfmccConfig {
    pub min_rate_bps: f64,
    pub max_rate_bps: f64,
    pub initial_rate_bps: f64,     // e.g., 1 pkt per R_max
    pub feedback_interval_ms: u64, // nominal feedback round length
    pub rate_smooth_alpha: f64,    // EWMA on X
    pub max_increase_per_rtt_pkts: f64, // 8s/R_max as in RFC §3.3
    pub clr_hysteresis_pct: f64,   // guard against flapping CLRs
    // maybe more knobs, but keep defaults close to RFC 4654
}
```

In `LocalConfig` (probably in `config.rs`):

* Add a `[reliable.tfmcc]` TOML section with defaults tuned similarly to the RFC’s recommended behaviour.

In `python-api/src/lib.rs::send_file`:

* Extend `congestion: Option<String>` parsing:

  ```rust
  let cc = match mode {
      "static" => CongestionControl::Static,
      "pgmcc"  => CongestionControl::Pgmcc(PgmccConfig::from(&cfg.pgmcc?)),
      "tfmcc"  => CongestionControl::Tfmcc(TfmccConfig::from(&cfg.tfmcc?)),
      other    => return Err(PyRuntimeError::new_err(format!(
          "invalid congestion control: {other}"
      ))),
  };
  ```

So you can choose `congestion="tfmcc"` from Python while you iterate.

---

## 6. Receiver‑side TFMCC logic

Add a new module: `dataplane/src/node/reliable/tfmcc.rs` for **receiver‑side state**, and another for **sender‑side** (you can house both in one file if you prefer, but splitting `TfmccReceiver` and `TfmccSender` structs makes things clearer).

### 6.1 Receiver state struct

In `tfmcc.rs`:

```rust
pub struct TfmccReceiver {
    pub receiver_id: u32,
    pub have_rtt: bool,
    pub have_loss: bool,

    // Loss history as in RFC 4654 §5 (loss events & intervals).
    loss_history: LossHistory,
    // RTT estimate and last measurement timestamps.
    rtt_s: f64,
    last_rtt_sample: Option<f64>,
    // Last computed desired rate.
    x_r_bps: f64,

    // For feedback suppression and rounds.
    last_fb_sent_at: Option<std::time::Instant>,
    last_fb_nr_seen: u8,
    last_ts_i_echo_ms: u32,
}
```

`LossHistory` holds the per‑packet reception / loss events as described in §5.1–5.6 (history of inter‑loss event intervals, discounting older history, etc.).([IETF Datatracker][1])

### 6.2 Hook into `receiver.rs`

In `receiver.rs::run`:

* Instantiate a `TfmccReceiver` when `cfg.cc` is `Tfmcc`.

  * You can pass in `cfg.common.local_node_id` as `receiver_id` or derive a random 32‑bit ID for this session.

#### 6.2.1 Loss history updates

Instead of letting the **sender** infer loss from SACK, we let the receiver track it locally:

* Extend `handle_data_frame` to feed each successfully received chunk into TFMCC’s loss history:

  ```rust
  if let Some(tfmcc) = tfmcc_state.as_mut() {
      tfmcc.on_data_arrival(idx); // records sequence arrivals & detects loss events
  }
  ```

You already have:

* `expected`, `highest_seen`, `received`, `pending` – this is enough to detect gaps and convert to “loss events” as in RFC 4654 (losses clustered within one RTT).([IETF Datatracker][1])

When you detect a new loss event per RFC’s logic, call:

```rust
tfmcc.on_loss_event(now, seqno);
```

The TFMCC receiver updates the loss event rate p based on inter‑loss intervals (Section 5.3–5.5).([IETF Datatracker][1])

#### 6.2.2 RTT measurement

We need timestamps from sender:

* From the new `TfmccDataHeader` decoded in `handle_data_frame`, extract `ts_i_ms`, `tr_r_echo_ms`, `fb_nr`, `R_max`, `is_clr`, etc., and call:

  ```rust
  tfmcc.on_data_header(ts_i_ms, tr_r_echo_ms, fb_nr, r_max_ms, now);
  ```

* Implement RTT as in RFC 4654 §4.3.2: `R_r = tr_r - ts_i'`, adjusting for local delays.([IETF Datatracker][1])

Once the receiver has at least one RTT and one loss event, it can compute Xᵣ.

#### 6.2.3 Desired rate calculation

Per RFC 4654 §2.1, use the recommended throughput equation:([IETF Datatracker][1])

```rust
fn tfrc_rate_bps(s_bytes: f64, rtt_s: f64, p: f64) -> f64 {
    let s = s_bytes;
    let r = rtt_s.max(MIN_RTT_S);
    let p = p.clamp(MIN_LOSS_P, 1.0);
    let term1 = (2.0 * p / 3.0).sqrt();
    let term2 = 12.0 * (3.0 * p / 8.0).sqrt() * p * (1.0 + 32.0 * p * p);
    8.0 * s / (r * (term1 + term2))
}
```

Then:

```rust
fn recompute_x_r(&mut self, packet_size: usize) {
    if self.have_rtt && self.have_loss {
        self.x_r_bps = tfrc_rate_bps(packet_size as f64, self.rtt_s, self.loss_history.p());
    } else {
        // Use conservative defaults (e.g., based on R_max) per RFC §3.3.
    }
}
```

#### 6.2.4 Feedback generation & suppression

Implement a method:

```rust
fn maybe_send_feedback(
    &mut self,
    now: Instant,
    last_data_header: &TfmccDataHeader,
    ctrl_io: &ControlEmitter, // the existing emitter in receiver.rs
) {
    // 1. Only eligible if x_r < X_supp unless R_r > R_max (per RFC §2.2.1/§4.5).
    // 2. Use fb_nr & fb_nr_echo to suppress feedback from older rounds.
    // 3. Apply randomization of send time within the feedback round.
}
```

Feedback suppression per RFC 4654 §4.5:

* When you see a new `fb_nr` in incoming data, that starts a new feedback round.
* Each receiver picks a random feedback delay in `[0, R_max]` weighted by its Xᵣ; those with lower rates send earlier.
* When a receiver overhears another receiver’s feedback (harder in an SSM/unicast feedback case, but for now you can keep a simpler model), it suppresses its own if that other feedback has Xᵣ ≤ its own.

Given your environment (likely a small set of datacenter nodes, maybe SSM), you can start with a **simplified suppression**:

* No overhearing between receivers (feedback is unicast).
* Suppress based on `X_supp` only: receiver sends feedback if `Xᵣ < X_supp` and **random coin flip** is below some probability `p_fb` proportional to `X_supp / Xᵣ` or group size.
* You can refine to full RFC behaviour later; for now, the important part is that **slow receivers have a much higher chance of being heard than fast ones**.

When you decide to send, you use the existing `ControlEmitter` to send a `RlmControl::TfmccFeedback`:

```rust
control_io.send(&RlmControl::TfmccFeedback {
    receiver_id: self.receiver_id,
    have_rtt: self.have_rtt,
    have_loss: self.have_loss,
    receiver_leave: false,
    tr_r_ms,
    ts_i_echo_ms,
    fb_nr_echo: last_data_header.fb_nr,
    x_r_bits_per_s: self.x_r_bps as u32,
});
```

Crucially: **this feedback is scheduled independently of SACK and REPAIR timers**.

---

## 7. Sender‑side TFMCC controller

Add to `tfmcc.rs`:

```rust
pub struct TfmccSender {
    cfg: TfmccConfig,
    packet_size: usize,

    // Current limiting receiver.
    clr_id: Option<u32>,
    x_bps: f64,          // current sending rate X
    r_max_s: f64,        // R_max
    fb_nr: u8,           // feedback round number
    last_round_start: Instant,

    // For each receiver: last RTT sample, last reported X_r, last_seen time.
    receivers: AHashMap<u32, ReceiverInfo>,
}

struct ReceiverInfo {
    x_r_bps: f64,
    rtt_s: f64,
    last_report_at: Instant,
    receiver_leave: bool,
}
```

### 7.1 Integrate TFMCC into `sender.rs`

In `SenderState`:

* Replace `pgmcc: Option<PgmccController>` with:

```rust
enum CcState {
    Static,
    Pgmcc(PgmccController),
    Tfmcc(TfmccSender),
}
```

Initialize `CcState::Tfmcc` in `SenderState::new(...)` when `cfg.cc == CongestionControl::Tfmcc`.

### 7.2 Handling TFMCC feedback

In `SenderState::handle_control(frame: InboundFrame)` (where you already call into `control::process_control_event`):

* Extend the match on `RlmControl` to handle `TfmccFeedback`:

```rust
match ctrl {
    RlmControl::TfmccFeedback { receiver_id, have_rtt, have_loss, receiver_leave, tr_r_ms, ts_i_echo_ms, fb_nr_echo, x_r_bits_per_s } => {
        if let Some(TfmccSender { .. }) = self.cc_state.tfmcc_mut() {
            self.tfmcc_on_feedback(...);
        }
    }
    // existing Ack/Sack/Repair cases unchanged
}
```

`tfmcc_on_feedback` should:

1. Compute instantaneous RTT to this receiver per RFC 4654 §3.2:

   `R_r = ts_now - ts_i'` (using the echoed timestamp).([IETF Datatracker][1])

2. Maintain R_max (including the `R_max = max(R_max, 8s/X + ts_gran)` floor).([IETF Datatracker][1])

3. Update `ReceiverInfo` entry for this `receiver_id` with Xᵣ and R_r.

4. Apply the **send‑rate adjustment rules** from RFC 4654 §3.3:([IETF Datatracker][1])

   * **Case 1: no CLR yet** ⇒ set CLR = r; X := min(Xᵣ, X + 8s/R_max).
   * **Case 2: r ≠ CLR, Xᵣ < X, receiver_leave false** ⇒ CLR = r, X := Xᵣ.
   * **Case 3: CLR is leaving** ⇒ CLR replaced by r but no immediate rate increase above current X for one feedback round.
   * **Case 4: r == CLR** ⇒ X := min(Xᵣ, X + 8s/R_max).

5. Start new feedback rounds and increment `fb_nr` per RFC §3.4, and update the on‑wire `fb_nr` in TFMCC data headers.

### 7.3 Driving the sender’s pacing

You already have:

```rust
let mut pacer = DataPacer::new(state.common.data_bucket.clone());
...
if let Some(rate) = state.maybe_pgmcc_recompute() {
    pacer.set_target_rate(rate);
}
```

Replace `maybe_pgmcc_recompute` with:

```rust
fn maybe_update_cc(&mut self) -> Option<f64> {
    match &mut self.cc_state {
        CcState::Static => None,
        CcState::Pgmcc(pgmcc) => pgmcc.maybe_recompute(...).map(|u| u.rate_bytes_per_s),
        CcState::Tfmcc(tfmcc) => {
            let rate = tfmcc.current_rate_bytes_per_s();
            Some(rate)
        }
    }
}
```

And in the event loop:

```rust
if let Some(rate) = state.maybe_update_cc() {
    pacer.set_target_rate(rate);
}
```

For TFMCC, you can also set a soft window:

```rust
fn window_limit(&self) -> usize {
    match &self.cc_state {
        CcState::Tfmcc(tfmcc) => {
            let r = tfmcc.r_max_s.max(MIN_RTT_S);
            let cwnd_chunks = (tfmcc.x_bps * r / (8.0 * self.common.chunk_size as f64))
                                .clamp(1.0, self.total_chunks as f64);
            cwnd_chunks as usize
        }
        _ => self.base_window,
    }
}
```

This keeps **reliability logic** (inflight limit, completion policy) basically unchanged, while TFMCC’s **rate** is enforced via `DataPacer`.

---

## 8. Handling SACK / NACK delays under TFMCC

Currently:

* `SackScheduler` coalesces SACKs with `sack_interval_ms` and only emits when `ready(...)` and timed out.
* `NackLimiter` suppresses repeated NACKs for the same chunk within `nack_min_interval_ms`.

Those directly feed `PgmccController::on_sack` / `on_repair`, so delays smear loss information in time.

With TFMCC:

1. **TFMCC’s loss and RTT estimates live entirely on the receiver**, using immediate local detection of missing or marked packets and timestamp fields in data packets. They **do not depend** on when SACKs or NACKs are emitted.
2. The only thing SACK/REPAIR timing affects is **how quickly reliability repair happens**.
3. You can therefore:

   * Keep `SackScheduler` and `NackLimiter` tuned for “reasonable” repair load without being terrified that they will distort congestion control.
   * Optionally tighten them for small groups/short transfers without affecting TFMCC’s correctness.

Concrete tuning suggestions once TFMCC is active:

* For sessions using `CongestionControl::Tfmcc`, allow **shorter SACK intervals** by default, e.g.:

  ```rust
  let sack_interval = if using_tfmcc {
      Duration::from_millis(5)
  } else {
      Duration::from_millis(cfg.sack_interval_ms)
  };
  ```

  This speeds up repair and also makes loss detection for reliability more timely.

* Keep `NackLimiter` roughly where it is now to avoid NACK storms, but consider making `nack_min_interval_ms` proportional to RTT (`~0.5 * R_max`) rather than a fixed config value.

The key point: **even if you keep the current SACK/NACK delays unchanged, TFMCC’s rate control will now be based on local receiver measurements and periodic TFMCC feedback**, not on those delayed reliability messages. That’s the big win.

---

## 9. Python API and tooling

Once TFMCC is implemented:

* Expose a **“tfmcc” mode** in the Python binding:

  ```python
  dp.send_file(
      group_ip="239.0.0.1",
      receiver_ids=[2,3,4],
      tensor_path="...",
      congestion="tfmcc",
      ack_policy="all",  # still used for reliability completion, not CC
  )
  ```

* Consider adding an optional **“min_rate”** / **“max_rate”** tuning knob in the TOML config so job scripts don’t have to touch internals.

* Add instrumentation endpoints:

  * Current CLR id.
  * `X` (sender rate), `R_max`, per‑receiver `X_r`.
  * These can be exposed over your existing controller interface or via logs.

---

## 10. Migration / implementation order

To keep risk manageable, I’d do this in phases:

1. **Refactor CC plumbing without behaviour change**

   * Introduce `CongestionControl::Tfmcc(TfmccConfig)` and `CcState` enum, but keep only `Static` + `Pgmcc` active.
   * Move all PGMCC‑specific bits into dedicated methods so the call sites become CC‑agnostic.

2. **Add TFMCC fields to messages**

   * Extend `rlm::encode_data/decode_data` and `RlmControl` with TFMCC fields/variant.
   * Wire through `receiver.rs` and `sender.rs` to **parse and ignore** TFMCC headers/feedback for now.

3. **Implement TFMCC receiver‑side logic**

   * `tfmcc.rs::TfmccReceiver` with:

     * Loss history (Section 5).
     * RTT measurement (Section 4.3).
     * Desired rate computation (Section 4.4 + eq. (1)).([IETF Datatracker][1])
   * Unit tests for:

     * Loss history → loss event rate.
     * RTT update smoothing.
     * Xᵣ monotonicity under various p, R.

4. **Implement TFMCC sender‑side controller**

   * `TfmccSender` per §3.1–3.3: X, CLR selection, R_max updates.([IETF Datatracker][1])
   * Unit tests for:

     * Rate increase cap (8s/R_max).
     * CLR change hysteresis.
     * Drop‑based halving when CLR feedback is missing.

5. **Wire feedback generation / suppression**

   * Implement `TfmccReceiver::maybe_send_feedback` and send `RlmControl::TfmccFeedback`.
   * In sender, process these feedbacks and adjust the pacer.

6. **Enable TFMCC behind a feature flag or config knob**

   * For initial deployments, use a config like:

     ```toml
     [reliable]
     cc = "tfmcc"
     ```

   * Keep PGMCC available for A/B comparisons.

7. **Tuning and validation**

   * Use your existing harnesses under `docs/testing` and `examples` to run:

     * single‑bottleneck comparisons vs TCP iperf3 flows.
     * heterogenous receivers (short/long RTT, different capacities).
   * Measure:

     * Rate fairness vs TCP.
     * Smoothness vs PGMCC.
     * Robustness when you crank up `sack_interval_ms` and `nack_min_interval_ms` to confirm CC is now decoupled.

---

## 11. Summary of how this fixes the SACK/NACK delay problem

Today:

* **Loss measurements are only visible at the sender when SACKs or NACKs arrive.**
* SACK coalescing and NACK limiting – which you need for reliability scalability – directly blur the loss signal PGMCC uses.
* That leads to:

  * Over‑optimistic loss estimates during bursts.
  * Under‑reaction to congestion.
  * Rate oscillations due to delayed “oh, hey, we dropped a lot” signals.

After this TFMCC plan:

* **Each receiver measures loss event rate and RTT locally**, immediately, per TFMCC.([IETF Datatracker][1])
* Receivers periodically send **explicit rate reports** (Xᵣ) in `RlmControl::TfmccFeedback`, independent of SACK/NACK timers.
* The sender’s rate is chosen based on the **worst receiver’s Xᵣ**, updated smoothly and bounded by 8s/R_max per RTT.([IETF Datatracker][1])
* SACK and REPAIR timers are free to be tuned for reliability without corrupting congestion control.

So you end up with:

* A **rate‑based** multicast congestion controller that matches RFC 4654 semantics.
* Clean separation between reliability and congestion control.
* Much more accurate and timely rate adaptation – even if SACK and NACK delays remain conservative for robustness.

[1]: https://datatracker.ietf.org/doc/html/rfc4654 "
            
                RFC 4654 - TCP-Friendly Multicast Congestion Control (TFMCC): Protocol Specification
            
        "

