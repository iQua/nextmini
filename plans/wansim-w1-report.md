# wansim W1 report — independent protocol endpoints and experiment 0

Date: 2026-07-17

Branch: `perfect-fec-runtime`

Baseline: `6888b86` (approved W0b)
Evidence class: **model-level causal evidence, not a WAN measurement**

## Verdict

W1 is complete. The independent section-P carousel endpoint, rounds baseline, and three striped
baselines all run over the W0b hop-TCP/fan-out/runtime substrate. Experiment 0 was stopped at its
first falsification as required, diagnosed, and then explicitly resumed to quantify the mechanism.

The strict-conservation premise held at the byte transport and application boundaries: every one of
the 640 main-grid trials recorded zero physical-link drops and zero receiver-inbox drops. Prediction
E0-a nevertheless **failed**. A `SourceDone` sent on a separate control connection can overtake data
that was already admitted by the source but remains queued or in flight on multi-hop data
connections. TCP preserves order within a connection, not across connections. The receivers
therefore issued positive, cached round deficits without any loss.

The remaining predictions passed as predeclared. Homogeneous paths gave identical local completion
for rounds and carousel. With crossed path capacities, carousel completed 1.642–1.795 ms sooner,
still inside the 6.1 ms control-latency tolerance, and beat every striped baseline. Its bandwidth
price was an ACK-flight tail of 20 frames with one receiver and 28 with three receivers. That tail is
31.25–43.75% of K=64, so it is causally classified but not described as “small.”

## Phase 1 — independent endpoints

The simulator does not invoke production session actors. Section P of
[`perfect-fec-runtime.md`](perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages)
is the semantic source; the implementation lives under `wansim/src/protocol/` and the nexosim
adapters under `wansim/src/overlay/w1_{source,receiver}.rs`.

### Carousel

- The sender freezes the configured peer set, continuously chooses any writable tree, generates a
  fresh ideal-code symbol, and rechecks cumulative completion immediately before admitting the
  frame to a tree's TCP send buffer.
- Per-peer `BlockAck` state is a join-semilattice. Inbound snapshots must be canonical, sorted,
  disjoint, and non-touching; a range starting at the watermark is folded into the prefix. Union is
  monotone, duplicates and reorderings are valid no-progress alive signals, and wire truncation
  keeps the lowest-ID ranges.
- `last_ack_seen` and `last_ack_progress` are independent clocks. Silence or live-without-progress
  aborts the whole sender. An aborted sender cannot emit again.
- Missing peers receive targeted `AckProbe`s. Completion requires cumulative completion from every
  frozen peer, after which the sender sends three best-effort `SessionComplete` copies. Receiver
  BlockAcks use debounce, heartbeat, and probe response; a locally complete receiver stays passive
  until `SessionComplete` or its replay window expires.
- The passive window is checked to exceed the longer stall-abort budget. Empty frozen quorums are
  immediate success for carousel, rounds, and striped senders.

### Rounds and striped baselines

- Rounds emits exactly K initial symbols, submits `SourceDone(round_id)` after those application
  writes have been accepted, waits for every receiver's cached `Need`, emits the maximum reported
  deficit, and repeats. The control path has real RTT; there is no synthetic one-RTT delay.
- Equal-split and largest-remainder rate-proportional striping emit finite per-tree quotas.
  Per-stripe FEC continuously emits fresh symbols for each stripe until every peer has acknowledged
  that stripe's independent DoF bucket. Zero-sized stripes require no acknowledgment.
- Pooled receivers have one ideal DoF bucket, rank `min(K, distinct innovative deliveries)`.
  Striped receivers have one such bucket per stripe. Decoder/sink acceptance occurs only after the
  explicit serial service center completes.

### Control transport and production pins

Each peer has one logical control scope, represented by separate days TCP flows for its two sequence
directions. Those flows have their own Reno state and are separate from every data-hop connection.
Frame identity is carried by a simulator-owned descriptor stream, but all modeled bytes traverse
TCP: the v10 lossless payload size, a four-byte stream length prefix, and the 40-byte TCP/IP segment
charge. Empty `BlockAck`, `AckProbe`, `SessionComplete`, `SourceDone`, positive `Need`, and
`StripeAck` consume respectively 36, 32, 24, 28, 42, and 28 application-stream bytes including the
prefix.

Observable production semantics are pinned by tests in
`wansim/src/protocol/{ack,carousel,rounds,striped}.rs` and
`wansim/tests/w1_protocol_gates.rs`, including:

- ack-join permutation and duplicate invariance;
- canonical snapshot rejection and lowest-range truncation;
- completion only after every frozen peer's cumulative ack;
- debounce, heartbeat, targeted probe, repeated completion, and dual-clock behavior;
- ACK-at-emission-deadline wins and no post-completion submission;
- separate per-peer control TCP scope and shared runtime mailbox before control/data splitting;
- byte-identical repeated runs and registration-order-invariant causal outcomes.

### Deliberate representations and omissions

These are deliberate W1 scope choices, not production claims:

1. A run contains one already-negotiated session. `session_id`, Manifest/Ready exchange, allocator
   randomness, and the post-actor runtime replay cache are implicit rather than reimplemented.
   The peer set is frozen at time zero.
2. Experiment 0 is a one-block ideal-code object. The full Blocks ack/range machinery is unit-tested,
   but the experiment emits only the empty or complete one-block snapshot.
3. Control identities use a descriptor side channel instead of a wire parser. Byte length,
   fragmentation, congestion, buffering, retransmission, and RTT are modeled by TCP; malformed-wire
   parsing is not.
4. A bidirectional TCP control scope is represented as two unidirectional days flows. This matches
   TCP's independent sequence spaces and directional congestion behavior but is not a socket API
   simulation.
5. Sink service always succeeds in W1. Section-P P8 timing is represented; sink-error behavior is
   production-conformance territory and is not re-tested here.
6. Experiment 0 has no control or data loss and no shared physical resource between its two trees.
   Loss, cross-tree physical coupling, and background traffic belong to later stages.

## Phase 2 — experiment 0

### Configuration

- K = 64 ideal DoF; 508 innovative bytes + four-byte logical prefix per data frame; MSS 512.
- Two copies of the W0b tree: sender → relayA → {receiver1, relayB}, relayB →
  {receiver2, receiver3}. Every overlay hop is an independent long-lived TCP connection.
- Sequential controller-order child admission is the default.
- Data propagation is 1 ms per hop. Control one-way propagation is 2 ms for receiver 1 and 3 ms
  for receivers 2/3. Control rate is 100 Mbit/s.
- Homogeneous profile: every data hop is 80 Mbit/s. Crossed profile: receiver 1's tree-0 path is
  fast and tree-1 child path slow; receivers 2/3 see the opposite. Fast is 80 Mbit/s and slow is
  20 Mbit/s.
- Socket send/receive buffers are 2,048 bytes; relay application and per-child queues are 8,192
  bytes; physical link queues are 65,536 bytes. Runtime-command and data inboxes are provisioned
  above the experiment flight.
- Receiver counts are {1, 3}; protocols are the five requested variants; profiles are
  {homogeneous, crossed}; seeds are 0–31: 2 × 2 × 5 × 32 = 640 trials.

The no-drop model has no stochastic decisions that affect timing, so all 32 seeds in a cell produce
the same integer result and mean equals P95. The repetitions still exercise scenario/PRF identity
and artifact determinism; they are not independent statistical samples.

### Required stop and diagnosis

The first screen was homogeneous pooled rounds, three receivers, seed 0. It stopped before the main
sweep:

| Receiver | Rank when round-0 `SourceDone` dispatched | Cached deficit | Last initial-K service | Control overtake |
| --- | ---: | ---: | ---: | ---: |
| 1 | 60 | 4 | 18.669209 ms | 124.960 µs |
| 2 | 60 | 4 | 19.724409 ms | 180.160 µs |
| 3 | 60 | 4 | 19.724409 ms | 180.160 µs |

Both drop counters were zero. The source had handed K frames to two data sockets, but that is not a
cross-connection departure or delivery barrier. Direct control took fewer hops than outstanding
data, so the `Need` snapshot was correct at its observation time and remained cached as rounds
requires. This is a **real modeled scope-ordering mechanism**, not a simulator conservation bug.
`screen-verdict.csv` records that diagnosis; the full run requires the explicit
`--accept-source-done-overtake` flag.

### Completion and emission results

All times below are barrier mean/P95; because every deterministic seed agrees, the two are equal.

| Profile | Receivers | Equal split | Rate-proportional | Per-stripe FEC |
| --- | ---: | ---: | ---: | ---: |
| homogeneous | 1 | 18.669209 ms / 64 | 18.669209 ms / 64 | 18.669209 ms / 80 |
| homogeneous | 3 | 19.724409 ms / 64 | 19.724409 ms / 64 | 19.724409 ms / 88 |
| crossed | 1 | 20.411202 ms / 64 | 28.900014 ms / 64 | 28.900014 ms / 81 |
| crossed | 3 | 21.632002 ms / 64 | 21.632002 ms / 64 | 21.632002 ms / 90 |

Cells show `completion / total source emissions`. Rate-proportional ownership uses static capacity
weights. At this small K it is worse for the one-receiver crossed cell: concentrating 51 frames on
one hop-wise TCP path loses startup/window parallelism, so nominal link capacity is not the same as
finite-transfer effective service rate.

| Profile | Receivers | Pooled rounds | Maximum transient deficit | Carousel | Carousel minus best striped/rounds |
| --- | ---: | ---: | ---: | ---: | ---: |
| homogeneous | 1 | 18.669209 ms / 68 | 4 | 18.669209 ms / 84 (tail 20) | 0 |
| homogeneous | 3 | 19.724409 ms / 68 | 4 | 19.724409 ms / 92 (tail 28) | 0 |
| crossed | 1 | 20.411202 ms / 69 | 5 | 18.769609 ms / 84 (tail 20) | −1.641593 ms |
| crossed | 3 | 21.632002 ms / 70 | 6 | 19.836802 ms / 92 (tail 28) | −1.795200 ms |

The crossed reductions are 8.043% and 8.299% of the best striped barrier (8.746% and 9.050%
speedup ratios). Carousel split emissions evenly between trees. Of its 20/28 excess emissions,
8/16 were emitted after the receiver barrier, and none were emitted after sender completion. The
remainder was already ACK-flight work that could arrive on a fast path before slow members of the
first K emission set.

### Predeclared prediction verdicts

| Prediction | Verdict | Evidence |
| --- | --- | --- |
| E0-a: no repair deficits | **FAIL** | 128/128 rounds trials had a positive report; 256 reports totaled 1,216 missing DoF, maximum 6, with zero drops. |
| E0-b: rounds and carousel local completion within one control-latency delta | **PASS** | 0/256 receiver comparisons exceeded 6.1 ms; maximum difference was 1.795200 ms. Homogeneous difference was exactly zero. |
| E0-c: carousel extra transmission is only ACK-flight tail | **PASS, qualified** | Total was exactly K + 20 or K + 28; no send followed sender completion. The 31.25–43.75% tail is large at K=64 and must not be called negligible. |
| E0-d: pooling beats striped ownership under unequal service | **PASS** | Fastest pooled beat fastest striped in all 64 crossed profile/receiver/seed pairs; margin 1.641593–1.795200 ms. |

The causal reading is narrower than “carousel always wins.” Under homogeneous paths every protocol
had the same local completion, exactly as expected. Under crossed paths, continuous pooled symbols
can replace slow in-flight members of the first K at the receiver. Rounds cannot exploit that until
its separately routed barrier/feedback cycle. This is a WAN-pipeline mechanism visible even with
reliable no-drop transport; its magnitude at production K is not established by W1.

## Validation suite

| Gate | Result |
| --- | --- |
| Tandem integer serialization | A 552-byte TCP/IP segment at 10 Mbit/s serialized in exactly 441,600 ns on each hop. Source emission to receiver delivery was exactly `2×441,600 + 2×1,000,000 = 2,883,200 ns`; relay forwarding began only after full-frame arrival. |
| Zero-extra vs effectively infinite buffers | With K=16 finite striping, 8 Gbit/s links, and near-zero delay/service, one-frame buffers completed in 5,292 ns versus 4,977 ns for 1 MiB buffers; both emitted exactly 16, had zero drops, and differed by 315 ns, below the declared 8,832 ns opportunity bound. |
| Edge-disjoint non-coupling | With quota `[0, 8]`, changing every unused tree-0 link from 80 Mbit/s to 1 bit/s left receiver completion, sender completion, and `[0, 8]` emissions exactly unchanged. |
| Fork/join conservation | Every active leaf completed; barrier equaled the maximum leaf completion; both drop counters were zero. |
| Registration-order metamorphism | Reversing model/link registration preserved the full causal projection and all outcomes. Existing relay occupancy records can differ in a transient same-time `value`, so this is not misreported as byte identity. |
| Repeat determinism | Two executions with identical registration produced byte-for-byte identical event CSV. Main artifacts reproduce under committed SHA-256 digests. |
| Plumbing capacity | Experiment maximum nexosim mailbox high-water was 12/256; mailboxes were nonbinding. |

## Reproduction

From the repository:

```bash
cd /Users/winifred/nextmini-perfect-fec/wansim
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo run --release --bin w1_experiment0 -- \
  screen ../results/wansim/w1-experiment0

# Inspect screen-verdict.csv before the diagnosed resume.
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo run --release --bin w1_experiment0 -- \
  sweep 32 ../results/wansim/w1-experiment0 \
  --accept-source-done-overtake

cd ../results/wansim/w1-experiment0
shasum -a 256 -c SHA256SUMS
```

Artifacts:

- `screening.csv` and `screen-verdict.csv`: stopping-cell causal diagnosis;
- `trials.csv`: all 640 exact trial rows;
- `summary.csv`: 20 profile/receiver/protocol aggregates;
- `predictions.csv`: machine-readable E0 verdicts;
- `SHA256SUMS`: committed reproduction digests.

## Commits

| Phase | Commit | Content |
| --- | --- | --- |
| Phase 1 | `fd50915` | Independent protocol endpoints, control TCP scopes, W1 topology, conformance gates. |
| Phase 2 | `573f4fa` | Strict-conservation harness, CLI, scenario overrides, closed-form/metamorphic validation. |
| Endpoint hardening | `7509fe3` | Empty quorum, abort stop, strict BlockAck input, and passive-window edge semantics. |
| Evidence | `155e813` | Screening diagnosis, 32-seed grid, summaries, prediction verdicts, and digests. |
| Report | this commit | W1 design notes, falsifier diagnosis, results, validation, and final gates. |

## Gates

| Gate | Result |
| --- | --- |
| `cd wansim && cargo fmt --check` | PASS |
| `cd wansim && cargo clippy --all-targets -- -D warnings` | PASS |
| `cd wansim && cargo nextest run` | PASS — 68 passed, 0 skipped |
| Root `cargo nextest run` spot regression | PASS — 828 passed, 17 existing skipped |

Commands used:

```bash
cd wansim
cargo fmt --check
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo clippy --all-targets -- -D warnings
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run

cd ..
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run
```

No W2 implementation is included.
