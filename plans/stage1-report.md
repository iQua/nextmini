# Stage 1 completion report

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Scope: Stage 1 only; Stage 2 was not started.

## What changed

### 1.0 Protocol plumbing

- Added `FecFeedbackMode::{Rounds, Carousel}` to configuration and FEC manifests while preserving
  `Rounds` as the default.
- Bumped the lossless wire protocol from version 7 to version 8 and documented the version-8 layout.
- Added validated carousel timing configuration and rejected `Carousel + METTLE` until Stage 2.
- Replaced deterministic flow-derived transfer IDs with a process-lifetime allocator backed by the
  OS-seeded cryptographic thread RNG. It rejects zero, retains every issued ID, and has a transfer
  reuse test.
- Threaded feedback mode and session identity through controller, message, policy, and dataplane
  construction paths without changing rounds-mode behavior.

### 1.1 Carousel control frames

- Added versioned `BlockAck`, targeted `AckProbe`, and `SessionComplete` control frames.
- Implemented canonical half-open completion ranges, watermark folding, sorted/disjoint validation,
  manifest bounds, deterministic lowest-range truncation, and the reserved METTLE stream-progress
  discriminant.
- Added encode/decode/validation coverage for malformed bodies, reserved variants, range overflow,
  canonicalization, and carousel-versus-rounds mode rejection.

### 1.2 Receiver acknowledgement state machine

- Implemented `Active -> PassiveComplete -> Finished` for carousel receivers.
- Added debounced cumulative acknowledgements, periodic heartbeats, targeted probe replies, eager FEC
  decode, and the rule that local completion is advertised only after sink acceptance.
- Added the passive replay window and runtime replay cache for final acknowledgements, including safe
  live-task-to-cache handoff and target-peer filtering.
- Validated that the passive window exceeds the sender abort budget plus its configured safety margin.

### 1.3 Carousel sender scheduling

- Implemented the RaptorQ carousel state machine: exact `0..K` source prefixes in block order,
  followed by one fresh repair ESI per globally incomplete block in round-robin order.
- Refactored data submission to one non-blocking tree sweep returning `Queued`, `AllWouldBlock`, or
  `AllClosed`; the outer loop services control, liveness, pacing, and backpressure boundaries.
- Added cumulative per-peer acknowledgement joins and stopped scheduling a block as soon as every
  frozen peer's joined state marks it complete.
- Rechecked completion after pacing and immediately before submission, emitted targeted probes only
  for missing peers, and repeated best-effort `SessionComplete` frames.
- Applied both Stage 0 review findings: ESI exhaustion aborts only when another emission is actually
  required, after a final completion check; `patch_tree_id` now uses checked offset arithmetic.

### 1.4 Per-peer liveness

- Added independent `last_ack_seen` and `last_ack_progress` clocks for every frozen active peer.
- Duplicate/reordered acknowledgements refresh only the silence clock; strict acknowledgement joins
  refresh progress. Silent and live-but-stalled peers abort the whole session independently.
- Started clocks at quorum freeze, excluded already-complete peers from later timer dependence, and
  retained immediate trivial success for an empty frozen quorum.
- Added silent, stalled, long-healthy, heterogeneous-progress, and empty-quorum coverage.

### 1.5 Metrics and deterministic observers

- Added a shared per-session `SessionMetrics` observer and caller-owned sender/receiver test hooks.
- Recorded `queued_after_final_ack_processed`, backpressure sweeps, enumerated wait-state counts and
  durations, checked per-block ESI sequences, receiver completion-tail symbols, safely deduplicated
  receiver symbols, and the `symbols_at_decode - K` histogram.
- Removed the no-op per-symbol logging hooks and made lifecycle counter logs report real sender and
  receiver state.
- Added event-boundary tests showing no post-final-ack queueing, monotone ESIs, duplicate suppression,
  receiver-tail accounting, decode overhead, and completion-repeat waits.

### 1.6 Conformance suite and benchmark evidence

- Added `fec_carousel_conformance.rs`, a deterministic adversarial harness covering acknowledgement
  permutation/duplication, acknowledgement loss with heartbeats, completion handoff recovery,
  incarnation safety, all-tree backpressure, pacing races, quorum safety, paused-time timer fairness,
  range scaling, eager decode, tree-label permutation, and metric semantics.
- Added a private-path conformance case that fills one tree lane, proves fallback queues fresh ESIs on
  another tree, then delivers final and stale acknowledgements out of order without reopening work.
- Fixed runtime dispatch to reject frames whose wire `session_id` differs from the runtime routing key;
  stale transfer incarnations can no longer install state in a reused session slot.
- Recorded the matched-seed rounds-versus-carousel trace comparison in
  `plans/stage1-rounds-carousel-benchmark-evidence.md`, including methodology, limitations, and
  interpretation tolerances. It remains evidence rather than a CI dominance assertion.

## Commits

| Sub-stage | Commit | Message |
| --- | --- | --- |
| 1.0 | `e4d2971` | `Add carousel protocol plumbing.` |
| 1.1 | `bbcfb1b` | `Define carousel control frames.` |
| 1.2 | `2cbb510` | `Implement carousel receiver acknowledgements.` |
| 1.3 | `9b54b14` | `Implement carousel sender scheduling.` |
| 1.4 | `73059ab` | `Enforce carousel peer liveness.` |
| 1.5 | `f740c2d` | `Expose carousel session metrics.` |
| 1.6 | `e371197` | `Add carousel conformance coverage.` |

## Test results

All cargo commands used `CARGO_INCREMENTAL=0` and
`PYO3_PYTHON=/opt/homebrew/bin/python3.13`.

- Every sub-stage passed `cargo fmt`, clippy with `-D warnings`, and nextest for its touched crates
  before commit.
- Focused Stage 1.6 conformance binary: 20 passed, 0 failed, 0 skipped.
- Focused private tree-fallback/freshness case: passed in both the library and binary test targets.
- `fec_round_regressions`: 2 passed, 0 failed, 0 skipped; the regression file was not modified.
- Gate 1 `cargo fmt --check`: passed.
- Gate 1 `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- Gate 1 `cargo nextest run`: 737 passed, 0 failed, 17 skipped.

The 17 skips are pre-existing ignored tests: 14 manual METTLE benchmark/reproduction reports, one
duplicate-coverage runtime test, and one rounds-mode red-test scaffold compiled in both the dataplane
library and binary targets. No controller test was skipped for lack of PostgreSQL, so no PostgreSQL
entry was added to `plans/perfect-fec-runtime-questions.md`.

## Open questions

None for Stage 1.

## Review follow-up fixes

The approved Stage 1 review follow-ups were completed before any Stage 2 work. No review item was
skipped or disputed, and `plans/perfect-fec-runtime-questions.md` required no new entry.

### Required fixes

| Item | Resolution | Commit |
| --- | --- | --- |
| 1 — runtime-actor deadlock | Made live-receiver delivery non-blocking, kept a completing receiver draining its inbox until replay installation is acknowledged, and added the full-data-inbox runtime-liveness conformance test. | `167661f` |
| 2 — completed transfer reported aborted | Made joined quorum completion win over control-channel disconnect, including when the completing acknowledgement is consumed inside a wait branch. | `8af234b` |
| 3 — timer starvation under acknowledgement flood | Gave pacing, backpressure, and feedback wait deadlines precedence over continuously ready control input so liveness and probe timers remain observable. | `8af234b` |
| 4 — probe loss during receiver handoff | Made completed replay handling cache-first during handoff and kept the receiver draining controls until the runtime confirms replay installation; probes are answerable across the transition. | `167661f` |
| 5 — unbounded replay cache | Added a retention deadline to Plain, FEC, and Carousel replay entries, insertion-time sweeping, and exact-session expiry checks, with an all-modes eviction test. | `cd78bbe` |
| 6 — pacing double-charge on retry | Retained the generated pending symbol and its pacing-charge state across `AllWouldBlock`, preventing payload regeneration and duplicate token charges. | `8af234b` |
| 7 — carousel timing rejected plain/rounds | Scoped timing validation to negotiated Carousel senders and Carousel receiver manifests; Plain and FEC Rounds startup now has regression coverage with otherwise-invalid carousel timing. | `b445f91` |
| 8 — duplicate flow delivery | Added a shared controller-side flow claim before random session-id allocation, used by startup and database-sync delivery, with a duplicate-builder regression test. | `9f775f5` |

### Test strengthening and wire hardening

| Item | Resolution | Commit |
| --- | --- | --- |
| T1 — incarnation safety | Reused a runtime/session route across successive transfers, injected stale payload and acknowledgement frames, and positively asserted clean successor receiver and sender completion. | `77d915b` |
| T2 — timer fairness | Continuously refilled a full receiver data inbox across both debounce and heartbeat windows and asserted both acknowledgements while production remained active. | `77d915b` |
| T3 — passive expiry | Added a receiver test that drops `SessionComplete` forever and proves termination at passive-window expiry. | `77d915b` |
| T4 — metrics positive path | Directly drove `record_queued_after_final_ack` and asserted the counter increments. | `77d915b` |
| T5 — allocator retention | Added deterministic candidate injection, pre-seeded issued ids, and proved zero and retained candidates are redrawn before accepting a fresh id. | `77d915b` |
| W1 — BlockAck canonical bytes | Canonicalized accepted BlockAck state during encoding, truncated reused output buffers to the canonical frame, asserted exact bytes, and added Plain/Rounds manifest rejection tests. | `bacdd42` |

The two cheap receiver timer nits were also applied in `cd78bbe`: deadline arithmetic is checked, and
an armed acknowledgement timer advances even when no BlockAck can yet be formed. The review's
Carousel+METTLE diagnostic note remains intentionally owned by Stage 2, and no METTLE restriction was
changed in this follow-up.

### Follow-up commits

| Concern | Commit | Message |
| --- | --- | --- |
| Runtime handoff, replay, and probe window | `167661f` | `Prevent receiver handoff deadlocks.` |
| Sender waits, completion outcome, and pending-symbol pacing | `8af234b` | `Correct carousel sender wait outcomes.` |
| Replay retention and receiver timer nits | `cd78bbe` | `Bound completed receiver replay retention.` |
| Carousel-only timing validation | `b445f91` | `Scope carousel timing validation.` |
| Controller flow deduplication | `9f775f5` | `Deduplicate lossless flow delivery.` |
| Canonical BlockAck encoding and mode validation | `bacdd42` | `Canonicalize BlockAck wire encoding.` |
| T1–T5 conformance strengthening | `77d915b` | `Strengthen carousel conformance coverage.` |

### Follow-up verification

All cargo commands used `CARGO_INCREMENTAL=0` and
`PYO3_PYTHON=/opt/homebrew/bin/python3.13`.

- Each concern commit passed formatting, clippy with `-D warnings`, and nextest for its touched
  crates before commit.
- The strengthened carousel conformance binary passed all 21 tests.
- Gate 1 `cargo fmt --check`: passed.
- Gate 1 `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- Gate 1 `cargo nextest run`: 755 passed, 0 failed, 17 pre-existing skips across 32 binaries.
- Focused `fec_round_regressions`: 2 passed, 0 failed, 0 skipped. The regression source file has no
  diff from the pre-follow-up Stage 1 report commit.
