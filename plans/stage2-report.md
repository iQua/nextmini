# Stage 2 completion report

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Scope: approved Stage 2.0 constraints plus Stage 2.1 through 2.6. Stage 3 was not started.

## What changed

### 2.1 Checked object-stream planning and admission

- Added a dedicated `ObjectSymbolPlan` with one global source namespace, checked source-to-object
  spans, reversible global/source-prefix mappings, deterministic `0..stream_count` stream ids, and
  exact final-source/final-prefix lengths. Padding exists only in the final encoded source and is
  excluded from sink spans.
- Added manifest-negotiated `MettleObjectStreamGeometry`: source-symbol bytes, sources per stream,
  stream count, and final-stream source count. Sender and receiver validate the exact same geometry.
- Enforced both approved per-prefix limits with checked arithmetic: at most 65,536 sources and at
  most 96 MiB of source payload (`N * T`). `block_seed(session_id, stream_id)` supplies the graph
  seed for each deterministic prefix.
- Made dense graph construction fallible for all `O(N)` allocations. Carousel receivers acquire a
  decoder permit and construct the dense decoder on `spawn_blocking` before sending `Ready`.
  Geometry, permit, allocation, or worker failure rejects the manifest installation cleanly, sends
  no `Ready`, releases the permit, and exits the receiver as aborted.
- Added configurable decoder admission knobs with validated defaults of 192 MiB per decoder and four
  permits, for a checked 768 MiB aggregate reservation.

### 2.2 METTLE stream-progress wire contract

- Used the reserved BlockAck discriminant for `MettleStream { stream_id,
  decoded_source_watermark, stalled }` and kept block-completion acknowledgements for the other mode
  combinations.
- Added the object-stream geometry to the manifest wire layout and validated source/payload caps,
  exact stream counts, final-prefix shape, per-stream watermarks, canonical missing-bin ranges, and
  scheme/feedback-mode legality.
- Preserved the mode matrix: `Rounds + METTLE` is the finite-block adaptation; only `Carousel +
  METTLE` negotiates object-stream geometry. RaptorQ keeps its existing rounds and carousel paths.

### 2.3 Paper-native carousel object streams

- Added a Carousel-only METTLE sender that pushes global object sources through one terminated
  encoder per negotiated prefix and emits bins in increasing bin-id order. It does not allocate the
  legacy per-block METTLE state.
- Added a Carousel-only METTLE receiver that decodes each prefix, commits decoded sources through
  `ObjectSymbolPlan`, and advances its advertised watermark only after the fallible sink write
  succeeds.
- Decoder lifetime is sequential: the current dense graph is dropped before the successor prefix is
  constructed. The one session permit spans the sequence, so at most one dense decoder is live for
  that session.
- Added exact multi-prefix round-trip coverage, including a partial final source with no padding
  leakage, in-order departure checks, stream transition/seed checks, and a real four-decoder
  admission test whose fifth construction is rejected with `Exhausted`.

### 2.4 Reorder-safe targeted repair epochs

- Bumped the final lossless protocol version to 10 and added `DepartureCheckpoint { stream_id,
  repair_epoch, departure_bin_exclusive }`.
- The sender queues a checkpoint only after every payload frame in that epoch has been accepted by a
  tree channel. It retransmits the latest checkpoint on the probe cadence so checkpoint loss cannot
  permanently suppress feedback.
- Receivers age checkpoint-covered gaps for the configured reorder budget before reporting
  epoch-tagged missing-bin ranges. Extreme reverse-order delivery inside the budget produces an
  empty missing set and zero required retransmissions.
- The sender deduplicates each peer's report per epoch, unions the quorum's ranges, and re-emits the
  sorted union once. The precisely defined no-progress count is the number of fully reported epochs
  in which the quorum-minimum committed watermark does not advance; the validated default of three
  schedules a complete cached-prefix replay and resets the counter.
- Added separate metrics for targeted retransmissions, full replays, and receiver duplicates. Tests
  cover near/far loss recovery using only targeted ranges, duplicate measurement, checkpoint loss,
  report deduplication/union, and the three-epoch fallback.

### 2.5 Dormant repair estimator removal

- Removed the unused dataplane `repair_deficit` helper and its METTLE branch, which had silently
  rebuilt metadata with zero overhead regardless of the negotiated coded rate.
- The live Carousel path uses repair epochs, while the regression-frozen Rounds receiver keeps its
  existing completion-probe behavior.

### 2.6 Exact finite-overhead accounting

- Reinterpreted the paper harness rows as target total transmitted overhead, not interior graph
  expansion. A deterministic binary solver now selects interior `c` such that the exact compressed
  terminal symbol count equals `ceil(K * (1 + target_total_overhead))`.
- Reports now include target total overhead, solved interior `c`, terminal symbol count, and actual
  total overhead (`terminal_symbol_count / K - 1`). Targets below the finite-tail floor are reported
  as unattainable instead of being mislabeled.
- Non-ignored assertions cover every Table-IV target at `K=100,000`; the 5.5% row is exactly 105,500
  transmitted symbols. A source-count sweep from 8,192 through 65,536 proves one object prefix pays
  one termination tail instead of repeatedly paying the old `K=256` tail.
- The manual Table-IV harness now defaults to 4,096 trials. With zero observed failures, its exact
  one-sided 95% upper bound is 0.000731113, below the stated `10^-3` target; the report prints this
  bound rather than inferring a stronger claim from too few trials.

## Commits

| Sub-stage | Commit | Message |
| --- | --- | --- |
| 2.1 | `4ed8da4` | `Add checked METTLE object-stream admission.` |
| 2.2 | `3b4026a` | `Negotiate METTLE stream progress on the wire.` |
| 2.3 | `538be07` | `Implement METTLE object-stream carousel.` |
| 2.4 | `abe0660` | `Add reorder-safe METTLE repair epochs.` |
| 2.5 | `2b6a00a` | `Remove the dormant METTLE repair estimator.` |
| 2.6 | `0d8c240` | `Correct METTLE finite-overhead accounting.` |

The approved Stage 2.0 measurement artifacts remain in `779d199` and `0680b86`; the binding review
constraints were committed in `a6537cd` before Stage 2.1 began.

## Gate 2 acceptance evidence

### Memory, construction, and concurrency

The explicit Gate 2 limits are a 25 ms dense-construction threshold at the maximum prefix, a 192
MiB peak decoder-state reservation, one live decoder per receiver session, and four concurrent
process-wide decoder permits.

Current-HEAD release-harness measurements at `N=65,536`, `T=1,400`, and `c=1/20` were:

| Scenario | Construction | Peak RSS | Peak logical payload upper bound | Max synchronous push |
| --- | ---: | ---: | ---: | ---: |
| Construct only | 9.150 ms | 13.906 MiB | 0 MiB | 0 ms |
| Terminal-source bin first | 9.711 ms | 14.016 MiB | 0.342 MiB | 0.0086 ms |
| Permanent leading stall | 10.576 ms | 117.016 MiB | 87.839 MiB | 0.2089 ms |

All rows are below the 25 ms construction and 192 MiB reservation thresholds. Unit and real-decoder
tests admit four concurrent receivers and reject the fifth cleanly; `4 * 192 MiB = 768 MiB` is
checked for overflow. Multi-prefix receiver coverage verifies that the old graph is dropped before
the successor is built.

Exact measurement commands:

```sh
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo build --release -p mettle --example decoder_layout_spike
target/release/examples/decoder_layout_spike dense construct 65536 1400
target/release/examples/decoder_layout_spike dense terminal-jump 65536 1400
target/release/examples/decoder_layout_spike dense prefix-stall 65536 1400
```

### Functional gate

- Mode matrix: sender policy, receiver support, manifest validation, and the rounds integration
  fixture prove that Rounds remains the finite-block METTLE adaptation and Carousel selects the
  object-stream implementation only when its geometry is present.
- Object mapping: exact, empty, partial-final-source, payload-cap, source-cap, multi-prefix boundary,
  reversible mapping, and exact sink reconstruction tests pass.
- Repair: extreme zero-loss reorder, checkpoint aging, near/far targeted recovery, per-epoch dedupe,
  duplicate metrics, and no-progress full replay pass.
- Accounting: all target finite counts, small-prefix tail-floor rejection, the object-prefix
  `K=256` tail comparison, and the 4,096-trial confidence requirement pass.
- Focused frozen rounds binaries: 3 passed, 0 failed, 0 skipped. The two
  `fec_round_regressions` tests are unchanged; the METTLE rounds fixture's only Stage 2 edit was the
  new receiver-config field set to `None`, with its finite-block assertions unchanged.

## Test results

Every Cargo command used `CARGO_INCREMENTAL=0` and
`PYO3_PYTHON=/opt/homebrew/bin/python3.13`.

- Every sub-stage passed `cargo fmt`, clippy with `-D warnings`, and nextest for its touched crates
  before commit.
- Stage 2.3 touched-crate run: 528 passed, 0 failed, 3 skipped.
- Stage 2.4 touched-crate run: 667 passed, 0 failed, 17 skipped.
- Stage 2.5 dataplane run: 534 passed, 0 failed, 3 skipped.
- Stage 2.6 METTLE run: 81 passed, 0 failed, 14 skipped.
- Gate 2 `cargo fmt --check`: passed.
- Gate 2 `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- Gate 2 `cargo nextest run`: 790 passed, 0 failed, 17 skipped across 32 binaries.
- Focused `fec_round_regressions` plus `fec_mettle_session`: 3 passed, 0 failed, 0 skipped.

The 17 full-gate skips are intentional ignored tests: 14 manual METTLE benchmark/reproduction
reports, one runtime integration case, and one red-test scaffold compiled in both dataplane library
and binary targets. No selected controller test was skipped for lack of PostgreSQL, so no question
entry was added.

## Open questions

None for Stage 2. No Stage 3 work was started.
