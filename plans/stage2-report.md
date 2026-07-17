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
  cover near/far loss recovery using only targeted ranges, duplicate measurement, packet-capture
  loss of the first checkpoint followed by probe-cadence retransmission, report
  deduplication/union, and the three-epoch fallback.

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

Post-review-fix release-harness measurements at `N=65,536`, `T=1,400`, and `c=1/20` were:

| Scenario | Construction | Peak RSS | Peak logical payload upper bound | Max synchronous push |
| --- | ---: | ---: | ---: | ---: |
| Construct only | 8.281 ms | 13.859 MiB | 0 MiB | 0 ms |
| Terminal-source bin first | 7.925 ms | 13.828 MiB | 0.342 MiB | 0.0094 ms |
| Permanent leading stall | 7.728 ms | 116.734 MiB | 87.839 MiB | 0.1707 ms |

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
- Post-review Gate 2 `cargo nextest run`: 808 passed, 0 failed, 17 skipped across 33 binaries.
- Focused `fec_round_regressions` plus `fec_mettle_session`: 3 passed, 0 failed, 0 skipped.

The 17 full-gate skips are intentional ignored tests: 14 manual METTLE benchmark/reproduction
reports, one runtime integration case, and one red-test scaffold compiled in both dataplane library
and binary targets. No selected controller test was skipped for lack of PostgreSQL, so no question
entry was added.

## Open questions

The sender-cache admission limit and automated performance-threshold policy are recorded in
`plans/perfect-fec-runtime-questions.md`. No Stage 3 work was started.

## Review follow-up fixes

Date: 2026-07-16

Scope: all four required findings, recommendations 5 through 10, and the item-11 nits from
`plans/stage2-review-claude.md`. Recommendations 7 and 9 are explicitly documented policies or
deferrals rather than silent omissions. Stage 3 was not started.

### Per-item resolutions

1. Peer bin ids are now checked against the current stream's `terminal_bin_count` before insertion
   into `seen_bin_ids`. Out-of-range traffic increments `receiver_invalid_symbols` and never affects
   retained state or decode histograms. The test mirrors the legacy out-of-range-ESI drop test.
   Commit: `6f92289`.
2. MettleStream BlockAck decoding now rejects the non-canonical `has_stall_evidence = false,
   range_count != 0` body instead of silently discarding ranges. Commit: `6f92289`.
3. MettleStream `for_wire` now validates the complete canonical range set, retains the lowest 255
   ranges, and is the receiver's sole wire clamp. The overflow test demonstrates convergence
   without collapsing gaps into one over-broad range. Commit: `cc98c22`.
4. BlockAck variant 2 and DepartureCheckpoint now have body-length totality, malformed-body,
   truncation, and count/flag sweeps; all seven previously unreferenced validation errors have exact
   negative tests, including mode mismatches. A sender backpressure test proves checkpoints cannot
   overtake epoch payload across `AllWouldBlock`, and a packet-capture integration test drops the
   first checkpoint and recovers through its cadence retransmission. The earlier report overclaim
   was replaced above with this precise evidence. Commits: `8ca0758`, `aafcada`.
5. The existing sender-side zero-stream completion path is now pinned by a Carousel+METTLE network
   test: a zero-byte object completes after quorum freeze without payload or BlockAck. Commit:
   `54f455b`.
6. Decoder admission now charges a checked `N * T + ceil(coded_rate * N * T)` logical payload
   estimate; tests distinguish the baseline and a higher coded rate under the same budget. Commit:
   `54f455b`.
7. A process-wide sender-cache permit is deferred because it needs a new ownership/configuration
   surface shared by concurrent sender tasks. Until that lands, operators must budget each active
   Carousel+METTLE sender for `terminal_bin_count(N, coded_rate) * T` encoded payload plus container
   overhead and cap concurrent senders accordingly. The design decision is tracked in
   `plans/perfect-fec-runtime-questions.md`.
8. Invalid decoder-budget configuration now fails construction through `try_new`; the infallible
   runtime constructor fails loudly instead of installing a reject-all runtime. Commit: `54f455b`.
9. Release RSS and construction checks remain host-sensitive manual gate evidence. The exact
   commands were rerun for this gate and the measurements above remain below 192 MiB and 25 ms.
   Automatic thresholding is deferred with rationale in `plans/perfect-fec-runtime-questions.md`;
   the same release measurements are mandatory at Gate 3.
10. Aged checkpoint gap computation is memoized and invalidated only when a newly seen bin changes
    the result, avoiding duplicate full-prefix walks from repeated BlockAck generation. Commit:
    `cc98c22`.
11. The cheap nits were resolved: zero-byte geometry validation no longer divides by zero;
    impossible post-validation geometry absence is a loud invariant; symbol-stream exhaustion is
    distinct from internal failure; decoder permits remain owned by orphaned blocking construction;
    and sender repair metrics are asserted. Commits: `8ca0758`, `54f455b`.

### Follow-up commits

| Concern | Commit | Message |
| --- | --- | --- |
| Peer input and canonical bodies | `6f92289` | `Harden METTLE peer input validation.` |
| Shared range truncation and gap memoization | `cc98c22` | `Apply canonical METTLE range truncation.` |
| Negative wire and validation coverage | `8ca0758` | `Strengthen METTLE wire validation coverage.` |
| Payload/checkpoint ordering and loss recovery | `aafcada` | `Prove METTLE checkpoint recovery ordering.` |
| Admission, empty objects, runtime failure, and lifecycle nits | `54f455b` | `Harden METTLE admission and lifecycle handling.` |

### Final Gate 2 evidence

Every command used `CARGO_INCREMENTAL=0` and
`PYO3_PYTHON=/opt/homebrew/bin/python3.13`.

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo nextest run`: 808 passed, 0 failed, 17 skipped across 33 binaries.
- Focused frozen `fec_round_regressions` and `fec_mettle_session`: 3 passed, 0 failed, 0 skipped.
- `git diff 34a2674..HEAD` for both frozen fixtures: empty.
- Release dense construction at the maximum prefix: 7.728-8.281 ms across the three scenarios,
  below 25 ms.
- Worst measured RSS: 116.734 MiB, below the 192 MiB per-decoder reservation.

The 17 skips remain the intentional ignored benchmark/reproduction and integration cases; no
selected controller test was skipped for unavailable PostgreSQL.
