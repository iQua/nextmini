# Stage 2 review (Claude)

Date: 2026-07-16
Scope: commits `4ed8da4..34a2674` (six sub-stage commits + report) against plan v2 Stage 2, §P, and
the binding Stage 2.0 spike constraints (`plans/stage2-spike-review-claude.md`).
Verdict: **APPROVED with required fixes before Stage 3.** The object-stream design, C5 repair-epoch
machinery, admission contract, and mode-matrix freeze are implemented correctly on the honest path
and well-anchored in tests; four findings (one peer-triggerable, three wire-contract/test-integrity)
must be fixed before Stage 3 builds on this layer.

## Verification performed

- Independent Gate 2 re-run on `34a2674`: fmt clean; workspace clippy `-D warnings` clean;
  `cargo nextest run` → 790 passed, 0 failed, 17 skips (composition re-verified). Worktree clean.
- Four independent reviews (2.1 plan/admission; 2.2 wire contract — with an empirical probe crate;
  2.3/2.4 runtime and repair epochs; Gate 2 coverage audit), all with file:line evidence.
- `fec_round_regressions.rs` has zero diff in the Stage 2 range; `fec_mettle_session.rs` diff is
  exactly one added `mettle_decoder_budget: None` field — the frozen rounds suite is intact (3/3).
- 2.6 statistics checked by hand: the zero-failure bound formula is the exact one-sided 95%
  Clopper–Pearson bound, 1 − 0.05^(1/4096) = 0.000731113 < 10⁻³ ✓; the 5.5% Table-IV row asserts
  exactly 105,500 at K = 100,000 ✓.

## Requirements verified met (summary)

- **2.1 / spike constraints**: dedicated `ObjectSymbolPlan` with reversible mapping and
  final-source-only padding (exhaustive roundtrip test across a 185-prefix geometry); both caps
  (≤65,536 sources, checked `N·T` ≤ 96 MiB) enforced at admission on BOTH ends with recomputed
  expected values, so sender/receiver geometry cannot diverge; ALL O(N) dense allocations are
  fallible (`try_reserve_exact` verified across decoder.rs; no infallible N-scaled allocation left
  on the carousel path); construction on `spawn_blocking` strictly before Ready; failure ⇒ no Ready,
  RAII permit release on every exit path, session Aborted, no partial install; 192 MiB × 4 knobs
  configurable with validated defaults and a checked aggregate; the fifth-decoder rejection test
  builds four REAL dense decoders through the production `install` path.
- **2.2**: MettleStream ack on the reserved discriminant with byte-pinned layout; watermark/stream
  bounds validated against negotiated geometry; version 7→10 with the layout table current and
  non-10 frames rejected; mode matrix enforced at the wire in both directions
  (`MettleObjectStreamGeometryRequired` / `...Unexpected`); hostile manifests cannot skip dataplane
  admission (caps re-validated in `manifest.validate()`).
- **2.3**: sender uses one terminated encoder per prefix, strict bin-id departure order, zero legacy
  per-block state (`finite_block_count == 0` asserted); receiver commits via `ObjectSymbolPlan`
  through the fallible sink path and advances its advertised watermark only after the write succeeds
  (P8); old graph dropped before successor construction under the same session permit.
- **2.4 / C5**: checkpoint-after-payload ordering is airtight (checkpoint reachable only with no
  pending symbol and an empty repair queue; `initial_departure_complete` flips only after the final
  bin is queued); checkpoint retransmits on probe cadence and re-delivery cannot reset receiver
  aging; gaps age against checkpoint + configured reorder budget with NO frontier-gap inference
  anywhere; reports epoch-tagged, deduped per (peer, epoch), quorum union re-emitted once; full
  replay after exactly 3 fully-reported no-progress epochs with correct counter resets. Zero-loss
  extreme reorder ⇒ empty missing set (test), near/far targeted recovery end-to-end with byte-exact
  sink (test), duplicates measured not asserted-less (test).
- **2.5**: dormant `repair_deficit` branch deleted (it silently rebuilt metadata with zero overhead —
  good riddance).
- **2.6**: binary solver for interior c with exact terminal counts asserted for all Table-IV rows;
  tail-floor unattainable path; the 8,192..65,536 sweep proves one prefix pays one termination tail
  and the K=256 regime cannot even reach the 5.5% target.
- **Stage 1 interplay**: P7 clocks fed by monotone MettleStream joins (stale acks cannot regress);
  completion requires every frozen peer's final watermark; SessionComplete/passive/replay-cache
  machinery reused; RaptorQ carousel byte-identical to before; empty quorum still trivial success.

## Required fixes (Codex, before Stage 3)

1. **MAJOR — unbounded `seen_bin_ids` from out-of-range peer bin ids**
   (`receiver/mettle_carousel.rs:237`; found independently by two reviews). The object-stream ingest
   path inserts wire `symbol_id` into a `BTreeSet<u32>` without bounding it by `terminal_bin_count`
   (`validate_block_symbol` bounds only `block_id`, and the decoder silently ignores out-of-range
   bins). A hostile or corrupt quorum peer sending distinct `symbol_id ≥ terminal_bin_count` grows
   receiver memory toward 2^32 entries — defeating the 192 MiB admission budget — and skews the
   `symbols_at_decode` histogram. The legacy path drops out-of-range ESIs before storage; restore
   that discipline here: validate the bin id against `terminal_bin_count` before insert, count it
   as invalid traffic, add the mirror of the legacy "out-of-range peer ESIs dropped before storage"
   test.
2. **MAJOR — MettleStream decode accepts a non-canonical body and silently drops evidence**
   (`messages/control_frames.rs:535-548`, empirically confirmed): `flags = 0` with
   `range_count > 0` parses the ranges then discards them via `has_stall_evidence.then_some(...)`,
   so two byte strings map to one value and re-encode ≠ wire bytes. Reject
   `!has_stall_evidence && range_count != 0` (mirror the Blocks variant's strictness) and pin with a
   test.
3. **MAJOR — P4 deterministic truncation is dead code for MettleStream, and the receiver's clamp is
   off-spec** (`messages/validation.rs:274-289`; `receiver/mettle_carousel.rs:356-365`).
   `for_wire`'s MettleStream truncate branch is unreachable because `canonicalized` errors with
   `TooManyMettleMissingBinRanges` first; meanwhile the receiver bypasses `for_wire` with its own
   clamp that collapses overflow into ONE covering range, over-requesting potentially the whole
   prefix. Fix both to P4's rule: keep the watermark and the LOWEST-bin-id ranges that fit the wire
   cap; make the receiver use the shared `for_wire` path; test convergence under overflow.
4. **MAJOR (test integrity) — missing negative-path coverage for the new wire frames, and a report
   overclaim.** No totality/malformed-body/truncation sweep exists for BlockAck variant 2 or
   DepartureCheckpoint (the seven new error variants are asserted nowhere), and
   `stage2-report.md:71`'s claimed "checkpoint loss" test does not exist — the checkpoint
   emission-ordering invariant (queued only after all epoch payload) and probe-cadence
   retransmission are enforced only structurally. Add: the totality sweeps mirroring the
   Need/Blocks tests; tests for the seven unreferenced error variants incl. mode-mismatch cases;
   a sender test that a checkpoint never overtakes pending epoch payload across an
   AllWouldBlock/backpressure retry; a checkpoint-loss end-to-end test (drop the first checkpoint,
   recover via cadence retransmission); and correct the report wording.

## Recommended (same pass if cheap, else record for Stage 3/5)

5. Zero-byte object over Carousel+METTLE aborts by stall timeout instead of trivially completing
   (`mettle_carousel.rs:126-128` returns no ack; sender requires per-peer ack entries). Make empty
   objects complete (ack an empty-object completion or trivially finish sender-side) + test.
6. Admission charge ignores the coded rate (`mettle_carousel.rs:92-98` charges 2·N·T; worst-case
   stall retention ≈ (1+rate)·N·T + graph): make the estimate rate-aware or bound the configured
   rate at validation.
7. Sender-side `bin_cache` retains a full encoded prefix (~(1+c)·96 MiB) per session with no
   process-wide cap — the same retention class the receiver budget bounds. Add a sender-side budget
   or document the operator-facing limit.
8. Budget-config errors degrade to warn + reject-all at runtime (`runtime.rs:487-493`): fail config
   validation loudly instead of silently disabling the paper-native path; the sender should also
   not start sessions its own receivers would reject.
9. Construction-latency/RSS thresholds are manual-evidence only (spike example asserts nothing).
   Either add a generously-bounded harness assertion or explicitly record them as
   re-measure-at-every-gate manual evidence; re-run required at Gate 3.
10. Hot-path cost: `block_ack()` is computed twice per inbound frame and an aged checkpoint walks
    `0..departure_bin_exclusive` each time (`receiver/mod.rs:275,285`; `mettle_carousel.rs:335-354`).
    Memoize the aged-gap computation.
11. Nits: pub `MettleObjectStreamGeometry::validate` div-by-zero on `source_symbol_bytes = 0`
    (validation.rs:49); unreachable `mettle_manifest_missing_geometry` branch vs silent-ignore
    asymmetry (receiver/mod.rs:783-786); `MettleObjectSymbolStream` conflates internal failure with
    clean exhaustion (sender/fec.rs:214-254); permit does not cover an orphaned `spawn_blocking`
    graph build if the receiver is dropped mid-construction; sender repair metrics
    (`mettle_targeted_retransmissions`, `mettle_full_replay_symbols`) recorded but never asserted.

## Cross-check notes

- The `seen_bin_ids` MAJOR was found independently by the admission review and the runtime review —
  treated as confirmed without a separate verify pass.
- "3/3 frozen rounds" = the two untouched `fec_round_regressions` tests plus `fec_mettle_session`
  whose only Stage 2 edit is the new config field set to `None`; verified via targeted git diff.
