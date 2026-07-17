# Stage 3.0/3.1 review and gate ruling (Claude)

Date: 2026-07-16
Scope: commits `fe3e1d5..757915f` against plan v2 Stage 3 (3.0/3.1 and the Gate 3 criteria).
Verdict: **Stage 3.0/3.1 work APPROVED. The research NO-GO for Stage 3.2 is ACCEPTED and is now
the recorded gate ruling: reservoir integration does not proceed on this branch.**

## Verification performed

- Independent gate re-run on `757915f`: fmt clean; `cargo nextest run` → 821 passed, 0 failed,
  17 pre-existing skips. Worktree clean.
- Reproducibility spot-check: re-ran `reservoir_storage_spike retain 65536 1400 2 100 5 100` from
  the committed binary — payload digest `ba1631c0cde1e383`, reserve payload 4,587,800 B, and
  3,277 reserve bins all match the report exactly.
- Methodology review of `plans/stage3-sim-report.md` and the `results/stage3/` artifacts.

## Why the NO-GO is credible (not a harness artifact)

- The mechanism explains the numbers: reserve bins are PRF-selected uniformly over eligible
  positions and fixed before transmission. Memoryless (BEC) loss is well-matched (99.73% at 2%
  erasure) but correlated bursts erase contiguous bin runs a fixed non-adaptive reserve cannot
  cover. The internal pattern is diagnostic: GE-long has HIGHER initial completion than BEC 1%
  (45.1% vs 24.0% — long good runs leave many trials untouched) yet far worse final completion
  (64.9% vs 99.98%) — exactly the signature of a construction-level mismatch with burst loss,
  not a simulator bug.
- The simulator is cross-validated against the real terminated encoder/rolling decoder on the same
  graph and delivered-bin set, uses exact finite counts from the corrected 2.6 solver, exact
  two-sided Clopper–Pearson intervals (pinned by tests), and 4,096-trial confirmation at the best
  grid point. The sweep is monotone in both rate knobs, and the best point is the most expensive
  one — there is no cheaper rescue point hiding in the grid.
- The plan's own requirement ("BEC p ∈ {0.1–2%} AND GE/bursty traces") is what fails; the four
  mechanical Gate 3 criteria all pass. This is the research gate functioning as designed: we did
  not spend a wire-contract change on a construction that cannot beat its fallback under bursts.

## Ruling and consequences

1. Stage 3.2 is NOT implemented. No manifest/wire fields (`c_total`, `c_wire`, reserve cardinality,
   PRF version, seed derivation) are added. Current wire and runtime behavior stands.
2. The prototype, harnesses, and results stay committed as research artifacts, all explicitly
   labeled BEYOND the METTLE paper. If the idea is revisited, the report's conditions bind:
   an explicit burst-channel completion target first, a burst-aware or adaptive design tested in
   simulation, an independent process-wide sender memory budget, and separate accounting for
   post-emission reserve retransmissions and multi-peer duplicates.
3. Per the plan's stage order (Stage 4 already extracted to `plans/rateless-mettle-vnext.md`), the
   branch proceeds to Stage 5 (docs + conformance polish).
4. Housekeeping for Stage 5: the Gate 3 manual decoder spike re-measurements are recorded
   (9.991 ms / 116.109 MiB — within limits); the two standing deferrals in
   `plans/perfect-fec-runtime-questions.md` (sender-cache pool, manual perf thresholds) remain open
   and must be reflected in the invariants doc's measured-vs-guaranteed table.
