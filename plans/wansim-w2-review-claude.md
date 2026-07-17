# wansim W2 review (Claude)

Date: 2026-07-17
Scope: commits `9152ef9..85ae5f4` against the W2 definition in `plans/wansim-plan.md` and the
three binding W1 review directions.
Verdict: **APPROVED, including the production NO-GO on isolated credit. W3 may proceed.**

## Verification performed

- Independent wansim gate: fmt/clippy clean, 80/80 tests (the suite itself contains the
  byte-identical-repetition and completion-sum invariants).
- Committed digest manifest verified: `shasum -c SHA256SUMS` 7/7 OK.
- Full 9,216-execution sweep reproduction launched independently from the release binary
  (~14 min); outcome to be appended below. Prior stages' reproductions (W0a, W0b, W1 full sweep)
  were all byte-identical, and the in-suite determinism gates cover the same machinery.
- All three W1 directions honored: K=512 with the tail-fraction measurement (14.13% vs 35.46% at
  K=64 — flight-roughly-constant confirmed); carousel-under-test with sequential default; the
  quantified recommendation delivered with evidence-row citations.

## The NO-GO is the right call, and the reasoning chain deserves recording

1. **Blocking is eliminated from consideration by the data**: up to 387.090 ms of healthy-receiver
   externality with ZERO byte loss (non-monotone in buffer budget — 1 BDP worse than both 0.25 and
   4). This retroactively validates the production hybrid-drop semantics (the try_send deadlock
   fix): hybrid had exactly zero healthy externality in all 72 cells.
2. **Isolated credit is Pareto-non-worse but not decisive**: 32/72 cells improved (up to 27.45%
   barrier, 26.44% emissions), but in the harsh decisive cells it exactly ties hybrid — the slow
   receiver's own decoder service dominates its barrier, and saved slow-branch traffic frees a
   resource nobody else uses while data links are edge-disjoint.
3. **The honesty clincher**: the simulated policy reconstructs payload from frame IDs; a production
   relay would have to retain real bytes (3.63 MiB/branch, 42.16 MiB aggregate in the worst
   screened cell — outside every declared buffer budget) or shed-and-repair, which is
   approximately the hybrid policy again. The report explicitly prices the gap between the
   simulated upper bound and any implementable design instead of hiding it.
4. Reconsideration conditions are concrete and testable (bounded byte-ownership design + W3
   shared-bottleneck benefit), so the NO-GO is falsifiable, not a door slammed.

## Also banked from W2

- Buffer non-monotonicity (1 BDP as the blocking worst case) — direct input for production
  config-default reasoning and a caution against "more buffer is safer" intuition.
- Critical-path mechanism separation: identical 515.7 ms totals decompose into three different
  causal stories (hybrid: late generation; blocking: chain-wide delay; isolated: early generation
  + 242.6 ms branch-credit hold). The attribution machinery works and is honest about the
  source→runtime aggregate not being separable into independent waits yet.
- The healthy externality of blocking attributes to shared upstream/fan-out work
  (212.3 ms of 228.2 ms), not decoder service — the mechanism, not just the magnitude.
- Liveness margin under a 1/10× straggler: 25.8% of the stall budget consumed — no abort
  pressure, but W3's coupled scenarios should keep watching this.

## Directions for W3 (binding)

1. W3's core question inherits W2's condition 2: does saved slow-branch traffic (or any policy
   difference) matter when overlay links SHARE physical bottlenecks? Run the overlap {0, 25, 50,
   100}% axis with aggregate capacity and TCP flow count controlled, reporting the pooling
   advantage separately from the path-diversity advantage (the W1-review flow-count confound rule
   stands).
2. Add background traffic as explicit flows per the plan (long-lived bulk + heavy-tailed on/off),
   not occupancy replay.
3. Keep hybrid drop as the only receiver policy in W3's main matrix (production mirror); isolated
   credit may appear once as the optimistic bound in the shared-bottleneck slice to answer
   reconsideration condition 2.
4. Control/data asymmetry experiments (ack incast, reverse-path bursts against BlockAck cadence)
   fold into W3 per the plan.

## Reproduction confirmation (appended)

The independent full-sweep reproduction completed: `trials.csv`, `decision-table.csv`,
`decisive-summary.csv`, and `critical-paths.csv` (9,216 executions / 56,448 completion rows) are
all byte-identical to the committed artifacts. Verification is complete.
