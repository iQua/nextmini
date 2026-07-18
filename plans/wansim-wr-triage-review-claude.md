# WR triage review and rerun ruling (Claude)

Date: 2026-07-18
Scope: commits `866874d..5772982` (interim state, resilient harness, single-cell causal replay,
Task 3 proposal) against my triage directive.
Verdict: **Triage APPROVED. The (c) simulator-artifact verdict is accepted; production §P is
exonerated on this evidence. Task 3 items 0–3 are approved for implementation under the digest
proof; the resumed WR sweep is authorized conditional on that proof passing.**

## Verification performed

- Independent wansim gate: fmt/clippy clean, 111/111 passed (1 pre-existing ignored manual probe),
  including the new `resume_after_kill_preserves_durable_cells_and_discards_torn_shards` gate.
- Triage artifact digests verified 5/5.
- Report evidence reviewed in full. The identifying observation is airtight: the source emitted
  EXACTLY 65,536 frames per tree — the silent `K × 8` harness ceiling — and stopped at 4.658 s
  while the session was incomplete; organic congestion does not land on an integer guard. The
  fatal window shows a healthy ack path (92 BlockAcks, 93 joins, silence age 283 ms) with zero
  data arrivals — a manufactured no-progress condition, correctly aborted by the §P clock.
- My own earlier suspicion — the (a) block-granularity hypothesis from the 0ba7420 quantization —
  is cleanly refuted (watermark stopped at 409 of 481, 72 units from the final block). Recording
  this explicitly: the triage protected us from acting on a wrong but plausible theory.
- The honest classifier correction ((b) surface label → (c) causal label, same trace, no rerun)
  is exactly the right epistemic behavior.

## Rulings

1. **No production change.** Rank-carrying heartbeats / finer progress credit / tail-scaled stall
   budgets remain candidates ONLY if a future cap-free replay demonstrates a genuine case (a).
2. **The failed WR run produces no performance evidence** (already so marked in the interim
   commit). The 11.56× intermediate emission observation stands as model-correct for the unpaced
   overload regime, with the terminal 16× voided as guard-induced.
3. **Task 3 approved as proposed, in its order**: item 0 (remove the silent ceiling; any explicit
   guard becomes an immediate named `emission_guard_exhausted` failure + the >8K-emissions
   regression) is a correctness prerequisite; items 1–3 (fixed local counters, bounded triggered
   timelines, backbone agenda merge) under the byte-identical boundary-digest proof across the
   matched cell set and both registration orders; item 4 (background actor aggregation) stays
   deferred — the profiling showed it is a secondary count and fluid aggregation would corrupt the
   A4 measurement.
4. **One scenario addition for the rerun**: the straggler slice gains a paced arm (production-like
   token-bucket pacer as a knob, unpaced remains the default axis) — production has an optional
   pacer, and the unpaced-only sweep would leave the overload-waste behavior unqualified.
5. **Rerun authorization is conditional, not open**: if every digest comparison in the
   equivalence matrix passes, proceed directly into the resumed per-cell WR sweep (the Task 1
   harness protects the investment); if ANY digest mismatches, stop for review — a mismatch is a
   correctness failure, not a tolerance question.

## Process notes

- The atomic-batch loss (3,040 cells) is now structurally prevented by the Task 1 shard store;
  the lesson — sweep task specs must require durable per-cell persistence — was recorded in
  memory at failure time.
- The mid-run model corrections (block-granular ack progress, load calibration, FIFO jitter,
  horizon extension) were each reviewed post-hoc and stand; future stage specs should require
  Codex to flag protocol-model changes at commit time rather than at stage end.
