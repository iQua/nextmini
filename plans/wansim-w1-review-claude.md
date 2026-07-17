# wansim W1 review (Claude)

Date: 2026-07-17
Scope: commits `fd50915..f48425e` against the W1 definition in `plans/wansim-plan.md` and the
three W0b review notes.
Verdict: **APPROVED. Experiment 0 did its job: one prediction failed for a real, diagnosed,
production-relevant reason; the other three passed as predeclared. W2 may proceed.**

## Verification performed

- Independent wansim gate: fmt/clippy clean, 68/68 tests.
- Committed artifact digests verified (`shasum -c SHA256SUMS`: OK).
- **Full experiment 0 independently reproduced**: re-ran the complete 640-trial sweep from the
  release binary into a fresh directory; `trials.csv`, `summary.csv`, and `predictions.csv` all
  byte-identical to the committed artifacts.
- The stop-and-diagnose discipline was honored: the sweep halts at the E0-a falsification and
  requires an explicit `--accept-source-done-overtake` flag to resume, with the diagnosis recorded
  in `screen-verdict.csv` before any further data was produced.

## The E0-a failure is the most valuable W1 result

Prediction E0-a (strict conservation ⇒ zero repair deficits) failed with zero physical drops and
zero inbox drops: `SourceDone` on the separate control connection overtakes data still queued on
multi-hop data connections — TCP orders within a connection, never across connections. Receivers
at rank 60/64 cached positive deficits (max 6) that no loss caused.

Production mapping: nextmini has the same structure (control on the default transport scope, data
on per-tree scopes — scope.rs:5), so deployed ROUNDS mode plausibly emits spurious repair traffic
after every round barrier for the same reason. Rounds is regression-frozen legacy, so this is
informational, but it materially strengthens the carousel design verdict: carousel has no round
barrier to race — cumulative BlockAcks are eventually consistent by construction, and a
"premature" ack snapshot is just a valid no-progress alive signal. The overtake mechanism is a
cost specific to barrier-style feedback over split transport scopes.

## Remaining verdicts, read narrowly (as the report itself insists)

- E0-b PASS: homogeneous paths ⇒ rounds == carousel exactly. Carousel's gain is NOT manufactured
  by the simulator; under strict conservation and equal paths there is none.
- E0-d PASS + a NEW third mechanism: under crossed path capacities carousel beats every baseline
  by 1.64–1.80 ms (8.0–8.3% of the barrier) with zero loss and zero drops — continuous pooled
  emission lets a fast path replace slow IN-FLIGHT members of the initial K window ("in-flight
  replacement"). This is distinct from the two known mechanisms (ownership waste; barrier removal)
  and was invisible to the opportunity-model simulator. Magnitude at production K is not
  established.
- E0-c PASS with the right qualification: the ACK-flight tail is exact (K+20 / K+28, nothing after
  sender completion) but 31–44% of K at K=64. The tail is roughly flight-proportional, not
  K-proportional, so production-scale K should shrink the fraction — W2+ must run larger K before
  any "small tail" language is used.
- Bonus finding: rate-proportional striping LOST to equal split in a crossed cell (28.9 vs
  20.4 ms) — concentrating a finite transfer on one hop-wise TCP path loses startup/window
  parallelism. Nominal capacity ≠ effective finite-transfer service; another argument that static
  ownership cannot be tuned into adequacy.

## Scope honesty

The six declared representations/omissions (one-block object, descriptor side channel, implicit
session negotiation, always-succeeding sink, no loss/coupling yet) are appropriate for W1 and
correctly labeled as scope choices rather than production claims. Determinism is reported
precisely (byte-identical repeats; registration-order metamorphism preserves causal outcomes with
transient same-time occupancy values correctly excluded from the identity claim).

## Directions for W2 (binding)

1. Run W2 at larger K (≥512) so tail fractions and flight sizes are not small-K artifacts.
2. The straggler × admission matrix runs carousel as the protocol under test (production mirror,
   sequential admission default), crossed exactly per the plan: policy {hybrid-drop, blocking,
   isolated-credit} × service rate {1×, ½×, ⅒×} × buffer budget {0.25, 1, 4} BDP × slow-receiver
   count × fan-out degree, with child-order sweep.
3. Deliverable stays: a quantified recommendation on whether isolated credit should be implemented
   in the production runtime, with the externality on healthy receivers as the primary metric.
