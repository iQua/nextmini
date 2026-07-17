# wansim W0b review (Claude)

Date: 2026-07-17
Scope: commits `af5abef..7ee653e` against the W0b definition in `plans/wansim-plan.md`
(resolutions item 4) and the three W0a review notes.
Verdict: **APPROVED — the WAN pipeline substrate is complete; W1 (§P endpoints + experiment 0)
may proceed.**

## Verification performed

- Independent wansim gate: fmt clean, clippy `-D warnings` clean, 36/36 tests.
- W0b golden tree trace independently reproduced byte-for-byte from the release binary; the W0a
  golden re-run and confirmed still byte-stable alongside it.
- Root workspace regression previously re-verified at 828/828 after the W0a review; W0b touched
  only `wansim/`.

## Assessment

- All three W0a review notes were honored: the tree plateau is enumerated over 25 named owners
  with intentional zeroes (6,144 admitted = six-owner first-child path; 12,288 resident = prefix +
  3 replicated leaf chains; no double counting of in-flight vs send-buffer bytes); the hybrid drop
  is proven causally — the covering TCP ACK arrived back at the sender at 16.568 ms before the
  50.832 ms application-boundary refusal, with `acked_through` carried on the drop record; mailbox
  instrumentation stayed on under fan-in (max 3/256, nonbinding).
- The first production-relevant quantitative result of the whole wansim effort: the sequential
  child-admission externality. With one paused first child, receivers 2/3 complete at 258.742 ms
  sequential vs 82.598 ms concurrent (3.1×), while the blocked receiver itself is unaffected
  (264.254 ms in both). The first block occurs at frame 6, exactly after the blocked child's real
  3,072-byte downstream chain fills — the design review's "not instantaneous" requirement holds.
  This is direct model evidence that processor.rs:1218's sequential await taxes healthy peers;
  W2 will decide whether the production change is justified.
- Honest qualification correctly stated: concurrent admission removes the configured-order
  externality, not conservation-induced coupling under finite relay memory — and the experiment
  deliberately provisions the relay buffer to isolate the former.
- Substrate correctness gates all present: conservation (each receiver exactly K=16, order
  preserved per child stream), five distinct TCP flow identities with no segment multicast,
  registration-order metamorphism, deterministic goldens for both scenarios.
- Scope discipline held: no §P machinery leaked into W0b; the per-node runtime-command stage is
  correctly noted as single-session-equivalent, with cross-session contention deferred.

## Notes for W1 (non-blocking)

1. Experiment 0's strict-conservation configuration must use the no-drop provisioning proven here;
   the falsifier's meaning depends on zero application-boundary drops, so assert the drop counters
   are zero in that scenario.
2. The §P endpoints must send control over their own connection (scope split) — the W0b control
   lane exists at the receiver but no control transport route was needed yet; W1 adds it.
3. Keep the sequential-admission default for all W1 experiments (production mirror); the
   concurrent variant stays a knob for W2.
