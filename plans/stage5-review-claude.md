# Stage 5 review and branch closure (Claude)

Date: 2026-07-16
Scope: commits `aec0ae6..c3d9ca1` (doc-only) against plan v2 Stage 5.
Verdict: **APPROVED. The perfect-fec-runtime branch is closed: all planned stages are done,
reviewed, and gated.**

## Verification performed

- Doc-only confirmed: no production/test source, wire, or manifest changes in the Stage 5 range.
- Final gate re-run on `c3d9ca1`: fmt clean; `cargo nextest run` → 821 passed, 0 failed, 17
  pre-existing skips. Worktree clean; `main` untouched; nothing pushed.
- Spot-checked seven cited test names across the invariants doc's evidence tables — all exist in
  the tree. The L1/L2/L3 → test map, the §P navigation summary (correctly deferential — "Section P
  wins"), and the version-10 layout-table link are all present.
- `docs/mettle-paper-notes.md` deviations section covers the mode matrix, the prefix-cap
  engineering deviation, the tail-floor unattainability finding, the corrected 2.6 overhead
  accounting, and the rejected reservoir extension with its Stage 3 evidence.
- The measured-vs-guaranteed table uses an explicit A (CI-asserted) / B (measured evidence) /
  C (paper-only, NOT independently verified) classification, states the deployed-envelope boundary,
  the 4,096-trial resolution bound (7.3e-4), and the explicit non-claims (sender-cache bound,
  manual perf budgets, reservoir rejection, vNext reseed).

## Final branch state

- Branch `perfect-fec-runtime`, forked from `codex/tree-scoped-transports-main` @ `e7aae32`.
- Lossless protocol version 10. Full suite: 821 passed / 17 intentional skips.
- Stages: 0 ✓ (reviewed, fixes verified) · 1 ✓ (reviewed, fixes verified) · 2 ✓ (reviewed, fixes
  verified, two recorded deferrals) · 3.0/3.1 ✓ with an accepted research NO-GO for 3.2 ·
  4 extracted to `plans/rateless-mettle-vnext.md` · 5 ✓ (this review).
- Standing open items (all recorded in `plans/perfect-fec-runtime-questions.md`): process-wide
  sender-cache admission before unbounded concurrent paper-native senders; manual perf-threshold
  policy (re-measure at every gate); re-check the 4-permit aggregate against real WAN nodes before
  any production benchmark campaign.
