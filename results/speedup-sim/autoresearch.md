# Pooled cross-tree FEC speedup experiment log

Scope: **model-level evidence using an ideal-DoF abstraction; not a codec result and not a WAN measurement**.

## Frozen experiment

- Objective: decompose the completion-time advantage of pooled, work-conserving FEC into (1) removal of stripe ownership and (2) removal of rounds-style feedback barriers.
- Primary state: one integer DoF bucket per receiver, with rank `min(K, distinct innovative deliveries)` and `K = 8192`.
- Coupling: all five protocols for one cell and seed read the same deterministic per-`(tick, tree, receiver)` opportunity and loss trace. Trace generation continues through intervals a protocol leaves idle.
- Matrix: `m in {2,4,8}`, six rate profiles, five loss profiles, receiver counts `{1,3,8}`, RTTs `{8,64,512}`, and 512 seeds per cell.
- Frozen seed domain: `0x504f4f4c5f444f46`, further domain-separated by tree count, rate profile, loss profile, tree, and receiver.
- Raw aggregates retain exact integer sums and nearest-rank P95 values; decimal means are derived only when CSV rows are written.

## Hypotheses and decisions

| Hypothesis | Test | Decision |
|---|---|---|
| P-a: homogeneous, no-loss rate-proportional striping matches pooling | Paired per-seed barrier gap | Keep: exact zero mean and zero maximum absolute gap over 4,608 paired trials. |
| P-b: ownership waste, rather than emission count, explains the striping gap | Across-cell Pearson correlations at RTT 64 | Qualify: gap/waste is positive (`r = 0.689172597`), but gap/emission-gap is stronger (`r = 0.886551973`). Ownership is the modeled cause of non-innovative delivery; emission count is the better direct completion-time proxy in this work-conserving model. |
| P-c: rounds-to-carousel gain grows with RTT times loss while carousel pays a small tail | Across-cell correlation plus sender tail-emission/total-emission ratios | Split: timing prediction passes strongly (`r = 0.961130411`); the tail is small at RTT 8 and 64 but is neither small nor constant over the whole sweep: 0.214%, 1.855%, and 12.734% at RTT 8, 64, and 512. Tail deliveries remain a separate receiver-counted metric. |

## Runs

1. Smoke sweep, one seed per cell: verified schema and all 39,971 CSV lines.
2. Full sweep, 512 seeds per cell: 21.58 s wall, 47.78 s user, 9.28 s system on the recorded workstation invocation; produced the committed CSVs.
3. Independent full rerun to `/tmp/pooling-speedup-reproduction-v3`: every committed CSV compared byte-for-byte equal with `cmp`.
4. Sanity audit: all 4,050 summary rows carry `K=8192` and 512 seeds; all five protocols are present; all 810 decomposition rows are present; no aggregated tree emission count exceeds its available-opportunity count.

The detailed methodology, grouped tables, and limitations are in `plans/pooling-speedup-sim-report.md`.
