# Stage 5 completion report

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Scope: documentation and conformance-evidence polish only. No production code, test code, manifest,
or wire behavior changed in Stage 5.

## Outcome

Stage 5 is complete and closes the implementation branch.

- Stage 3.2 remains a research **NO-GO**. Reservoir repair stays out of production and is documented
  everywhere as an extension **BEYOND the METTLE paper**.
- Stage 4 remains extracted to `plans/rateless-mettle-vnext.md` and is not part of this branch's
  completion criteria.
- Lossless protocol version 10 and the Stage 2 Rounds/Carousel behavior stand unchanged.
- The final documentation distinguishes CI-asserted contracts, manual evidence, paper-only claims,
  and known non-claims instead of presenting all evidence as equivalent.

## Deliverables

### Runtime invariants and protocol evidence

Created [`docs/perfect-runtime-invariants.md`](../docs/perfect-runtime-invariants.md).

- Maps L1 pooling, L2 no ownership, and L3 work conservation to exact enforcing test names and
  source files.
- Summarizes the receiver and sender state machines as a navigation index derived from Section P,
  while linking back to Section P as the sole normative specification.
- Links the version/layout history in `messages/src/lossless_session/mod.rs`.
- Defines the deployed verification envelope: RaptorQ bounds, METTLE's 65,536-source and 96 MiB
  negotiated prefix caps, and receiver decoder admission defaults.
- Classifies every load-bearing claim as CI-asserted, manually measured, or taken from the METTLE
  paper without independent verification.
- States explicitly that 4,096 zero-failure trials resolve only a one-sided 95% upper bound of
  `0.000731113` (about `7.3e-4`) and cannot verify smaller failure probabilities claimed by the
  paper.
- Records the absent process-wide sender-cache bound and host-sensitive decoder thresholds as
  non-claims/open policy rather than guarantees.

### METTLE paper deviations

Updated [`docs/mettle-paper-notes.md`](../docs/mettle-paper-notes.md).

- Names `Rounds + METTLE` as the regression-frozen finite-block adaptation and `Carousel + METTLE`
  as the paper-native object/prefix stream path inside negotiated deployment caps.
- Documents the discrete termination tail floor and the fact that applying the paper's 5.5% target
  at `K=256` is unattainable.
- Documents corrected Stage 2.6 accounting as
  `terminal_symbol_count / K - 1`, including the compressed tail, and the exact 105,500-symbol
  `K=100,000` / 5.5% fixture.
- Records the reservoir experiment and its 4,096-trial BEC/Gilbert–Elliott evidence, the accepted
  no-go ruling, and the absence of reservoir manifest/wire fields.

### Benchmark evidence

Created [`docs/perfect-runtime-benchmark-evidence.md`](../docs/perfect-runtime-benchmark-evidence.md).

- Folds in the Stage 1 matched-seed Rounds/Carousel methodology and all nine recorded trace rows.
- Recomputes the exact deltas: Carousel emitted 2.00% more symbols in aggregate and used 8.75% fewer
  logical completion ticks in aggregate; per-row tick reductions ranged from 6.16% to 11.19%.
- Retains the 5% emission and 10% tick interpretation bands and explicitly says they are not CI
  dominance thresholds.
- Folds in the Stage 3 manual dense-decoder remeasurement at `N=65,536`, `T=1,400`: 9.991 ms maximum
  construction and 116.109 MiB maximum RSS, against the manually evaluated 25 ms / 192 MiB budgets.
- Records exact reproduction commands and the mandatory manual remeasurement policy.

The historical 6–10% trace shorthand was corrected in the claims table to the exact 6.16–11.19%
per-row range derived from the committed raw counts.

## Commits

Stage 5 starts after the accepted Stage 3 ruling at `0825eb7` and ends at this report commit:
`0825eb7..HEAD`.

| Concern | Commit | Message |
| --- | --- | --- |
| Invariants, state-machine index, and claims classification | `aec0ae6` | `Document perfect runtime invariants.` |
| METTLE integration deviations and research ruling | `88ead9d` | `Document METTLE runtime deviations.` |
| Consolidated benchmark methodology and results | `872d6f3` | `Record perfect runtime benchmark evidence.` |
| Exact logical-trace range correction | `c943289` | `Correct the benchmark evidence range.` |
| Final Stage 5 and branch closure report | `HEAD` | `Document Stage 5 branch closure.` |

After this report is committed, the Stage 5 range contains five commits. The complete feature branch
range remains `e7aae32..HEAD`, 57 commits after the recorded fork point. Stage 5 changes only the
three documentation files above and this report.

## Final gate

Every Cargo command used:

```sh
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13
```

Results:

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed; no warnings. |
| `cargo nextest run` | Passed: 821 tests passed, 0 failed, 17 intentional skips across 33 binaries. Run id `ace4bebf-61c5-4ff0-a550-acc8c4842e02`. |

The frozen rounds regression tests remained in the unchanged production/test tree and passed as part
of the full run. No controller test was skipped for unavailable PostgreSQL.

## Open questions summary

No new Stage 5 question was added. The existing entries in
[`plans/perfect-fec-runtime-questions.md`](perfect-fec-runtime-questions.md) remain accurate and are
now linked from the claims table:

1. **Process-wide sender-cache admission:** Carousel+METTLE receiver memory has permits, but sender
   encoded-prefix retention still lacks a symmetric process-wide permit pool. Deployments must cap
   concurrent senders from the per-session bound.
2. **Automated performance thresholds:** release construction/RSS remain host-sensitive manual gate
   evidence until a controlled benchmark runner exists.
3. **Reservoir research conditions:** any future revisit needs a predeclared burst-channel efficacy
   threshold, an independent sender-memory admission design, and separate post-emission
   retransmission/duplicate accounting. It is not a blocker because reservoir integration was
   rejected.

There are no open questions blocking closure of `perfect-fec-runtime`.
