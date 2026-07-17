# wansim W3 review and experiment-ladder closure (Claude)

Date: 2026-07-17
Scope: commits `d2e21fd..c12dc50` against the W3 definition in `plans/wansim-plan.md` and the four
binding W2 review directions.
Verdict: **APPROVED. The planned experiment ladder (W0a–W3) is closed. W4 calibration remains
deferred, as planned.**

## Verification performed

- Independent wansim gate: fmt/clippy clean, 89/89 tests.
- Digest manifest verified: 10/10 CSVs OK.
- **Full sweep independently reproduced** (~140 s): all 10 artifact CSVs byte-identical.
- All four W2 directions honored: overlap axis with constant aggregate capacity and controlled
  TCP flow counts (matched dummy-payload second connection for the single-tree baseline — the
  flow-count confound is genuinely controlled, with its cost honestly declared); explicit TCP
  background flows, no occupancy replay; hybrid-only main matrix with the single isolated-credit
  reconsideration slice; control-asymmetry experiments with liveness margins reported.

## The three W3 results, as I read them

1. **A4 is falsified in the coupled model** (delivered-rate correlation −0.176 → 0.968 across the
   overlap axis). The theory's exogenous-rate assumption is a real idealization with a measured
   breaking point; any paper claim built on (A4) must scope itself to edge-disjoint or
   lightly-coupled deployments, or drop to the weaker sample-path statements.
2. **The two-advantage decomposition is the honest headline**: in this no-straggler, no-loss,
   background-competing matrix, pooled FEC contributes 0.31–1.10% and path diversity 47.4–47.8%.
   Context matters and the report states it: pooling's value concentrates where W1/W2 found it —
   crossed capacities (in-flight replacement), stragglers (drop-repair), and ownership under
   time-varying rates — not in a healthy symmetric steady state. The ladder's aggregate story is
   consistent, not contradictory: each mechanism has its regime, and a paper combining the pooling
   and path-diversity terms into one number would overstate the coding mechanism.
3. **The isolated-credit question is now fully resolved on current evidence**: W2 condition 2 is
   met (5.826 ms healthy benefit + 882 KiB shared-leaf savings), condition 1 (a bounded
   real-payload ownership design) remains unmet, so the production NO-GO stands with exactly one
   open door: propose a bounded ownership or shed-and-repair design, then re-run the shared-leaf
   slice.

## Also banked

- Production §P cadence defaults validated under adversarial reverse-path conditions: 1× is the
  best tested compromise (0.5× floods its own feedback path; 2× leaves more in flight); ≥95.1%
  stall-budget margin everywhere.
- Rounds-vs-carousel under coupling is reported as a genuine latency/bandwidth trade in the
  no-drop regime (rounds: marginally earlier receiver barrier, fewer emissions, but 5.2–5.3 ms
  later sender completion and the W1 overtake deficits in every execution) — the report resists
  declaring a universal winner, which is correct.
- The statistical-multiplexing surprise (full overlap improving absolute completion under fixed
  aggregate capacity) is properly quarantined as a topology/workload interaction.
- The W0a–W3 synthesis section is accurate against the four stage reports and reviews and is
  suitable as the paper-appendix seed it was requested to be.

## Ladder closure

Six stages (W0a, W0b, W1, W2, W3 + the plan/discussion round), all reviewed, all independently
reproduced byte-for-byte, 89 conformance tests, root workspace untouched at 828 throughout.
Standing state:

- Production verdicts: carousel/BlockAck/hybrid-drop/sequential-order semantics supported;
  naive blocking rejected; isolated credit NO-GO with two explicit reconsideration conditions
  (one now met); 1× cadence retained.
- Theory verdicts: Prop 1/2/3 mechanisms observed in their regimes; A4 measured to failure under
  overlap; the third mechanism (in-flight replacement) documented in W1.
- Open work, all deliberately deferred: W4 calibration (ns-lossless + Arbutus probe ladder, blind
  prediction) before any "twin" language or WAN effect-size claims; the bounded-ownership design
  question; finer-than-aggregate source→runtime attribution.
