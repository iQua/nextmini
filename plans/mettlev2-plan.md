# METTLE v2 Plan

## Goal

Rebuild the METTLE codec and its nextmini integration so that:

- the codec matches the paper's construction instead of a paper-inspired variant
- the first comparison against RaptorQ uses the paper's experiment contract
- no runtime or wire integration proceeds until the codec gate is convincing

This plan is the in-repo mirror of the active Linear project `METTLE v2`.

## Paper Fidelity Rules

When the paper gives a concrete construction or experiment setting, implement that setting first.

Use the following interpretation:

- Part II is the authority for codec construction details
- Part I is the authority for the reported benchmark claims and comparison framing
- do not substitute a "better" or "simpler" codec variant until the paper-faithful baseline exists

### Required codec shape

- `l = 4` edges per source symbol
- coupling window `w = 600`
- one TLE edge plus three randomized non-TLE edges
- non-TLE profile `(zeta_2, zeta_3, zeta_4) = (1/2, 1/4, 1/8)`
- source identity is time order / sequence number
- the TLE edge is deterministic: `h1(y) = floor((1 + c) * y)`
- coded packets are identified by bin index
- decoding is left-to-right peeling with occasional backward recovery through later bins

### Required evaluation contract

- symbol size: `1500` bytes
- METTLE uses a large streaming source sequence rather than small fixed source blocks
- coding-efficiency comparison uses the paper's latency-matched RaptorQ setup
- decode-speed comparison is measured as decode time per packet

## Current Status

As of `2026-04-14`, local branch `mettlev2` is here:

- done: `NEX-89` scaffold `mettle/`
- done: `NEX-90` params
- done: `NEX-91` graph placement
- done: `NEX-92` incremental encoder
- in progress: `NEX-93` online peel decoder
- done: `NEX-94` codec-core validation harness
- done: `NEX-95` local decode-speed checkpoint
- done: `NEX-102` tail compression
- done: `NEX-105` codec benchmark matrix

Current blocker:

- the local release checkpoint now stays clearly faster than RaptorQ across Table V's `k` values
- paper-completeness work still remains before runtime / wire integration, especially `NEX-103` fair comparison

## Work Sequence

### PR-01 Codec-first gate

1. `NEX-89` Scaffold `mettle` crate
2. `NEX-90` Define METTLE v2 discrete params
3. `NEX-91` Implement METTLE graph placement
4. `NEX-92` Implement incremental METTLE encoder
5. `NEX-93` Implement online peel decoder
6. `NEX-94` Add codec-core validation harness
7. `NEX-95` Add codec decode-speed checkpoint
8. `NEX-105` Add codec benchmark matrix

Gate:

- do not proceed to `NEX-96` until `NEX-95` is convincing and the local benchmark matrix remains stable

### PR-02 Minimal nextmini integration

1. `NEX-96` Add METTLE wire mode
2. `NEX-97` Add lossless mode policy plumbing
3. `NEX-98` Add no-loss METTLE sender path
4. `NEX-99` Add no-loss METTLE receiver path

### PR-03 Minimal repair semantics

1. `NEX-100` Add METTLE runtime shell semantics and repair path
2. `NEX-101` Add end-to-end METTLE integration tests
3. `NEX-107` Add ns-lossless single-host validation for METTLE

### PR-04 Paper completeness

1. `NEX-102` Add tail compression

### PR-05 Fair comparison

1. `NEX-103` Add fair compare harness

### PR-06 Optional cleanup

1. `NEX-104` Rename block-first `Fec` path to `RaptorQ`

### Later optimization only after correctness

1. `NEX-106` Optimize METTLE streaming session shell

## Exact Experiment Mirror

### METTLE codec parameters

- packet size: `1500` bytes
- `l = 4`
- `w = 600`
- non-TLE profile `(1/2, 1/4, 1/8)`

### Channels

Use the same ten channels from the paper:

- BEC: `0.01`, `0.02`, `0.03`, `0.08`, `0.10`
- GE1 VoIP
- GE2 WiMAX
- GE3 Video-conf-light
- GE4 Video-conf-heavy
- GE5 Long-fade

### METTLE latency reference

Average latency targets from Table II:

- BEC(0.01): `61`
- BEC(0.02): `84`
- BEC(0.03): `117`
- BEC(0.08): `133`
- BEC(0.10): `199`
- GE1: `37`
- GE2: `72`
- GE3: `53`
- GE4: `127`
- GE5: `50`

### RaptorQ latency-matched coding-efficiency comparison

Use Table IV's `k` values:

- BEC(0.01): `114`
- BEC(0.02): `168`
- BEC(0.03): `236`
- BEC(0.08): `269`
- BEC(0.10): `405`
- GE1: `84`
- GE2: `149`
- GE3: `114`
- GE4: `257`
- GE5: `101`

For LT, fix `k = 400`.

### RaptorQ decode-speed comparison

Use Table V's representative `k` values:

- `127`
- `257`
- `511`
- `1002`
- `2040`
- `4069`
- `8194`

The first latency-matched decode-speed claims in the paper are based on the small-`k` region, especially `k` between `100` and `400`.

## Commit Policy

- keep feature commits small and intentional
- target roughly `100` added lines per commit and split before `120` if possible
- commit locally once validation passes
- do not push by default
- no per-commit mandatory review during this phase
- do a batch review once a logically complete slice is ready

## Immediate Next Step

Keep tightening `NEX-93` only where it moves the implementation closer to the paper, then finish `NEX-103` before starting runtime / wire integration.
