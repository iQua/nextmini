# wansim W2 report — straggler × receiver admission

Date: 2026-07-17

Branch: `perfect-fec-runtime`

Baseline: `8f0169a` (approved W1)

Evidence class: **model-level causal evidence, not a WAN measurement or calibrated twin**

## Production verdict

**NO-GO on implementing per-receiver isolated credit in the production runtime now.** Keep the
deployed hybrid drop-on-full policy, reject naive blocking, and retain isolated credit as an
optimistic simulator bound for W3.

This is not a finding that isolated credit has no value. Across the 72 matched topology/service/
buffer/order cells, isolated credit never made barrier completion or healthy-receiver completion
worse than hybrid drop. It improved barrier completion in 32 cells, by 12.125001 ms on average
among those cells and by as much as 27.338400 ms (27.45%). It also reduced source emissions in the
same 32 cells, by as much as 202 frames (26.44%), eliminated application-boundary deficits, and
reduced traffic delivered to the slow receiver in all 72 cells.

The decisive harsh cells do not justify the production complexity, however. With one 1/10×
receiver among eight and a 0.25-BDP budget, hybrid and isolated credit had exactly the same
515.197002–516.260202 ms barrier, zero healthy-receiver externality, 986–990 source emissions, and
25.7–25.8% use of the stall-abort budget. Isolated credit moved work from slow-branch transport and
receiver drops into branch-specific deferred delivery; it did not reduce sender work or completion
time in those cells.

More importantly, the simulated isolated policy is deliberately favorable. A blocked child retains
one constant-size contiguous frame-ID range, hard-bounded by the finite 8,192-frame tree stream,
and reconstructs deterministic simulator payload when credit returns. Production relays forward
opaque frame bytes and cannot reconstruct those bytes from a cursor. In the largest screened cell,
the range represented 7,443 frames on one branch, and 86,351 branch-frame obligations remained
across the tree when session completion made them cancelable. Retaining real 512-byte frames would
be about 3.63 MiB on that branch and 42.16 MiB in aggregate, outside the declared BDP queue budget.
Dropping those bytes instead would be upstream shedding backed by carousel repair, not the tested
zero-deficit isolated-credit policy.

Reconsider this NO-GO only after both conditions hold:

1. A production design proves bounded ownership of real frame bytes (or a valid source-side,
   per-receiver replay cursor) without new head-of-line coupling or an unbounded relay cache.
2. W3 shows that the saved slow-branch traffic materially improves healthy receivers or shared-link
   utilization under physical overlap. W2's edge-disjoint data links cannot establish that benefit.

The evidence rows behind this decision are in
[`decision-table.csv`](../results/wansim/w2-straggler/decision-table.csv), keyed by policy, fan-out,
and child order, and
[`screening-summary.csv`](../results/wansim/w2-straggler/screening-summary.csv), keyed by the full
matrix dimensions.

## What W2 models

W2 runs the independent W1 pooled-carousel endpoint over two copies of a deterministic balanced
fan-out tree. It never calls a production actor. Section P of
[`perfect-fec-runtime.md`](perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages)
remains the normative protocol source.

Configuration and checked BDP geometry live in `wansim/src/scenario/w2_config.rs`; topology and
model wiring in `wansim/src/scenario/w2.rs`; policy mechanics in
`wansim/src/overlay/{tree_relay,w1_receiver}.rs`; the matrix and artifact reducers in
`wansim/src/experiment/w2_straggler.rs`; and gates in `wansim/tests/w2_gates.rs`.

Every matrix run has one ideal-code object block with K = 512. A logical data frame is 508 payload
bytes plus the four-byte stream length prefix, carried in 512-byte TCP payload segments. Rank is
`min(K, distinct innovative deliveries)`; no real FEC codec runs in the event loop. The sender
continuously submits fresh pooled symbols to either writable tree, rechecks completion immediately
before submission, joins cumulative BlockAcks, probes missing peers, uses independent silence and
no-progress clocks, and repeats best-effort SessionComplete.

The topology builder preserves controller receiver order and realizes these per-tree shapes:

| Receivers | Requested max fan-out | Relays | Overlay edges |
| ---: | ---: | ---: | ---: |
| 3 | 2 | 2 | 5 |
| 3 | 4 | 1 | 4 |
| 8 | 2 | 7 | 15 |
| 8 | 4 | 5 | 13 |

The degree is a maximum: the three-receiver degree-4 case has three root children. Every overlay
edge is an independent long-lived days-derived TCP connection with independent Reno state. Data
links are 80 Mbit/s with 1 ms one-way propagation per hop. Each peer's independent control TCP
connection is 100 Mbit/s, with one-way propagation equal to its data-hop count in milliseconds.
Control traffic carries its real section-P framing charge.

The healthy decoder/sink service center takes 100 µs per frame. The single slow receiver is
receiver 0 and uses 100 µs, 200 µs, or 1 ms per frame for the 1×, 1/2×, and 1/10× axes. Runtime
command service takes 5 µs. Carousel timing is: 500 µs BlockAck debounce, 5 ms heartbeat, 4 ms
AckProbe cadence, 250 ms silence timeout, 2 s stall timeout, and 2.5 s receiver passive window.

### Admission policies

- **Hybrid drop (production mirror):** TCP receives and cumulatively acknowledges the bytes first.
  The shared runtime-command stage then dispatches data to the bounded inbox. Full-inbox data is
  dropped and read credit is returned; the control lane remains available. Later fresh carousel
  symbols replace the lost innovative opportunity.
- **Naive blocking:** a full inbox leaves the data command at the head of the shared runtime
  mailbox and returns no TCP read credit. Pressure traverses the real receiver/socket/link/relay
  chain. Because the shared stage is blocked, control behind that command can also wait. No data is
  dropped, but configured sequential child admission exposes healthy siblings to head-of-line
  blocking.
- **Isolated credit:** a full data inbox returns no data credit, but the receiver can dispatch
  control past the blocked data command. TCP pressure reaches that child's independent virtual
  sender. After its real child queue fills, the relay records a branch-local contiguous frame-ID
  debt and releases shared payload ownership, so later configured children continue. Debt is
  replayed in order as credit returns. Debt still present at the harness stop is completion-
  cancelable but is reported rather than silently discarded. This is the optimistic representation
  discussed in the verdict; it is not an existing production facility.

Hybrid and blocking retain sequential controller-order relay admission. The existing concurrent
variant remains configurable but is not a W2 axis. Isolated credit necessarily adds independent
per-child progress while still iterating children in controller order.

### BDP geometry

Per-hop BDP is computed from the configured data path, not supplied as a magic queue size:

```text
80,000,000 bit/s × (2 × 1,000,000 ns) / 8 / 1,000,000,000 = 20,000 bytes
```

The budget scales every identity proportionally; it never collapses the chain into one buffer.
Frame-counted stages round up to complete 512-byte frames.

| Budget | Scaled bytes | Socket send | Socket receive | Physical queue | Relay application | Child queue | Runtime data / inbox |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.25 BDP | 5,000 | 1,250 | 1,000 | 1,250 | 750 | 750 | 2 / 2 frames |
| 1 BDP | 20,000 | 5,000 | 4,000 | 5,000 | 3,000 | 3,000 | 6 / 6 frames |
| 4 BDP | 80,000 | 20,000 | 16,000 | 20,000 | 12,000 | 12,000 | 24 / 24 frames |

The shared runtime mailbox additionally reserves 32 commands for control. That reserve is outside
the data-BDP accounting and is identical for all policies.

## Experiment matrix and deterministic discipline

The screening matrix is exactly:

```text
3 admission policies
× 3 receiver service rates
× 3 buffer budgets
× 2 receiver populations (1 slow of 3, 1 slow of 8)
× 2 maximum fan-out degrees
× 2 child orders
= 216 cells × 32 seeds = 6,912 executions
```

The decisive `1/10× × 0.25 BDP × 1 of 8` slice has 12 policy/fan-out/order cells extended to 128
seeds. Its matched 1× baselines are also extended to 128 seeds, for 9,216 actual simulator
executions total. There is no result replication or row weighting.

Each nexosim instance uses one worker. The reproduction command uses eight host threads only across
independent `(scenario, seed)` instances, stores results by canonical task index, and stable-sorts
all serialized rows. The W2 channel has no stochastic loss process, so payload PRF seeds do not
change timing: means equal P95 in each cell. The requested repetitions exercise scenario identity,
parallel reduction determinism, and artifact reproducibility; they are not independent samples of
a noisy WAN.

## Decisive results

All values are exact per-trial means over 128 executions. `Slow delivered` counts complete logical
frames delivered by hop TCP to receiver 0 through its local completion. `Slow deficits` counts
innovative opportunities dropped at that receiver before completion. Carousel has no separate
repair frame kind: later fresh emissions recover those deficits.

| Policy | Fan-out | Order | Barrier (ms) | Healthy Δ (ms) | Source emissions | Slow delivered | Slow deficits | Policy mechanism cost |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| hybrid | 2 | slow first | 516.260202 | 0 | 990 | 968 | 454 | 510 deficits over all 8 receivers |
| hybrid | 2 | slow last | 516.260202 | 0 | 990 | 968 | 454 | 510 deficits over all 8 receivers |
| hybrid | 4 | slow first | 515.197002 | 0 | 986 | 970 | 456 | 519 deficits over all 8 receivers |
| hybrid | 4 | slow last | 515.197002 | 0 | 986 | 970 | 456 | 519 deficits over all 8 receivers |
| blocking | 2 | slow first | 516.260202 | 229.043588 | 557 | 516 | 0 | 1,074 blocking observations |
| blocking | 2 | slow last | 516.260202 | 227.043588 | 557 | 516 | 0 | 1,076 blocking observations |
| blocking | 4 | slow first | 515.197002 | 229.406788 | 545 | 516 | 0 | 1,064 blocking observations |
| blocking | 4 | slow last | 515.197002 | 227.406788 | 545 | 516 | 0 | 1,066 blocking observations |
| isolated | 2 | slow first | 516.260202 | 0 | 990 | 516 | 0 | 964 deferred / 511 replayed; HWM 228; 453 outstanding |
| isolated | 2 | slow last | 516.260202 | 0 | 990 | 516 | 0 | 964 deferred / 511 replayed; HWM 228; 453 outstanding |
| isolated | 4 | slow first | 515.197002 | 0 | 986 | 516 | 0 | 958 deferred / 504 replayed; HWM 227; 454 outstanding |
| isolated | 4 | slow last | 515.197002 | 0 | 986 | 516 | 0 | 958 deferred / 504 replayed; HWM 227; 454 outstanding |

The primary metric is decisive: blocking imposes 227.044–229.407 ms on the slow receiver's healthy
siblings. Slow-first adds almost exactly 2 ms relative to slow-last; it changes neither barrier nor
the result for hybrid/isolated. Hybrid drop and isolated credit both remove 100% of that healthy
externality. Hybrid does so by accepting receiver-boundary loss; isolated does so by retaining a
branch obligation before transport delivery.

All decisive policies have the same slow-receiver barrier because 512 × 1 ms decoder/sink service
dominates. Hybrid/isolated emit 474–478 frames beyond K, versus 33–45 for blocking. Only 8 of the
hybrid/isolated emissions and 4–5 blocking emissions occur after the receiver barrier. The rest is
work admitted while the sender still lacks a complete cumulative BlockAck; it must not all be
labeled ACK tail or drop repair.

## Whole-matrix hybrid-versus-isolated comparison

The following aggregates each service/buffer point over its eight population/fan-out/order cells.
Positive barrier/emission savings favor isolated credit. Isolated was never worse.

| Slow service | Budget | Cells faster | Mean barrier saved (ms) | Mean source emissions saved | Mean slow deliveries saved | Mean hybrid slow deficits |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1× | 0.25 BDP | 8/8 | 4.246403 | 8.00 | 8.75 | 8.375 |
| 1× | 1 BDP | 8/8 | 25.439800 | 186.00 | 193.00 | 198.500 |
| 1× | 4 BDP | 0/8 | 0 | 0 | 907.50 | 931.500 |
| 1/2× | 0.25 BDP | 8/8 | 5.121500 | 9.50 | 11.25 | 10.000 |
| 1/2× | 1 BDP | 8/8 | 13.692300 | 108.75 | 351.75 | 360.375 |
| 1/2× | 4 BDP | 0/8 | 0 | 0 | 2,411.50 | 2,434.750 |
| 1/10× | 0.25 BDP | 0/8 | 0 | 0 | 453.50 | 455.500 |
| 1/10× | 1 BDP | 0/8 | 0 | 0 | 3,342.75 | 3,348.750 |
| 1/10× | 4 BDP | 0/8 | 0 | 0 | 14,791.50 | 14,815.500 |

The maximum isolated-credit improvement is the `1× / 1 BDP / 3 receivers / fan-out 4 / slow
first` row in `screening-summary.csv`: 27.338400 ms, or 27.45%, and 202 source frames, or 26.44%.
Across all 32 improved cells, mean barrier savings are 12.125001 ms and mean source savings are
78.0625 frames.

The 4-BDP rows explain why branch traffic is not enough for a GO. With 1/10× service, hybrid
delivers about 15,350 frames to the slow receiver before it finishes K=512; isolated delivers 560.
Yet both policies emit roughly 15,512–15,638 frames at the source and complete at the same time.
Isolated credit keeps wasted traffic off the final branch, but W2 assigns distinct physical
resources to distinct overlay links; the saved leaf capacity is not another receiver's bottleneck.
W3 is the proper place to test whether it matters on a shared physical resource.

## Blocking externality and buffer sensitivity

Naive blocking produced no application deficits in any of 3,072 trials, but every trial recorded
real chain blocking. It caused positive healthy externality in 40/72 screening cells. Hybrid and
isolated had exactly zero healthy externality in all 72 cells.

| Slow service | Budget | Mean healthy externality (ms) | Range across topology/order (ms) |
| --- | ---: | ---: | ---: |
| 1× | 0.25 BDP | 0 | 0 |
| 1× | 1 BDP | 0 | 0 |
| 1× | 4 BDP | 0 | 0 |
| 1/2× | 0.25 BDP | 0 | 0 |
| 1/2× | 1 BDP | 25.989400 | 25.593200–26.408400 |
| 1/2× | 4 BDP | 6.569200 | 6.369200–6.769200 |
| 1/10× | 0.25 BDP | 228.703988 | 227.043588–230.958787 |
| 1/10× | 1 BDP | 385.871000 | 384.674800–387.090000 |
| 1/10× | 4 BDP | 225.650800 | 224.650800–226.650800 |

The non-monotonic 0.25/1/4-BDP result was an honest surprise. One BDP is worst for blocking in the
slow cases: it admits enough work to synchronize a long shared-chain stall but not enough to absorb
it. Four BDP partly decouples healthy work before the chain closes. “More buffer is always safer”
is not supported; queue identity and placement matter.

## ACK flight, liveness, and no-straggler gate

The matched provisioned no-straggler cell uses 1 ns decoder service and four BDP. All three policies
produce the exact completion vector `26.281203;26.281203;25.182003 ms`, 736 source emissions, and
zero drops, blocking observations, or isolated debt. This is recorded in
[`sanity.csv`](../results/wansim/w2-straggler/sanity.csv).

| K | Total emissions | Emitted after receiver barrier | Tail fraction |
| ---: | ---: | ---: | ---: |
| 64 | 282 | 100 | 35.4609% |
| 512 | 736 | 104 | 14.1304% |

K=512 reduces the ACK-flight fraction by 21.3305 percentage points, or 60.15% relative to K=64,
while the absolute in-flight count remains roughly constant. This passes the W1 review direction;
14.13% is smaller, not negligible.

W2 has one acked object block, so BlockAck does not report intermediate symbol rank. The no-progress
clock therefore runs from Ready freeze until the complete ack. Maximum pressure was 258/1,000 of
the 2 s stall budget: 516.260202 ms, leaving 1.483740 s. Heartbeats keep the separate silence clock
alive. No peer approached abort.

## Critical-path attribution

[`critical-paths.csv`](../results/wansim/w2-straggler/critical-paths.csv) contains one row for every
receiver completion in every execution: 56,448 rows. Each row pins the final tree/frame and sums
exactly to that receiver's completion timestamp. The decisive barrier-receiver means are:

| Policy | Source generation (ms) | Source → runtime (ms) | Runtime wait (ms) | Decoder queue (ms) | Service (ms) | Total (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| hybrid | 505.728029 | 7.419195 | 0.010000 | 1.571378 | 1.000000 | 515.728602 |
| blocking | 480.427403 | 30.362999 | 1.943200 | 1.995000 | 1.000000 | 515.728602 |
| isolated | 268.162416 | 242.627986 | 1.943200 | 1.995000 | 1.000000 | 515.728602 |

The equal totals hide different mechanisms. Hybrid generates the final accepted symbol late after
many application drops, then delivers it quickly. Blocking delays both global generation and the
transport chain. Isolated generates the eventual final frame much earlier, then holds it behind
branch credit for 242.628 ms.

For the healthy receiver that finishes last in each decisive trial, hybrid and isolated have
exactly zero component delta against their matched all-healthy baseline. Blocking adds 228.225188
ms on average: 212.264987 ms in delayed source generation and 16.060201 ms from source to runtime,
with −0.100 ms net in the two local queue terms. This directly attributes the healthy externality
to shared upstream/fan-out work rather than decoder service.

`Source → runtime` is a causal boundary aggregate over TCP serialization, propagation, queueing,
relay service, and transport-credit blocking. W2 does not pretend those overlapping internal waits
are independently additive. Finer resource attribution remains a future instrumentation task.

## Honest surprises and limits

- The strongest result against naive blocking is not surprising; the magnitude is. A single
  1/10× receiver delayed healthy siblings by up to 387.090 ms even though TCP lost no bytes.
- Isolated credit helped moderate 1×/1/2× cells but not the deliberately harsh 1/10× decisive
  cells. There, decoder service fixes the barrier and the work-conserving source can emit 15,000+
  frames while waiting for one complete-block ack. A small healthy-path ACK tail does not imply
  small overhead under a slow decoder.
- Buffer sensitivity is non-monotone. One BDP produced more blocking externality than either
  0.25 or 4 BDP in the slow cases.
- The isolated-credit result is an optimistic upper bound because simulator payload is
  deterministic and reconstructible from frame ID. Real relay payload ownership is the unresolved
  implementation problem behind the NO-GO.
- W2 has no stochastic loss, cross-traffic, shared physical bottleneck between distinct overlay
  links, CPU contention, codec algebra, allocation failure, or sink error. Decoder/sink cost is a
  fixed serial service time. Results are causal evidence inside this scenario envelope, not a WAN
  performance prediction.

## Policy and determinism gates

The W2 tests pin:

- hybrid refusal occurs after transport delivery and a covering TCP acknowledgment, and creates a
  pre-completion innovative deficit;
- naive blocking fills and blocks the actual reliable buffer chain, with zero application loss;
- isolated credit gives control priority, never blocks a healthy sibling's frames, has zero loss,
  and keeps a contiguous debt range below the hard finite-stream bound;
- the range rejects gaps, changing wire geometry, and capacity overflow;
- the provisioned no-straggler cell is policy invariant;
- K=512 has a smaller ACK-flight fraction than K=64;
- repeated W2 executions are byte-identical;
- every critical-path decomposition equals its receiver completion; and
- the eight-receiver fan-out/fan-in mailbox plumbing remains below capacity.

Across the full matrix, physical `queue_drop + segment_drop` is exactly zero. Maximum nexosim
mailbox high-water is 75/256 (32/256 in the decisive hybrid/isolated degree-2 cells), so simulator
mailboxes are nonbinding. The existing W0a, W0b, and W1 gates are included unchanged in the 80-test
wansim suite.

## Artifacts and reproduction

Committed under [`results/wansim/w2-straggler/`](../results/wansim/w2-straggler/):

- `trials.csv`: 9,216 exact execution rows;
- `screening-summary.csv`: 216 screening cells;
- `decisive-summary.csv`: 12 main 128-seed cells with sums and means;
- `decision-table.csv`: compact production-decision projection;
- `critical-paths.csv`: 56,448 per-receiver causal rows;
- `tail-fraction.csv`: matched K=64/K=512 ACK-flight measurement;
- `sanity.csv`: three-policy no-straggler result; and
- `SHA256SUMS`: digest manifest for every CSV.

Exact reproduction command:

```bash
cd /Users/winifred/nextmini-perfect-fec/wansim
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo run --release --bin w2_straggler -- \
  32 128 8 ../results/wansim/w2-straggler

cd ../results/wansim/w2-straggler
shasum -a 256 -c SHA256SUMS
```

The release sweep took about 14 minutes on eight host workers and used roughly 1.3–1.5 GiB peak
resident memory. Each simulation remained single-worker. This is harness execution cost, not a
modeled or production memory claim.

## Commits

| Concern | Commit | Content |
| --- | --- | --- |
| Admission and topology | `9152ef9` | Three receiver policies, dynamic 3/8-receiver balanced trees, independent child TCP, W2 configuration. |
| Harness | `e480db5` | Deterministic task matrix, matched baselines, release CLI, summary artifacts, W2 gates. |
| Attribution | `e433fd5` | Source-generation component and exact completion-sum invariant. |
| Bounded policy state | `e1111a4` | Constant-size hard-bounded debt run, per-completion attribution, slow-branch and policy-cost metrics. |
| Evidence | `f9de4d5` | 9,216 executions, summaries, per-receiver paths, and SHA-256 manifest. |
| Report | this commit | Production verdict, methodology, decomposition, limitations, and final gates. |

## Final gates

| Gate | Result |
| --- | --- |
| `cd wansim && cargo fmt --check` | PASS |
| `cd wansim && cargo clippy --all-targets -- -D warnings` | PASS |
| `cd wansim && cargo nextest run` | PASS — 80 passed, 0 skipped |
| Root `cargo nextest run` spot regression | PASS — 828 passed, 17 existing skipped |
| Artifact cardinality/invariant audit | PASS — 9,216 trials, 56,448 completions, zero loss, exact sums |
| `shasum -a 256 -c SHA256SUMS` | PASS — 7/7 CSV artifacts |

Commands used for Cargo gates always set
`CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13`. The unchanged Cargo warning about
the nested vendored-days patch table remains expected; the wansim workspace root supplies the
effective nexosim path patch.

No W3 implementation is included.
