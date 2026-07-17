# wansim W3 report — coupling and control asymmetry

Date: 2026-07-17

Branch: `perfect-fec-runtime`

Scope: W3 only; W4 calibration was not started

Evidence class: **model-level causal evidence, not a WAN measurement or calibrated twin**

## Verdict

W3 is complete. It establishes three distinct results inside the simulated envelope.

1. **The exogenous-tree-rate assumption A4 fails under physical overlap.** The cross-seed
   delivered-rate correlation of two saturated lane probes moves from -0.176 at edge-disjoint
   overlap to 0.968 at full overlap. Shared TCP queues and shared cross-traffic therefore make
   tree rates endogenous rather than independent inputs.
2. **Most of the two-tree completion benefit in this matrix is path diversity, not pooled FEC.**
   Carousel beats the strongest ownership baseline at every overlap, but only by 0.238–0.901 ms
   (0.31–1.10%). Choosing two paths instead of the matched best single path saves 69.570–73.511 ms
   (47.38–47.80%). The decomposition is exact and separately reported.
3. **W2 reconsideration condition 2 is met, but the production decision remains NO-GO.** On the
   harsh shared-leaf slice, isolated credit removes 452 slow-branch deliveries and 882,144 shared
   wire bytes and improves the healthy receivers by 5.826001 ms. It does not improve the barrier
   or source emission count, and W2 condition 1—a bounded real-payload
   ownership design—remains unsatisfied. Keep production hybrid drop.

The control study does not identify a reason to change the W2 BlockAck timings. Under eight-way ACK
incast plus seeded reverse bursts, the existing 1x cadence has the best mean sender completion and
smallest ACK-flight tail of the three tested settings. All settings consume at most 4.9% of the
2-second stall budget, leaving at least 95.1% margin.

These conclusions concern this deterministic model and parameter envelope. They are not Arbutus
predictions. W4 calibration remains deferred exactly as requested.

## Questions and experiment design

W3 answers four predeclared questions:

- How does physical overlap change the independent pooling advantage and path-diversity advantage?
- Does sharing bottlenecks measurably violate theory assumption A4?
- Does isolated credit become useful when the traffic it suppresses competes with healthy traffic?
- Do reverse-path incast and bursts require a different BlockAck debounce/heartbeat cadence, or
  threaten the liveness clocks?

All protocol endpoints remain the independent wansim implementations from W1. The production
actors are not called. Carousel uses the section-P cumulative BlockAck state machine, hybrid
drop-after-transport delivery, and configured-order sequential relay admission. The codec remains
an ideal DoF bucket: a distinct delivered frame contributes one innovative degree of freedom until
rank K. This isolates queueing, transport, fan-out, admission, and feedback mechanisms; it does not
model METTLE or RaptorQ algebra or CPU cost.

### Main physical-overlap matrix

The main matrix uses K=512, three receivers, two receiver-covering overlay trees, a 508-byte symbol
plus 4-byte length prefix per frame, persistent per-hop Reno TCP, finite buffers, and the W1 tree
shape. Each execution has four serial physical bottleneck stages on the two trees' root data paths:

- 0%, 25%, 50%, and 100% overlap correspond to 0, 1, 2, and 4 shared stages;
- a private stage has two independent 80 Mbit/s servers and two 65,536-byte queues;
- a shared stage has one 160 Mbit/s server and one 131,072-byte queue;
- installed aggregate capacity is therefore exactly 160 Mbit/s at every stage and every overlap;
- total modeled propagation over the four stages is 1 ms in either case;
- downstream overlay links remain edge-disjoint at 160 Mbit/s so the axis changes only the named
  physical root-path bottlenecks.

The phrase “640 Mbit/s across four stages” in `flow-counts.csv` is installed-capacity accounting,
not an end-to-end throughput claim: the stages are serial.

Each lane has one foreground tree TCP connection, one long-lived bulk TCP flow in each direction,
and one seeded on/off TCP flow in each direction. The on/off durations use a bounded heavy-tailed
power-of-two distribution, with 0.5 ms on and 0.75 ms off bases and exponent capped at 6. All
background bytes and ACKs traverse the physical path as real days-derived Reno segments; there is
no occupancy replay. This produces two foreground and eight explicit background TCP flows at every
overlap. A shared server sees ten active flows; each private lane server sees five.

The compared protocols are:

- **carousel:** pooled DoF, continuously work-conserving across both trees;
- **per-stripe FEC:** the strongest ownership baseline, with an independent DoF bucket per stripe;
- **best single tree:** both single-tree candidates are run on every seed and the faster barrier is
  selected. The otherwise idle foreground connection emits matched non-useful payload, so it keeps
  the same two foreground TCP connections and total active flow count rather than gifting the
  baseline one fewer competing flow;
- **pooled rounds:** a reduced 32-seed slice at 0% and 100%, for feedback-barrier cost only.

The 0% and 100% endpoints use 128 decisive seeds per main protocol. The 25% and 50% interior points
use 32 screening seeds. The rounds slice uses 32 seeds per endpoint. Main and rounds artifacts
contain 1,344 raw executions. Main cells had exactly zero receiver-application drops and zero
physical-link drops; their finite queues and TCP backpressure remained lossless.

### Exact advantage definitions

For mean receiver-barrier time B:

```text
pooling advantage       = B(per-stripe FEC) - B(carousel)
path-diversity advantage = B(best matched single tree) - B(per-stripe FEC)
total two-tree advantage = B(best matched single tree) - B(carousel)
```

The committed artifact asserts the additive identity for every overlap. This avoids attributing
the ordinary benefit of having two usable paths to pooled FEC.

### A4 measurement

Two saturated bulk background flows, one per lane, serve as protocol-independent rate probes. The
primary statistic correlates their total delivered bytes across seeds. It avoids interpreting the
shared source start clock as physical coupling. A secondary within-trace statistic divides each
probe trace into 5 ms windows, skips startup, differences adjacent windows, and reports both signed
and absolute mean correlation. Finite samples and TCP dynamics mean the edge-disjoint estimate is
not expected to be exactly zero.

### Isolated-credit reconsideration slice

This deliberately appears once. It uses K=512, eight receivers, fan-out two, one 1/10x slow
receiver first in controller order, a 0.25-BDP buffer budget, and the W2 hybrid versus optimistic
isolated-credit implementations. Receiver 0 and healthy receiver 1 share the same 100%-overlapped
leaf bottleneck on each tree. There are 128 paired seeds per policy (256 executions). This is the
harshest W2 case placed on the resource whose absence made W2 inconclusive about condition 2.

### Control/data-asymmetry slice

This slice uses K=512, eight healthy receivers, fan-out four, production-mirror hybrid admission,
a shared 20 Mbit/s reverse bottleneck, and 16 ms reverse propagation. It crosses:

- ACK incast only (32 screening seeds) versus ACK incast plus a seeded on/off reverse TCP flow
  (128 decisive seeds); and
- debounce/heartbeat timing at 0.5x, 1x, and 2x the W2 values. The exact pairs are
  0.25/2.5 ms, 0.5/5 ms, and 1/10 ms.

The AckProbe interval and liveness budgets are held fixed. This is 480 executions. Physical queue
drops are counted separately from post-TCP receiver-inbox drops; TCP retransmission keeps the
transport byte stream reliable.

## Main result: pooling and path diversity are different mechanisms

| Physical overlap | Seeds | Carousel barrier | Per-stripe barrier | Best matched single | Pooling advantage | Path-diversity advantage | Total two-tree advantage |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 128 | 80.744513 ms | 81.645999 ms | 155.157339 ms | 0.901486 ms (1.1041%) | 73.511340 ms (47.3785%) | 74.412826 ms |
| 25% | 32 | 79.981353 ms | 80.445828 ms | 153.349403 ms | 0.464475 ms (0.5773%) | 72.903575 ms (47.5408%) | 73.368050 ms |
| 50% | 32 | 78.970703 ms | 79.711828 ms | 151.865916 ms | 0.741125 ms (0.9297%) | 72.154088 ms (47.5117%) | 72.895213 ms |
| 100% | 128 | 75.738519 ms | 75.976638 ms | 145.546869 ms | 0.238119 ms (0.3134%) | 69.570231 ms (47.7991%) | 69.808350 ms |

The pooling advantage is positive at all four overlap levels, so ownership still has a measurable
cost. It is small and non-monotone, however, and falls to 0.31% at full overlap. The much larger
47–48% path-diversity term comes from using two TCP paths rather than serializing useful ownership
on one matched path. This is the clean split requested by the W1 review: “pooled FEC is faster” is
true here, but would badly overstate the coding mechanism if it included the path-diversity term.

An honest surprise is that absolute completion improves as overlap rises. Holding aggregate
installed capacity fixed does not hold queueing behavior fixed: at 100%, one 160 Mbit/s work-
conserving server statistically multiplexes the two lanes and their bursty cross-traffic, whereas
at 0% each lane is confined to its own 80 Mbit/s server. This is a resource-pooling effect of the
physical queue, not evidence that overlap is intrinsically beneficial. A differently calibrated
cross-traffic mix could reverse it.

Per-tree foreground utilization stays modest because the transfer is finite and TCP startup plus
background sharing matter: carousel's tree-0/tree-1 means are 190/189, 190/192, 194/194, and
204/202 permille from 0% through 100%. The maximum nexosim mailbox high-water in the main matrix is
108/256, so engine plumbing is not the bottleneck.

## A4 violation: tree rates become endogenous

| Overlap | Seeds | Cross-seed delivered-rate correlation | Mean within-trace delta correlation | Mean absolute within-trace correlation | Reading |
|---:|---:|---:|---:|---:|---|
| 0% | 128 | -0.176208 | 0.496478 | 0.553111 | finite-sample edge-disjoint reference |
| 25% | 32 | 0.082289 | 0.303955 | 0.446363 | shared-stage coupling appears |
| 50% | 32 | 0.393881 | 0.239238 | 0.345400 | material endogenous coupling |
| 100% | 128 | 0.968379 | 0.436880 | 0.456988 | nearly common cross-seed rate state |

The primary cross-seed measure rises from a weak negative edge-disjoint estimate to 0.968 under
full sharing. That is direct model evidence against A4 in this topology: congestion windows,
bursts, and queue service on shared physical links jointly determine both tree rates. The secondary
windowed metric is noisier and non-monotone because differencing emphasizes Reno burst timing; it
supports activity coupling but should not be treated as a monotonic overlap estimator.

## Critical-path shifts

The causal decomposition uses the same exact boundary method as W2. For each receiver's completing
frame it partitions source generation time, the aggregate source-to-runtime interval, runtime wait,
decoder queue, and decoder service. The parts sum exactly to completion; overlapping waits are not
added. The table below uses the barrier receiver for carousel.

| Overlap | Source generation | Source → runtime | Runtime wait | Decoder queue | Decoder service | Barrier total | Final tree counts |
|---:|---:|---:|---:|---:|---:|---:|---|
| 0% | 77.360261 ms | 3.369055 ms | 0.005071 ms | 0.000125 ms | 0.010000 ms | 80.744513 ms | t0 61 / t1 67 |
| 25% | 76.634078 ms | 3.332274 ms | 0.005000 ms | 0 | 0.010000 ms | 79.981353 ms | t0 15 / t1 17 |
| 50% | 75.696329 ms | 3.259374 ms | 0.005000 ms | 0 | 0.010000 ms | 78.970703 ms | t0 18 / t1 14 |
| 100% | 72.517813 ms | 3.205705 ms | 0.005000 ms | 0 | 0.010000 ms | 75.738519 ms | t0 128 / t1 0 |

Most of the 5.006 ms endpoint shift is earlier generation of the eventual completing frame
(4.842 ms), not decoder work. Source-to-runtime falls only 0.163 ms. At full overlap, deterministic
tie resolution makes tree 0 the completing tree in every decisive run; that does not imply tree 1
was unused—mean emissions remain split and are recorded in `trials.csv`.

The source-to-runtime term is intentionally aggregate. The current recorder can prove causal
boundaries but cannot uniquely assign overlapping TCP, physical-queue, relay, and mailbox waits.
That limitation remains from W2.

## Rounds under coupling

| Overlap | Protocol | Receiver barrier | Sender completion | Feedback-completion lag | Mean emissions | Positive deficit reports | Deficit total / max |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0% | carousel | 80.744513 ms | 84.108425 ms | 3.363911 ms | 556 | — | — |
| 0% | pooled rounds | 80.673804 ms | 89.295755 ms | 8.621951 ms | 516 | 96 | 447 / 11 |
| 100% | carousel | 75.738519 ms | 79.170925 ms | 3.432406 ms | 562 | — | — |
| 100% | pooled rounds | 75.624292 ms | 84.466081 ms | 8.841788 ms | 515 | 96 | 363 / 4 |

Rounds reaches the receiver barrier 0.071–0.114 ms earlier and emits fewer frames in this no-drop
slice, but its sender finishes 5.187–5.295 ms later. Every rounds execution has three positive
deficit reports because independent control TCP can carry `SourceDone` past data still queued on
the data connections, the W1 falsifier mechanism. Full overlap slightly increases the rounds
feedback lag rather than removing the barrier cost. Carousel spends bandwidth to avoid that wait;
the report does not convert the resulting latency/bandwidth trade into a universal winner.

## Isolated credit on a shared leaf

| Metric (128 paired seeds) | Hybrid drop | Isolated credit | Isolated saving |
|---|---:|---:|---:|
| Barrier completion | 516.364602 ms | 516.364602 ms | 0 |
| Healthy-receiver completion | 282.281414 ms | 276.455413 ms | 5.826001 ms |
| Source emissions | 990 | 990 | 0 |
| Slow-branch deliveries through completion | 968 | 516 | 452 |
| Shared-leaf wire bytes | 2,310,720 B | 1,428,576 B | 882,144 B |

This answers W2 reconsideration condition 2 **yes**: when the slow branch and a healthy branch share
a physical server, keeping obsolete slow-branch traffic off that server benefits the healthy
receiver. It still does not justify production work:

- slow decoder service fixes the barrier, so neither barrier nor sender work improves;
- the model's isolated debt is an optimistic frame-ID obligation, not retained 512-byte payload;
- W2 measured up to 42.16 MiB aggregate real-payload ownership for that abstraction, outside the
  declared buffer budgets; and
- no bounded ownership or shed-and-repair design has been specified. Shedding is already close to
  hybrid drop.

Therefore condition 2 is now supported, condition 1 is not, and the production recommendation
remains **NO-GO on isolated credit; retain hybrid drop**. The saved-wire result is useful input if a
bounded design is proposed later.

## Control asymmetry and liveness

Barrier completion is 98.656202 ms in every control cell because it is receiver-local; the reverse
path controls when the sender learns completion and how much work remains in flight.

| Reverse condition | Cadence | Seeds | Sender completion | Feedback lag | Emissions / tail | ACK probes total | Reverse bytes | Reverse background | Link drops | Stall pressure / margin |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| incast | 0.5x | 32 | 115.184402 ms | 16.528200 ms | 858 / 124 | 1,280 | 120,460 B | 0 | 0 | 4.9% / 95.1% |
| incast | 1x | 32 | 115.434402 ms | 16.778200 ms | 864 / 130 | 1,184 | 66,892 B | 0 | 0 | 4.9% / 95.1% |
| incast | 2x | 32 | 115.634402 ms | 16.978200 ms | 870 / 136 | 1,280 | 40,000 B | 0 | 0 | 4.9% / 95.1% |
| incast + bursts | 0.5x | 128 | 116.295820 ms | 17.639618 ms | 868 / 134 | 5,169 | 139,550 B | 17,334 B | 40 | 4.9% / 95.1% |
| incast + bursts | 1x | 128 | 115.535277 ms | 16.879075 ms | 865 / 131 | 4,736 | 85,102 B | 16,817 B | 0 | 4.9% / 95.1% |
| incast + bursts | 2x | 128 | 115.717970 ms | 17.061768 ms | 870 / 136 | 5,136 | 58,240 B | 16,822 B | 2 | 4.9% / 95.1% |

With incast alone, 0.5x completes 0.25 ms before 1x but almost doubles reverse control bytes. Under
the decisive incast-plus-burst condition, 1x is best: it is 0.761 ms faster than 0.5x and 0.183 ms
faster than 2x, emits the fewest frames, has the smallest tail, and experiences no physical queue
drops. Faster feedback is not free—0.5x creates more ACK/probe load and competes with itself. Slower
feedback reduces reverse bytes but leaves more source work in flight. The existing 1x timing is the
best balance in this slice.

The application-drop totals are 59,904–60,672 over each 32-seed incast group and
241,816–242,688 over each 128-seed burst group. These are deliberate hybrid receiver-boundary
drops after reliable TCP delivery, not transport loss, and carousel replaces the resulting DoF.
The 40 and 2 physical queue drops in the burst 0.5x and 2x groups are recovered by TCP. No session
approaches abort: maximum no-progress age is 49/1,000 of the two-second stall timeout, and heartbeat
traffic keeps the independent silence clock live.

## Determinism, artifacts, and reproduction

All arithmetic timestamps and counters are integers. Every simulated execution uses one nexosim
worker; the CLI's eight host workers run independent simulations and do not alter event ordering.
The counter PRF is keyed by scenario and master seed with component domains. Local same-time
decisions use the W0a one-nanosecond deferred rule and stable keys. The W3 gate reverses model
registration and requires an identical full event CSV.

Committed artifacts live in [`results/wansim/w3-coupling/`](../results/wansim/w3-coupling/):

- `trials.csv` and `summaries.csv`: main and rounds executions;
- `advantage-decomposition.csv`: exact pooling/path-diversity split;
- `a4-correlation.csv`: primary and secondary coupling statistics;
- `critical-paths.csv`: 4,032 exact receiver decompositions;
- `flow-counts.csv`: stage capacity, queue sharing, and flow-count controls;
- `isolated-credit-{trials,summary}.csv`: the single reconsideration slice;
- `control-asymmetry-{trials,summary}.csv`: incast, burst, cadence, and liveness results; and
- `SHA256SUMS`: digests for all ten CSVs.

Exact full-sweep command:

```sh
cd /Users/winifred/nextmini-perfect-fec/wansim
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo run --release --bin w3_coupling -- \
  32 128 8 ../results/wansim/w3-coupling
```

Digest verification:

```sh
cd /Users/winifred/nextmini-perfect-fec/results/wansim/w3-coupling
shasum -a 256 -c SHA256SUMS
```

The authoritative run took 140.90 s wall time (1,027.75 s user, 30.54 s system), with macOS
`time -l` reporting 1,503,150,080 maximum resident bytes and 814,175,296 peak memory footprint. A
fresh second full run took 134.96 s; all ten CSVs compared byte-for-byte identical to the committed
artifacts. There are 1,344 main/rounds rows, 256 isolated-credit rows, 480 control rows, and 4,032
critical-path rows, excluding headers.

## Validation and gates

W3 adds gates that prove:

- the 0/25/50/100 masks have exact quarter-stage cardinality while capacity and active flow count
  remain constant;
- bulk and bounded heavy-tailed traffic are actual TCP byte streams with reverse ACK traffic;
- the matched single-tree candidate uses the already-existing second foreground TCP connection;
- repeat execution is byte-identical and reverse registration is metamorphically identical;
- the eight receiver controls and reverse burst share the configured TCP path;
- both admission policies traverse the harsh shared leaf; and
- experiment workers reject binding nexosim mailboxes. Main-matrix high-water is at most 108/256.

Final gates:

| Gate | Command | Result |
|---|---|---|
| Wansim format | `cd wansim && CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo fmt --check` | Green |
| Wansim lint | `cd wansim && CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo clippy --all-targets -- -D warnings` | Green |
| Wansim conformance | `cd wansim && CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run` | 89 passed, 0 skipped |
| Artifact digests | `cd results/wansim/w3-coupling && shasum -a 256 -c SHA256SUMS` | 10/10 OK |
| Independent full reproduction | Full release command to `/tmp/w3-coupling-repro`, then `cmp` all CSVs | 10/10 byte-identical |
| Root regression | `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run` | 828 passed, 17 existing skipped |

## Honest limitations and surprises

- This is model-level causal evidence, not a WAN measurement. No claim in this report uses the word
  “twin”; W4 must calibrate and blindly validate the model before that term is defensible.
- The main overlap axis covers the two trees' root physical path. Downstream overlay links are
  edge-disjoint. The separate shared-leaf slice tests the receiver-branch mechanism; it is not a
  full arbitrary physical topology sweep.
- The background distribution is deterministic, seeded, and bounded. It is useful for burst
  mechanism tests but is not fitted to Arbutus traffic and must not be called an empirical WAN
  workload.
- The best-single baseline's second connection deliberately carries non-useful matched payload.
  This controls TCP flow count and congestion competition, but represents the cost of reserving the
  second flow rather than an operator who would remove it entirely.
- The ideal DoF bucket has no decoding failure, rank deficiency, CPU, allocator, disk, or sink
  variance. Node service is fixed and serial; host scheduling and shared CPU are absent.
- Physical queues can drop packets and days-derived TCP retransmits them. The main matrix happened
  to have no such drops; the control burst slice did. Receiver hybrid drops happen only after TCP
  acknowledgment and are a separate application event.
- More overlap improved absolute completion because the shared 160 Mbit/s server statistically
  multiplexed bursty flows better than two fixed 80 Mbit/s partitions. This was not preordained by
  “overlap” and should be treated as a topology/workload interaction, not a general law.
- The pooling advantage is real but much smaller than the path-diversity advantage in this matrix.
  A paper claim that combines them would be misleading.

## Commits

| Commit | Concern |
|---|---|
| `d2e21fd` | Coupled physical paths, explicit TCP background flows, shared control/leaf paths |
| `f3d16a9` | Deterministic W3 harness, advantage/A4/attribution slices, W3 gates |
| `c2014fa` | Rounds feedback-completion and deficit accounting |
| `64ca962` | Committed W3 CSV artifacts and SHA-256 manifest |
| this commit | W3 report and experiment-ladder synthesis |

## What wansim established overall, W0a–W3

This is the experiment-ladder summary intended for a future paper appendix. Every statement below
is model-level evidence unless it names a structural test.

**Foundation (W0a).** A pinned, locally vendored days/nexosim fork can express nextmini's reliable
buffer chain after adding explicit send/receive socket flow control, advertised windows, read
credit, persist behavior, and the cumulative-ACK gap fix. The single chain reached the exact
five-owner backpressure plateau of 5,120 bytes. At 1 Mbit/s it serialized a 552-byte segment every
4,416,000 ns, recovered deterministic loss without an application gap, and remained byte-stable
under repeat and registration-order tests. This retired the foundational “can the engine model
reliable backpressure deterministically?” risk.

**Fan-out and admission (W0b).** The receiver-covering tree reproduced the exact 12,288-byte
resident-copy plateau without double counting. Sequential child admission did not block later
children until the first child's real 3,072-byte downstream chain filled; once full, healthy
receivers completed at 258.742001 ms instead of 82.598002 ms under concurrent admission. Hybrid
drop was pinned as an application-boundary event after a covering hop TCP ACK, with the control lane
exempt. This validated the substrate on which the protocol comparisons run and showed that
configured child order is a causal mechanism.

**Independent protocol falsifier (W1).** Strict no-drop transport did not imply zero rounds
deficits: `SourceDone` on a separate control TCP connection overtook queued data in every rounds
trial. That was a real cross-connection ordering mechanism, not simulated loss. Homogeneous local
completion matched; on crossed paths carousel beat the best striped baseline by 1.642–1.795 ms.
At K=64 its 20–28-frame ACK-flight tail was 31.25–43.75%, so W1 explicitly rejected calling it
small. The independent section-P endpoints passed join, deadline, frozen-peer, liveness, and
work-conservation tests.

**Stragglers and admission policy (W2).** Naive blocking is untenable: one 1/10x receiver imposed up
to 387.090 ms on healthy siblings despite zero byte loss. Hybrid drop and optimistic isolated
credit eliminated that externality in every tested cell. K=512 reduced the matched ACK-tail
fraction from 35.46% to 14.13%. Isolated credit sometimes improved moderate cells, but tied hybrid
in the decisive edge-disjoint straggler cells and required unbounded real-payload ownership (up to
42.16 MiB aggregate). The resulting production recommendation was NO-GO, with two explicit
reconsideration conditions: bounded ownership and evidence that saved branch traffic matters on a
shared bottleneck.

**Coupling and feedback (W3).** Physical sharing drove the primary delivered-rate correlation to
0.968, falsifying exogenous-rate assumption A4 in the fully overlapped model. The speedup
decomposition showed only 0.31–1.10% pooling advantage but 47.38–47.80% path-diversity advantage.
Putting the slow and healthy branches on a shared bottleneck made W2 condition 2 true: isolated
credit saved 882 KiB and 5.826 ms of healthy completion. Condition 1 remains open, so the NO-GO
stands. Eight-way reverse incast and bursts left at least 95.1% stall-budget margin, and the existing
BlockAck cadence was the best tested burst compromise.

**Overall production reading.** The ladder supports the branch's carousel, cumulative BlockAck,
hybrid drop-after-TCP, and sequential-order semantics as internally coherent and causally
observable. It strongly rejects replacing hybrid drop with naive blocking. It does not yet support
adding isolated credit, claiming that all two-tree speedup comes from pooled FEC, or treating tree
rates as independent under shared physical bottlenecks. It also does not establish WAN effect
sizes. That last step is W4: calibrate protocol-independent probes against ns-lossless and Arbutus,
then make blind predictions before calling wansim a digital twin.

W3 closes the planned experiment ladder. No W4 calibration work was started.
