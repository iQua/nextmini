# Wansim WR Tier-1 realistic-cloud decision slice

Date: 2026-07-18

Branch: `perfect-fec-runtime`

Evidence revision: `f937f62` (emission semantics) plus `8e6d525` (the frozen Tier-1 task set)

Evidence class: **model-level evidence from an ideal-DoF discrete-event model, not a WAN
measurement and not a reproduction of DigitalOcean**.

## Executive verdict

The reduced Tier-1 run completed **224/224 cells successfully with zero protocol or harness
failures**. The toy-world mechanism story survives, but only in a qualified form:

1. Carousel did not materially change the healthy receiver barrier relative to pooled rounds, but
   it learned sender completion 354--429 ms sooner by avoiding rounds feedback barriers.
2. Pooled FEC saved 27.2--29.8 ms over per-stripe FEC. Path diversity saved a further
   112.8--115.4 ms and accounted for 79--81% of the total two-tree advantage.
3. Cap-free hybrid stragglers completed in every seed with at least 98.716% liveness-budget margin,
   including the formerly failing seed 15. The cost was severe: mean source emissions were
   39.94 times K and P95 was 65.30 times K in the unpaced model.
4. A4 is not defensible as an exogenous-independent-rate assumption in this placement. Within-run
   tree-rate correlation was strongly positive at 30% offered load and negative at 70%; even its
   sign is regime-dependent.
5. The production 1x BlockAck cadence was safe but not fastest here. The 0.5x arm learned sender
   completion 3.97 ms sooner, while all three arms had indistinguishable receiver barriers and
   emission counts. W3's 1x optimum under reverse incast does not generalize to this slice.
6. No liveness abort occurred. The smallest margin was 84.609% in pooled rounds; carousel retained
   at least 98.716% margin, including the straggler slice.

These are **selected-slice verdicts**, not holds-across-cloud-envelope verdicts. The explicit user
simplification canceled other providers, placements, 50% load, jitter-off, K=65,536, concurrent
sessions, blocking admission, pacing, Tier 2, and W4. The wansim effort closes with this report.

## Scope and method

The authoritative task set was:

| Slice | Cells |
|---|---:|
| Main: utilization {30%, 70%} x five executable protocol arms x 16 seeds | 160 |
| Hybrid-drop straggler at 70% x 16 seeds | 16 |
| BlockAck cadence {0.5x, 1x, 2x} at 70% x 16 seeds | 48 |
| **Total** | **224** |

The five main arms are carousel, pooled rounds, per-stripe FEC, and both candidate single trees.
The reported `best-single` value selects the faster candidate on each coupled seed, so it is an
oracle-strong baseline. Every arm uses the same seed for background transitions, propagation
jitter, payload identity, and tie resolution; TCP paths then evolve endogenously from each
protocol's queue occupancy.

All cells use the representative `digitalocean-like/west-origin` scenario: sender `sfo3`, receivers
`nyc3`, `fra1`, and `sgp1`, and distinct relay regions where placement permits. The synthesized
inputs include 2 Gbit/s VM NIC caps, directed 800 Mbit/s trunks, finite 3 MiB NIC and 6 MiB trunk
queues, asymmetric inter-region propagation, seeded +/-5% temporal jitter in 100 ms epochs, and
explicit bulk plus bounded heavy-tailed TCP background flows. The shared NA-west/NA-east trunks
each carry 12 modeled flow directions: four foreground and eight background. Sharing emerges from
routing; no overlap percentage is an input.

These values are representative model inputs, not measurements, guarantees, or inferred provider
topology. Public sources establish only region identities and broad networking context:

- [DigitalOcean regional availability](https://docs.digitalocean.com/platform/regional-availability/)
- [DigitalOcean Droplet network limits](https://docs.digitalocean.com/products/droplets/details/limits/)
- [AWS regions](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html)
  and [EC2 network bandwidth](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html)
- [Google Cloud locations](https://cloud.google.com/about/locations),
  [VM bandwidth](https://docs.cloud.google.com/compute/docs/network-bandwidth), and
  [Network Service Tiers](https://docs.cloud.google.com/network-tiers/docs/overview)

The transport is the vendored days-derived window-scaled Reno model with persistent independent
TCP state per overlay hop, finite socket buffers, advertised windows, retransmission, relay frame
assembly, and sequential child admission. Data uses 508-byte symbols plus the four-byte logical
length prefix and modeled TCP/IP serialization overhead. Control uses independent TCP connections
over the same physical resources. The protocol endpoints independently implement the normative
[section-P state machines](perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages).
The codec is deliberately abstract: rank is `min(K, distinct innovative deliveries)` at K=8,192.

Each simulation uses one nexosim worker; 20 host workers run independent cells. The authoritative
Boston execution began at 02:06:56 EDT and was observed complete by 03:03:47 EDT, about 56 minutes
51 seconds. Observed aggregate RSS peaked near 15.2 GiB while many straggler cells overlapped; it
fell as their retained event records were reduced and released.

## 1. Pooled rounds versus carousel

Positive time gaps mean rounds finished later. All values below are paired over 16 seeds.

| Offered load | Receiver barrier gap, mean [P5, P95] | Sender gap, mean [P5, P95] | Carousel emissions | Rounds emissions | Positive rounds deficits |
|---:|---:|---:|---:|---:|---:|
| 30% | -0.009 ms [-0.011, -0.005] | +353.591 ms [159.129, 468.220] | 139,335 | 57,671 | 208 |
| 70% | -0.026 ms [-0.311, +0.001] | +428.878 ms [178.165, 546.167] | 91,438 | 57,332 | 202 |

Rounds was microscopically earlier at the receiver barrier, which is operationally a tie beside
the 1.706 s mean barrier. Carousel finished at the sender in 1.767 s; rounds took 2.121 s at 30%
and 2.196 s at 70%. This is the section-P mechanism: rounds waits for deficit feedback barriers,
while cumulative BlockAck lets carousel finish as soon as every frozen peer's joined progress is
complete.

The latency result is not free. `carousel_tail_emissions_mean`, defined as emissions minus K, was
131,143 at 30% and 83,246 at 70%. No receiver application drops occurred in these healthy cells;
the extra work is predominantly unpaced work-conserving departure and feedback flight. Therefore
the selected slice supports the sender-completion mechanism, not a claim that carousel minimizes
bytes. It also shows why a future production study must qualify the optional pacer's rate and
burst before treating these emission counts as a deployment prediction.

**Verdict:** the rounds-to-carousel sender benefit holds at both selected loads; receiver-barrier
benefit does not. The size of the gain and its bandwidth price are regime-dependent.

## 2. Pooling advantage versus path-diversity advantage

The additive identity is the W3 definition:

```text
pooling advantage        = B(per-stripe FEC) - B(carousel)
path-diversity advantage = B(best single tree) - B(per-stripe FEC)
total advantage          = B(best single tree) - B(carousel)
```

It held exactly for every paired seed.

| Offered load | Carousel | Per-stripe | Best single | Pooling, mean [P5, P95] | Path diversity, mean [P5, P95] | Share of total: pooling / diversity |
|---:|---:|---:|---:|---:|---:|---:|
| 30% | 1,706.375 ms | 1,733.561 ms | 1,848.946 ms | 27.186 ms [20.160, 31.503] | 115.385 ms [111.475, 118.757] | 19.1% / 80.9% |
| 70% | 1,706.711 ms | 1,736.542 ms | 1,849.319 ms | 29.830 ms [27.104, 32.864] | 112.778 ms [105.754, 117.217] | 20.9% / 79.1% |

Pooling reduces the carousel baseline by 1.59--1.75%; path diversity contributes another
6.61--6.76%. The total best-single gap is 142.6 ms, or 8.36% of carousel completion. This is larger
pooling share than W3's healthy toy topology, but the ordering is the same: most two-tree benefit
comes from path diversity, and combining both mechanisms into one “FEC speedup” number would be
misleading.

**Verdict:** the decomposition survives the selected cloud-like placement. Pooling is real and
secondary; path diversity remains dominant.

## 3. Hybrid straggler and the removed emission ceiling

All 16 hybrid-drop straggler cells completed. There were no blocking waits and no liveness aborts.

| Metric | Mean | P5 | P95 |
|---|---:|---:|---:|
| Receiver barrier | 8.918 s | 8.409 s | 10.242 s |
| Sender completion | 8.956 s | 8.449 s | 10.285 s |
| Source emissions | 327,176 | 101,450 | 534,926 |
| Emissions / K | 39.94x | 12.38x | 65.30x |
| Aggregate application drops | 377,498 | 97,563 | 645,208 |
| Stall-budget consumption | 0.707% mean | 0.471% | 1.284% |

Healthy siblings completed only 12.801 ms later on average than their coupled all-healthy cells;
hybrid drop continued to isolate receiver service pressure. The slow receiver, however, converted
that isolation into wire work: mean emissions were 3.58 times the already-unpaced healthy-carousel
mean and almost 40 times K. Aggregate drops can exceed source emissions because the same logical
departure can be delivered and dropped at more than one receiver boundary.

Fourteen of 16 seeds exceeded the old 65,536-frame limit on at least one tree, and 11 exceeded its
old two-tree total. The formerly fatal seed 15 completed at an 8.431 s barrier and 8.468 s sender
time after 521,629 emissions; its busiest tree emitted 449,139 frames. Its stall consumption was
only 0.518%. This directly confirms the triage: the old K*8 per-tree stop manufactured the prior
no-progress failure. Across the slice, the maximum single-tree count was 450,558, and the maximum
total was 534,926.

The cap-free behavior is protocol-correct for an unpaced work-conserving carousel but economically
harsh. It is replacement traffic plus queued/feedback-flight work, not a small “ACK tail.” The
user explicitly canceled the paced arm, so this report does not estimate how production pacing
would change the result and does not recommend a new pacer default.

The reduced scope also canceled the naive-blocking arm. Consequently WR does not independently
re-estimate W2's hybrid-versus-blocking delta; W2's finding that blocking creates healthy-receiver
externality remains the available evidence.

**Verdict:** hybrid carousel completes and stays live in this harsh slice, including the original
failure seed, but its unpaced emission cost is a first-order limitation rather than a footnote.

## 4. A4 delivered-rate correlation

| Offered load | Cross-seed total-rate correlation | Within-trace mean | P5 | P95 |
|---:|---:|---:|---:|---:|
| 30% | 0.000 | +0.587 | +0.252 | +0.755 |
| 70% | -0.179 | -0.239 | -0.342 | -0.088 |

At 30%, both trees tend to move together through shared resources. At 70%, competition makes their
short-window rates anti-correlated. A zero cross-seed statistic at 30% is not evidence of
independence: the within-run time series is strongly correlated, and the physical routes share
explicit finite queues and TCP flows. The sign reversal is the useful result: coupling is
endogenous and load-dependent, so a single fixed correlation correction cannot rescue A4.

**Verdict:** A4's exogenous-independent-rate form fails in this placement. A weaker sample-path
claim remains the appropriate theory interface.

## 5. BlockAck cadence

| Cadence | Receiver barrier | Sender completion | Emissions | Stall P95 / margin |
|---:|---:|---:|---:|---:|
| 0.5x | 1,706.712 ms | 1,763.453 ms | 91,438 | 0.920% / 99.080% |
| 1x | 1,706.711 ms | 1,767.420 ms | 91,438 | 0.969% / 99.031% |
| 2x | 1,706.712 ms | 1,775.469 ms | 91,439 | 0.989% / 99.011% |

Receiver completion differs by less than one microsecond. Relative to 1x, 0.5x learns sender
completion 3.967 ms sooner and 2x learns it 8.049 ms later. Unlike W3's reverse-incast-plus-burst
slice, this three-receiver placement does not make faster feedback self-congest enough for 1x to
win. WR does not export reverse control-byte counts, so this latency table alone cannot justify
changing the production default.

**Verdict:** all cadences are safe; the “1x is optimal” claim is regime-dependent. Keep the current
default absent a deployment-calibrated control-cost study.

## 6. Liveness margins

Every cell terminated normally; `failures.csv` is empty.

| Slice/protocol | Maximum stall-budget consumption | Minimum margin |
|---|---:|---:|
| Main carousel | 0.969% | 99.031% |
| Hybrid straggler carousel | 1.284% | 98.716% |
| Cadence sweep | 0.989% | 99.011% |
| Main per-stripe FEC | 12.088% | 87.912% |
| Main pooled rounds | 15.391% | 84.609% |

No selected cell approached either a liveness abort or the 60 s observation horizon. The former
seed-15 failure was entirely the deleted simulator ceiling, not BlockAck quantization, ACK-path
silence, or a sender join defect.

**Verdict:** section-P liveness defaults have ample margin in this Tier-1 slice. This does not
bound outages, provider pauses, or placements omitted by the simplification order.

## Does the toy-world story survive realism?

**Mostly, with two important corrections.** The independent mechanism claims survive: carousel
removes sender feedback barriers; pooling beats ownership; path diversity supplies most of the
two-tree advantage; hybrid drop keeps healthy siblings isolated; and shared bottlenecks invalidate
exogenous tree rates. But W3's particular 1x cadence optimum is not universal, and an unpaced
work-conserving source can spend 40--65 times K under a slow receiver. The latter is both a modeled
network cost and the source of a highly skewed host runtime in exact segment-level DES.

The production reading is therefore narrow: keep cumulative BlockAck, carousel semantics, and
hybrid drop; do not fold path diversity into an FEC-only claim; do not assume A4 on shared routes;
and do not interpret unpaced WR emission totals as production overhead. No new production change
is authorized by this stage.

## Artifacts, determinism, and reproduction

Committed results live in [`results/wansim/wr-realistic/`](../results/wansim/wr-realistic/):

- `trials.csv`: 224 canonical trial rows;
- `summaries.csv`: 14 grouped rows;
- `advantage-decomposition.csv`, `rounds-vs-carousel.csv`, `a4-correlation.csv`,
  `straggler.csv`, and `cadence.csv`: the six decision views;
- `sharing-structure.csv`: 16 emergent resource-sharing rows;
- `failures.csv`: empty, because all cells succeeded;
- `wr-cells/`: 224 durable per-cell shards plus the checked manifest;
- `CELL-SHA256SUMS`: 673 per-cell file digests; and
- `SHA256SUMS`: the nine result CSVs plus the cell-digest manifest.

Both digest manifests verify completely. A second full sweep and the proposed optimization digest
matrix were explicitly canceled by the simplification order. Determinism remains structurally
covered by the byte-identical repeat and registration-order gates in the W0--W3 suite; this report
does not mislabel a single-run hash as independent reproduction.

Exact authoritative command on Boston:

```sh
cd /home/xindan/wansim-wr-ed31c2a
CARGO_INCREMENTAL=0 PYO3_PYTHON=/usr/bin/python3 \
  target/release/wr_realistic 16 20 /home/xindan/wr-tier1-f937f62
```

Resume command, which schedules only missing cells:

```sh
target/release/wr_realistic \
  16 20 /home/xindan/wr-tier1-f937f62 --resume
```

## Gates

| Gate | Result |
|---|---|
| Tier-1 durable execution | 224 success, 0 failure |
| Result digests | 10/10 top-level and 673/673 cell files verified |
| Wansim `cargo fmt --check` | Green |
| Wansim `cargo clippy --all-targets -- -D warnings` | Green |
| Wansim `cargo nextest run` | 114 passed, 1 existing manual probe skipped |
| Root `cargo nextest run` spot check on Boston/Linux | 826 passed, 0 failed, 17 existing skips |

The historical 828-test spot-check count in W0--W3 was recorded on macOS. The synchronized
Boston source tree enumerates 826 tests on Linux because the dataplane has target-gated local-I/O
modules (for example, non-Linux `local/writer.rs` versus Linux `writer_tso.rs`). The source and
test-file inventories were checked before the run; this is a platform-specific inventory count,
not two failed or silently skipped tests. The authoritative Boston gate above is fully green.

## Commits and closure

| Commit | Concern |
|---|---|
| `f937f62` | Remove the silent K*8 emission ceiling; make explicit guard exhaustion loud |
| `8e6d525` | Freeze the 224-cell Tier-1 queue and persist the first 30 shards |
| `65d3725` | Persist the next shard batch |
| `b249958` | Checkpoint the main-matrix tail |
| `6190ad0` | Persist early straggler and cadence shards |
| `532683b` | Persist the late Tier-1 tail |
| final report commit | Final shards, aggregate CSVs, digests, gate results, and this report |

No Tier 2, W4, simulator optimization, digest-equivalence matrix, or production change was
started. The planned wansim effort is closed after this report unless a future user directive
explicitly reopens it.
