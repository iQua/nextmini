# Cloudcast POLICY on wansim transport: completion-time comparison (v2)

Date: 2026-07-20

Branch: `perfect-fec-runtime`

Status: final Tier-1 result. This v2 analysis supersedes the v1 headline in commit `4b335cb`.

Evidence revisions: `bcb433f` (throughput frontier and shared-topology harness) and `b218e6c`
(results and digests).

Evidence class: **model-level algorithm evidence, not a WAN measurement and not a
system-level execution of the native Cloudcast/Skyplane artifact**.

## Verdict

This report is deliberately scoped to a **fixed-resource, fixed-topology, completion-time-only
comparison**. Cloudcast natively minimizes deployment and transfer cost subject to a completion
budget. That broader objective is not evaluated here. Instead, the experiment asks the direct
Prop-2 question: when every arm receives the same two trees, actors, links, queues, background
traffic, and transport, does pooled-FEC carousel complete faster than Cloudcast's finite ownership
striping?

Yes, in this Tier-1 model slice:

- At 30% background load, carousel reduced mean receiver-barrier completion from 741.878 ms to
  696.117 ms: **45.761 ms, or 6.17%**.
- At 70% background load, carousel reduced it from 747.229 ms to 696.580 ms: **50.648 ms, or
  6.78%**.
- Carousel beat Cloudcast POLICY on every matched seed: **32/32**.
- The paired per-seed advantage was 45.761 [25.385, 76.209] ms at 30% and
  50.648 [26.519, 82.520] ms at 70%, reported as mean [nearest-rank P5, P95].

The earlier 17.8% result is retired. Its one-second deadline let Cloudcast select a single tree,
and only the Cloudcast arm received that selected placement. It therefore compared carousel's
two-tree race against a non-racing baseline on a different topology. V2 removes both confounders.
The corrected claim is a smaller but consistent **6.2--6.8% completion-time gain** for pooling
over finite stripe ownership in this model and envelope.

## Fixed comparison scope

Every execution uses the existing representative `digitalocean-like/west-origin` envelope:

- sender region `sfo3` and receivers `nyc3`, `fra1`, and `sgp1`;
- background utilization `{30%, 70%}` with seeded propagation jitter enabled;
- `K=8192`, 508-byte payloads, and 4,161,536 logical source bytes;
- 16 coupled seeds per executable variant;
- hybrid receiver admission with no configured straggler;
- the W0b hop-wise reliable TCP substrate, finite queues, and sequential relay admission; and
- one fixed VM/NIC service resource per used region.

The exact small-scale planner first searches the same six-region resource graph over 20 executable
two-relay receiver-covering trees, every ordered pair, and every split of eight equal stripes:
3,460 assignments per utilization. It computes the exact cost-under-deadline policy frontier, but
the comparison selects its **first feasible point**, i.e. the globally minimum estimated
completion budget. This is equivalent to tightening Cloudcast's completion constraint to the
throughput frontier within the executable tree family.

The selected two-tree placement is then frozen and applied to **all five execution variants**:
Cloudcast POLICY, carousel, per-stripe FEC, and both single-tree candidates. Background flows are
anchored only after this placement is applied. Thus every reported arm sees the same physical and
overlay resource envelope. `best-single` executes both selected trees on each seed and reports the
faster completion, making it an oracle-strong single-tree baseline.

The protocol semantics remain distinct:

- **Cloudcast POLICY on wansim transport:** eight finite equal stripes, pure replication, no FEC,
  no repair, and exactly one owner tree per stripe.
- **Pooled-FEC carousel:** interchangeable innovative DoF across both trees with cumulative
  BlockAck completion.
- **Per-stripe FEC:** independent continuous FEC stream and DoF bucket per owned stripe.
- **Best-single:** the faster of the two one-tree executions for each matched seed.

## Completion-budget frontier and selected plan

The table records the exact policy transitions as the deadline is relaxed. Dollar objective values
are intentionally omitted: this report makes no cost comparison. “Estimate” is the deterministic
planner estimate, not the DES outcome. A relay pair `A -> B` denotes the two relay positions in the
fixed W0b caterpillar tree.

| Load | First-feasible budget | Tree 0 relays | Tree 1 relays | Stripe split | Planner estimate | Used in comparison |
|---:|---:|---|---|---:|---:|:---:|
| 30% | 152.292288 ms | `nyc3 -> tor1` | `lon1 -> fra1` | 4 / 4 | 152.292288 ms | Yes |
| 30% | 201.450515 ms | inactive | `nyc3 -> fra1` | 0 / 8 | 201.450515 ms | No |
| 70% | 188.358934 ms | `nyc3 -> tor1` | `lon1 -> fra1` | 4 / 4 | 188.358934 ms | Yes |
| 70% | 243.358934 ms | `nyc3 -> sgp1` | `lon1 -> fra1` | 4 / 4 | 243.358934 ms | No |
| 70% | 260.698667 ms | `nyc3 -> sgp1` | `lon1 -> fra1` | 5 / 3 | 260.698667 ms | No |
| 70% | 278.038400 ms | `nyc3 -> sgp1` | `lon1 -> fra1` | 6 / 2 | 278.038400 ms | No |
| 70% | 280.717867 ms | inactive | `nyc3 -> fra1` | 0 / 8 | 280.717867 ms | No |

The selected point is a genuine two-tree race at both loads: four stripes and 4,096 source symbols
per tree. At 30%, a budget of 152,292,287 ns is infeasible; at 70%, 188,358,933 ns is infeasible.
The selected budget is therefore not an arbitrary forced split: it is the minimum feasible point
of the exact reduced-family optimizer. The same selected relays arise at both loads.

This is the throughput-oriented use of the paper's stripe-tree decision space. It preserves the
paper's finite equal-stripe ownership semantics, but it does not claim to reproduce Cloudcast's
full VM-count or monetary optimization. The relevant native formulation is in Sections 3.3.1--3.3.3
of the [Cloudcast paper](https://www.usenix.org/system/files/nsdi24-wooders.pdf); the larger-scale
approximation is discussed in Section 3.4.

## Completion-time results

The simulator warms background flows for one simulated second. Committed CSV timestamps are
absolute; all values below subtract that 1,000,000,000 ns warm-up and are foreground durations.
Values are mean [nearest-rank P5, P95] over 16 seeds.

| Load | Arm | Receiver barrier | Sender completion |
|---:|---|---:|---:|
| 30% | Cloudcast POLICY on wansim transport | 741.878 [731.615, 755.224] ms | 794.471 [784.848, 808.790] ms |
| 30% | Pooled-FEC carousel on wansim transport | **696.117 [668.352, 710.060] ms** | **756.780 [726.967, 771.657] ms** |
| 30% | Per-stripe FEC on wansim transport | 756.523 [744.941, 772.996] ms | 809.159 [796.118, 826.383] ms |
| 30% | Best-single tree on wansim transport | 839.942 [808.268, 852.641] ms | 892.697 [859.597, 905.809] ms |
| 70% | Cloudcast POLICY on wansim transport | 747.229 [736.069, 760.631] ms | 800.161 [789.150, 816.885] ms |
| 70% | Pooled-FEC carousel on wansim transport | **696.580 [667.154, 710.059] ms** | **757.697 [725.767, 771.657] ms** |
| 70% | Per-stripe FEC on wansim transport | 957.296 [740.752, 2,422.386] ms | 1,005.849 [795.500, 2,454.178] ms |
| 70% | Best-single tree on wansim transport | 839.927 [808.261, 852.638] ms | 892.820 [859.590, 907.073] ms |

The matched Cloudcast-to-carousel differences are the load-bearing comparison:

| Load | Receiver-barrier reduction | Relative reduction | Paired P5--P95 | Carousel wins | Sender-completion reduction |
|---:|---:|---:|---:|---:|---:|
| 30% | 45.761 ms | 6.17% | 25.385--76.209 ms | 16/16 | 37.691 ms / 4.74% |
| 70% | 50.648 ms | 6.78% | 26.519--82.520 ms | 16/16 | 42.464 ms / 5.31% |

Carousel is the fastest arm by both receiver barrier and sender completion at both loads. Cloudcast
POLICY is second at both loads. The finite Cloudcast arm emitted exactly `K` and had no receiver
application drop or physical-queue drop in all 32 trials.

At 70%, per-stripe FEC has a long P95 tail. Its continuous independent streams produced 14,724
aggregate physical queue drops across the 16 trials; TCP recovered them, and receiver application
drops remained zero. Carousel encountered 4,111 physical queue drops at that load but retained a
tight completion distribution. This is useful mechanism evidence, not a general claim that
per-stripe FEC always has a heavy tail.

## Interpretation of the Prop-2 comparison

The experiment isolates ownership from topology:

1. Cloudcast divides the object into eight owned stripes. Four must complete over each selected
   tree, and a receiver cannot substitute surplus delivery on one tree for a missing stripe on the
   other.
2. Carousel places every innovative delivery into one receiver-wide DoF bucket. Faster service on
   either tree can replace slower service on the other until rank reaches `K`.
3. Because both protocols use exactly the same selected trees and resources, placement quality and
   path diversity are controlled rather than folded into the pooling number.

The 32/32 paired wins support the predicted pooling advantage on these stochastic sample paths.
The magnitude is modest: about 6--7%, not a topology-independent constant. This agrees with the
earlier mechanism ladder: pooling gains grow when path service diverges over time, while healthy
near-symmetric service leaves less ownership waste to recover.

The paper-safe claim is:

> In the deterministic wansim cloud envelope, with a fixed resource allocation and the exact same
> two Cloudcast-selected trees, pooled-FEC carousel reduced mean receiver-barrier completion by
> 6.2--6.8% relative to Cloudcast's finite striped replication policy across 32 matched trials.

Claims that are **not** supported include beating native Cloudcast end to end, improving its full
cost-aware objective, reproducing a public cloud, or obtaining a universal 6--7% gain.

## Implementation fidelity and deviations

The arm retains the policy properties relevant to this comparison:

- eight equal finite stripes, each assigned to one receiver-covering tree;
- exact checked resource demand over node NICs and physical trunks;
- a completion-budget constraint and deterministic tree/stripe selection;
- pure replication with no erasure coding or online rerouting; and
- full-object completion only after every owned stripe completes at every receiver.

The deliberate deviations are unchanged except for the throughput-frontier scoping:

- Wansim searches the 20 two-relay caterpillar trees its actors can execute, not every directed
  tree in Cloudcast's 71-region formulation.
- Exact enumeration replaces the paper's MILP/large-region approximation at six-region scale.
- VM count, VM startup, VM price, object-store I/O, and Skyplane gateway process behavior are
  normalized away.
- All arms use wansim's same hop-wise TCP implementation, queues, and control paths rather than
  native Skyplane transport.
- The first feasible deadline point is selected to study maximum completion performance under
  fixed resources. This is not presented as Cloudcast's normal cost-minimizing operating point.

These choices make the result an algorithm-level ownership comparison. They do not make it a
native Cloudcast system benchmark.

## Limitations

- Only one representative profile, one placement, two offered loads, and 16 seeds were authorized.
  P5 and P95 are coarse nearest-rank order statistics at this sample count.
- The 4.161536 MB object is far smaller than Cloudcast's 100 GB paper experiments. Fixed
  propagation and framing are proportionally larger here.
- The scenario uses representative cloud magnitudes, not measured DigitalOcean paths or provider
  guarantees.
- Carousel and per-stripe FEC use ideal innovative-DoF semantics. Real codec CPU, rank defects,
  storage behavior, and decoder scheduling are outside this simulator.
- The Cloudcast POLICY arm does not model Skyplane object-store reads/writes, multipart chunks,
  HTTP registration, pools of native gateway processes, TLS, or VM provisioning.
- Background traffic, TCP, queueing, and jitter are modeled, but application CPU contention and
  real kernel scheduling are not.
- Continuous FEC is unpaced in this scenario. Its traffic pressure and tail must not be generalized
  to every production pacing configuration.
- Raw CSVs retain previously established wire/cost-accounting columns for schema compatibility.
  They are not analyzed or used by this completion-time report.

## Artifacts and reproduction

Committed v2 artifacts live in
[`results/wansim/cloudcast-comparison-v2/`](../results/wansim/cloudcast-comparison-v2/):

- `trials.csv`: 128 reported rows, four arms x two loads x 16 seeds;
- `summaries.csv`: the eight completion-time aggregate rows used above;
- `cloudcast-policy-frontier.csv`: all seven exact deadline-to-plan transitions;
- `sharing-structure.csv`: physical-resource flow counts for all five execution variants;
- `failures.csv`: empty;
- `comparison-v2-cells/`: 160 durable execution shards plus the experiment manifest;
- `CELL-SHA256SUMS`: 481 per-cell/manifest file digests; and
- `SHA256SUMS`: five aggregate CSV digests plus the cell-digest manifest.

All **481/481** cell digests and **6/6** top-level digests verify. A resume pass skipped all 160
terminal cells and reproduced the same five aggregate hashes byte-for-byte.

Authoritative Boston command:

```sh
cd /home/xindan/wansim-cloudcast-bcb433f
source "$HOME/.cargo/env"
CARGO_INCREMENTAL=0 cargo +1.96.0 run --release \
  --bin cloudcast_comparison -- 16 20 results-v2
```

Resume command:

```sh
target/release/cloudcast_comparison 16 20 results-v2 --resume
```

The release build took 13.46 seconds. The complete command took 10 minutes 16.61 seconds including
that build, with a maximum observed RSS of 6,530,344 KiB and no swap.

## Gates and commits

| Gate | Result |
|---|---|
| Boston durable execution | 160 success, 0 failure |
| Durable resume | 160 skipped, 0 rerun, aggregate hashes unchanged |
| Digest verification | 481/481 cell files and 6/6 top-level entries green |
| Wansim `cargo fmt --all --check` | Green |
| Wansim `cargo clippy --all-targets -- -D warnings` | Green |
| Wansim `cargo nextest run` | 123 passed, 1 existing manual probe skipped |

| Commit | Concern |
|---|---|
| `bcb433f` | Add the exact deadline frontier, select its fastest two-tree point, and apply that topology identically to every arm |
| `b218e6c` | Commit all 160 v2 shards, aggregate CSVs, and digest manifests |
| this document | Replace the v1 cost/performance headline with the scoped completion-time verdict |

No new profile, larger `K`, straggler arm, native Cloudcast deployment, production change, or
follow-on experiment was started.
