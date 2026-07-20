# Cloudcast POLICY on wansim transport comparison

Date: 2026-07-20

Branch: `perfect-fec-runtime`

Evidence revisions: `13189fa` (planner and protocol arm), `8302639` (cost model and
durable harness), and `e16b946` (results and digests).

Evidence class: **model-level algorithm evidence, not a WAN measurement, not a cloud-billing
estimate, and not a system-level execution of the native Cloudcast/Skyplane artifact**.

## Executive result

The requested Tier-1 comparison completed **160/160 execution cells with zero failures**. The
reported view contains four matched arms over 16 seeds at each of 30% and 70% offered background
load. Two candidate trees were executed for `best-single`; the faster one on each seed was selected,
so that arm is an oracle-strong baseline.

The result is a clean cost/performance tradeoff, not a one-axis winner:

- **Cloudcast POLICY on wansim transport was by far the cheapest arm.** Its modeled foreground
  egress cost was $0.000155329 for the 4.161536 MB logical object at both loads. Carousel cost
  11.94x as much at 30% and 8.87x as much at 70%.
- **Pooled-FEC carousel was the fastest arm.** It reduced receiver-barrier time by 153.134 ms at
  30% and 153.344 ms at 70% relative to Cloudcast POLICY on wansim transport, a 17.82--17.83%
  reduction relative to the latter's foreground duration.
- **Cloudcast POLICY on wansim transport met its one-second completion budget in every seed.** Its
  worst receiver barrier was 877.020 ms after foreground start and its worst sender completion was
  931.282 ms.
- **The exact policy selected one minimum-cost tree for all eight stripes at both loads.** The
  one-second budget did not require path striping in this small-object cell. That is the intended
  cost-under-deadline behavior, but it means this slice validates placement and ownership policy,
  not a multi-tree Cloudcast solution.
- **Carousel dominated per-stripe FEC in this slice:** it was 27.186--29.830 ms faster and very
  slightly cheaper. Best-single was only 10.592--10.732 ms faster than Cloudcast POLICY on wansim
  transport, but cost 8.79--8.99x as much because this existing baseline continuously emits
  per-stripe FEC until feedback rather than sending one finite uncoded copy.

Accordingly, the fair paper statement is: **pooled-FEC carousel buys about 18% lower completion
time than the cost-minimizing Cloudcast policy plan in this selected wansim cell, while spending
about 9--12x its modeled egress dollars.** It would be false to say that carousel simply “beats
Cloudcast” without the cost qualifier.

## Scope and metric interpretation

All cells use the existing representative `digitalocean-like/west-origin` scenario:

- origin `sfo3`;
- receivers `nyc3`, `fra1`, and `sgp1`;
- background utilization `{30%, 70%}`;
- seeded propagation jitter enabled;
- `K=8192`, 508-byte payloads, and therefore 4,161,536 logical source bytes;
- 16 coupled seeds per executable variant;
- hybrid receiver admission with no configured straggler; and
- the W0b hop-wise reliable TCP substrate with finite queues and sequential relay admission.

The simulator warms background flows for one simulated second. The committed CSV stores absolute
simulation timestamps. All completion numbers in this report subtract that 1,000,000,000 ns
warm-up and are therefore foreground transfer durations.

Cost is measured at every foreground overlay hop's source interface. It includes actual modeled
TCP/IP bytes for data, control frames, acknowledgements, and retransmissions, including the
four-byte logical frame prefix. Same-region traffic has a zero price; background flows are not
charged to an arm. `foreground_egress_wire_bytes` includes both priced and zero-priced source-NIC
bytes, while `modeled_egress_nano_usd` applies the directed price matrix only to priced bytes.

Physical queue drops are transport events recovered by the modeled TCP implementation. No
receiver application drop occurred in any comparison cell. Cloudcast POLICY on wansim transport
had zero physical queue drops; continuous FEC arms generated more queue pressure and
retransmission traffic.

## Two-axis results

Values are mean [nearest-rank P5, P95] over 16 seeds. Costs are modeled USD for this one
4.161536 MB logical object.

| Load | Arm | Receiver barrier | Sender completion | Modeled egress cost | Mean emissions |
|---:|---|---:|---:|---:|---:|
| 30% | Cloudcast POLICY on wansim transport | 859.509 [828.533, 874.551] ms | 912.123 [879.861, 928.813] ms | $0.000155329 [$0.000155329, $0.000155329] | 8,192 |
| 30% | Pooled-FEC carousel on wansim transport | 706.375 [678.118, 725.268] ms | 766.963 [736.667, 786.961] ms | $0.001854298 [$0.001577904, $0.001968878] | 139,335 |
| 30% | Per-stripe FEC on wansim transport | 733.561 [705.958, 750.644] ms | 786.251 [756.475, 804.132] ms | $0.001864142 [$0.001577786, $0.001968783] | 138,381 |
| 30% | Best-single tree on wansim transport | 848.917 [818.549, 864.706] ms | 901.559 [869.878, 918.970] ms | $0.001396655 [$0.001281197, $0.001471589] | 77,822 |
| 70% | Cloudcast POLICY on wansim transport | 860.055 [828.533, 877.020] ms | 913.004 [879.861, 931.282] ms | $0.000155329 [$0.000155329, $0.000155329] | 8,192 |
| 70% | Pooled-FEC carousel on wansim transport | 706.711 [678.115, 725.941] ms | 767.420 [737.336, 787.661] ms | $0.001377159 [$0.000926552, $0.001879317] | 91,438 |
| 70% | Per-stripe FEC on wansim transport | 736.542 [705.956, 758.805] ms | 789.380 [756.677, 813.149] ms | $0.001382951 [$0.000950932, $0.001879207] | 91,056 |
| 70% | Best-single tree on wansim transport | 849.324 [818.550, 865.382] ms | 902.192 [869.879, 920.005] ms | $0.001364692 [$0.000777164, $0.001468378] | 75,658 |

The unpaced work-conserving FEC arms emitted substantially more than `K`. Their modeled costs
therefore include the same feedback-flight behavior already documented by the WR report. Higher
background load reduced the opportunities available before feedback completion, so the 70% cells
counterintuitively emitted and cost less while completing at nearly the same barrier. This is a
model-level interaction between unpaced departure, queues, and feedback; it is not a claim that
real cloud congestion lowers a transfer bill. Production's optional pacer is outside this frozen
comparison scope.

The two-axis ratios are:

| Load | Carousel time reduction vs Cloudcast POLICY | Carousel cost / Cloudcast POLICY | Per-stripe cost / Cloudcast POLICY | Best-single cost / Cloudcast POLICY |
|---:|---:|---:|---:|---:|
| 30% | 153.134 ms / 17.82% | 11.938x | 12.001x | 8.992x |
| 70% | 153.344 ms / 17.83% | 8.866x | 8.903x | 8.786x |

Cloudcast POLICY on wansim transport and carousel are both on the observed Pareto frontier:
neither dominates the other on completion and egress cost. Per-stripe FEC is dominated by
carousel in both selected load cells. The best-single point remains a narrow speed/cost tradeoff
against Cloudcast POLICY on wansim transport, not a cost winner.

## Policy implementation and paper mapping

The implementation follows the algorithmic intent of the
[Cloudcast paper](https://www.usenix.org/system/files/nsdi24-wooders.pdf):

- Section 2 and Section 2.1 define multiple receiver-covering stripe trees and per-GB egress cost.
- Sections 3.3.1--3.3.3 select a distribution tree per equal stripe, minimize VM plus path cost,
  and constrain the plan by path/node capacity under a user completion budget.
- Section 3.4 introduces node/hop restrictions and a greedy stripe-iterative approximation for
  the much larger 71-region problem.
- Appendix A.1 uses 8--16 stripes and reports diminishing returns beyond roughly ten; this arm
  uses eight.

At the six-region wansim scale, a heavyweight MILP dependency would add portability risk without
adding solution quality. The planner instead exactly enumerates the reduced executable family:

1. The sender and three receiver regions are fixed by the scenario.
2. Each candidate is the W0b receiver-covering shape `sender -> relayA -> {receiver0, relayB}` and
   `relayB -> {receiver1, receiver2}`.
3. `relayA` and `relayB` are distinct and drawn from the five non-source regions, producing 20
   deterministic candidate trees.
4. The planner enumerates every ordered pair of candidate trees and every `0..=8` split of eight
   equal stripes. Assigning nonzero stripes twice to the identical candidate is eliminated,
   leaving 3,460 evaluated assignments.
5. For each assignment, exact integer resource demand is accumulated over VM NIC and physical
   trunk resources. Trunk capacity is reduced by the configured background-utilization profile;
   the completion estimate is maximum resource serialization plus critical propagation.
6. Feasible plans must fit the one-second foreground budget. The objective minimizes logical
   payload egress cost, followed deterministically by estimated completion, active-tree count,
   candidate IDs, and stripe split.

The selected plan was identical at both loads:

| Load | Estimated completion | Budget | Estimated logical-payload egress | Stripe assignment | Active relays |
|---:|---:|---:|---:|---|---|
| 30% | 201.451 ms | 1,000 ms | $0.000124846 | all 8 stripes to tree slot 1 | `nyc3 -> fra1` |
| 70% | 280.718 ms | 1,000 ms | $0.000124846 | all 8 stripes to tree slot 1 | `nyc3 -> fra1` |

The active tree is `sfo3 -> nyc3`, local fan-out to receiver `nyc3`, then `nyc3 -> fra1`, local
fan-out to receiver `fra1`, and `fra1 -> sgp1`. With a uniform $0.01/GB inter-region magnitude,
this uses three priced inter-region payload traversals. The actual wire-accounted cost was 1.244x
the planner's logical-payload estimate because the runtime result includes framing, TCP/IP,
control, and acknowledgements.

Every Cloudcast POLICY on wansim transport trial emitted exactly `K`, had no application or
physical-queue drop, and used finite ownership striping. There is no erasure coding, repair
symbol, interchangeable DoF, or carousel behavior in this arm.

## Deliberate deviations and repairs relative to the public artifact

This is a faithful **policy reimplementation inside a normalized simulator**, not a claim of
source compatibility with the native artifact. The deviations from the paper are explicit:

- **Tree family:** the paper can select a general directed receiver-covering tree, subject to its
  hop restriction. Wansim enumerates only the two-relay W0b tree shape its actors execute.
- **Tree slots:** wansim exposes two physical tree slots. Multiple paper stripes assigned to one
  slot are aggregated into an equivalent finite source-symbol quota. At `K=8192`, each of eight
  equal stripes owns exactly 1,024 symbols.
- **VM decision:** the paper jointly selects VM counts and includes VM-time cost. Every arm here
  gets one fixed, identical actor/NIC resource at each used region, so VM placement/count and VM
  dollars are normalized away. Reported cost is egress only.
- **Throughput input:** the paper uses profiled region-pair throughput. This planner uses the
  committed scenario's representative NIC/trunk capacities and configured background fraction;
  the subsequent DES, not the planner, resolves TCP competition and jitter.
- **Data plane:** the paper rides Skyplane gateways, object stores, bounded chunk queues, and pools
  of TCP connections. This arm rides wansim's hop-by-hop reliable TCP relay model and 508-byte
  logical frames.

The independent implementation also avoids, rather than reproduces, the defects catalogued in
[`cloudcast-baseline-analysis.md`](cloudcast-baseline-analysis.md):

- the absent throughput CSV is replaced by an explicit checked `CloudScenario`;
- every successful planner call returns a typed validated plan;
- edge/resource capacity is accumulated from the current enumerated route, so there is no stale
  edge variable;
- object size, one-second budget, and eight-stripe geometry are passed explicitly to planner and
  endpoint;
- all five non-source candidate regions are considered deterministically, with no random
  15-region sampling;
- receiver reachability is guaranteed by candidate construction and revalidated before execution;
  and
- exact enumeration removes the Gurobi dependency at this scale.

The missing paper approximation is not reconstructed because it is unnecessary for 20 candidates.
The exact reduced-family search is stronger and easier to audit for this scale, but it does not
claim equivalence to the paper's full-region optimum.

## Representative egress-price model

Prices are scenario inputs expressed as integer nano-USD per decimal GB. They are representative
public magnitudes, not measurements, quotes, invoices, or claims that a provider's private
backbone has the modeled topology.

| Profile | Modeled directed inter-region magnitude | Public basis |
|---|---|---|
| `aws-like` | $0.01/GB between `us-east-1` and `us-east-2`; $0.02/GB for other modeled inter-region pairs | [Amazon EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/) and source-side inter-region charging guidance |
| `gcp-like` | $0.02/GB within North America or Europe; $0.05/GB North America-Europe; $0.08/GB for modeled Asia pairs | [Google Cloud VPC pricing](https://cloud.google.com/vpc/pricing) |
| `digitalocean-like` | flat $0.01/GB numerical magnitude between different regions | [DigitalOcean bandwidth billing](https://docs.digitalocean.com/platform/billing/bandwidth/) publishes $0.01/GiB and no regional variation |

Same-region entries are zero. DigitalOcean's source uses GiB, while the model deliberately uses
one decimal-GB unit for every arm and retains the public `0.01` magnitude. Consequently these
numbers support controlled relative comparisons, not reproduction of a provider bill.

## Fairness statement

The comparison normalizes the following across arms:

- logical object, origin, receivers, physical backbone, finite queues, transport implementation,
  receiver admission, seed, jitter process, and offered background load;
- one modeled VM/NIC service resource per overlay actor;
- actual foreground wire-byte accounting under one directed price matrix; and
- application completion: the barrier is the last receiver to obtain the entire object.

Background flows are anchored to the original west-placement relay roots for every arm. A
Cloudcast-selected relay therefore cannot accidentally move its competing traffic to an easier
trunk. Cloudcast POLICY on wansim transport is allowed to choose relay regions and stripe mapping,
because that placement decision is the policy being evaluated. The other arms retain the existing
WR west-origin placement.

`best-single` executes both candidate tree variants on each coupled seed and reports the faster
barrier; tree 1 won all 32 matched cells. Artificial W3 flow-count matching is disabled, so its
cost and traffic represent one active tree rather than a dummy second stream.

The fairness boundary is equally important: this is an algorithm-level common-transport
comparison. It does not compare native Cloudcast/Skyplane against the Nextmini system, and it does
not include the paper's VM-cost axis. A paper may use these numbers for the placement/striping
policy baseline only if it retains those labels and separately reports or scopes away compute
cost.

## Honest limitations

- Only one representative profile, one placement, two loads, and 16 seeds were authorized. P5
  and P95 are coarse nearest-rank order statistics at this sample count.
- The 4.161536 MB object is far smaller than Cloudcast's 100 GB paper experiments. Header/control
  costs and the fixed propagation term are therefore proportionally larger.
- The one-second budget was loose enough that the cost optimum used one tree. This run does not
  characterize Cloudcast's multi-tree stripe allocation under a tight budget. A larger object or
  tighter still-feasible SLO would be a separate experiment and was not added.
- Carousel and per-stripe FEC use ideal innovative-DoF semantics; no real codec CPU, rank defect,
  storage, or queueing cost is modeled.
- The Cloudcast POLICY arm models pure finite ownership transfer, but not Skyplane's object-store
  reads/writes, multipart chunking, HTTP registration, process scheduling, TLS, or VM startup.
- The egress ledger counts modeled wire bytes exactly, but the price tables are representative
  magnitudes and omit included allowances, tiering, taxes, provider-specific exceptions, and VM or
  object-store charges.
- The continuous FEC arms are unpaced in this scenario. Their emission and cost ratios must not be
  generalized to a paced production deployment.
- The public `skyplane/nsdi` code was not repaired or run here. These results cannot substantiate a
  system-vs-system claim about the native Cloudcast implementation.

## Artifacts, determinism, and reproduction

Committed artifacts live in
[`results/wansim/cloudcast-comparison/`](../results/wansim/cloudcast-comparison/):

- `trials.csv`: 128 reported rows, four arms x two loads x 16 seeds;
- `summaries.csv`: the eight two-axis aggregate rows used above;
- `cloudcast-policy-plans.csv`: both deterministic planner decisions;
- `sharing-structure.csv`: emergent physical-resource flow counts for all five execution variants;
- `failures.csv`: empty;
- `comparison-cells/`: 160 durable execution shards and the experiment manifest;
- `CELL-SHA256SUMS`: 481 per-cell/manifest file digests; and
- `SHA256SUMS`: five aggregate CSV digests plus the cell-digest manifest.

All **481/481** cell digests and **6/6** top-level digests verify. A resume pass skipped all 160
terminal cells and reproduced the same five aggregate hashes byte-for-byte. This proves durable
resume/aggregation stability; the existing wansim determinism gates, rather than a hash of one
stochastic sweep, cover repeat and registration-order invariance.

Authoritative Boston command:

```sh
cd /home/xindan/wansim-cloudcast-8302639
source "$HOME/.cargo/env"
CARGO_INCREMENTAL=0 cargo +1.96.0 run --release \
  --bin cloudcast_comparison -- 16 20 results
```

Resume command:

```sh
target/release/cloudcast_comparison 16 20 results --resume
```

The release build took 13.73 seconds. The timestamped log ran from 00:42:58 to 00:52:25 EDT,
about 9 minutes 27 seconds including that build. Peak observed RSS during the concurrent cell run
was approximately 7.2 GiB.

## Gates and commits

| Gate | Result |
|---|---|
| Durable Boston execution | 160 success, 0 failure |
| Durable resume | 160 skipped, 0 rerun, aggregate hashes unchanged |
| Digest verification | 481/481 cell files and 6/6 top-level entries green |
| Wansim `cargo fmt --all --check` | Green |
| Wansim `cargo clippy --all-targets -- -D warnings` | Green |
| Wansim `cargo nextest run` | 122 passed, 1 existing manual probe skipped |

| Commit | Concern |
|---|---|
| `13189fa` | Add the exact reduced-family planner and finite pure-replication protocol arm |
| `8302639` | Add representative provider price matrices, wire-cost accounting, and the durable Tier-1 harness |
| `e16b946` | Commit all 160 cell shards, aggregate CSVs, and digest manifests |
| this document | Record the two-axis verdict, fairness boundary, and reproduction details |

No native Cloudcast deployment, new profile, larger `K`, straggler arm, production change, or
follow-on experiment was started.
