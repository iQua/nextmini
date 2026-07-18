# Cloudcast Baseline Due Diligence

Date: 2026-07-18

Status: analysis only. This note records a read-only paper/code audit; it does
not implement a Cloudcast arm or modify either runtime.

## Executive verdict

Cloudcast is a relevant and defensible **named external baseline** for a paper
about pooled-FEC carousel multicast. It is recent, cloud-specific, explicitly
cost-aware, and its data plane is based on pure striping and replication rather
than pooled coding. It is therefore a useful test of whether pooled FEC improves
on a strong contemporary striped-tree policy.

However, three qualifications are load-bearing:

1. The public Cloudcast artifact is not runnable as checked out. It contains
   missing inputs, fatal planner defects, incomplete plumbing between the
   optimizer and the chunker, and a substantial gap between the paper's
   approximation algorithm and the checked-in implementation. Running it today
   would be an artifact-reconstruction project, not a normal benchmark setup.
2. Nextmini's in-tree `Cloudcast` mode is not a faithful reimplementation of
   the Cloudcast paper. It is a static weighted/fixed-stripe executor over
   controller-provided trees and contains none of Cloudcast's cost/SLO/VM/tree
   optimization. Used alone under the Cloudcast name, it would be a strawman.
3. Cloudcast optimizes monetary cost subject to a completion-time budget. A
   fair comparison must report both completion performance and cost, or must
   explicitly scope itself to a fixed-resource, fixed-topology throughput-only
   comparison. A throughput-only result cannot support a claim that Nextmini
   beats Cloudcast's full objective.

The recommended baseline strategy is a combination:

- **(b), mandatory and primary:** faithfully implement Cloudcast's placement
  and striping policy as a wansim arm, so Cloudcast striping and pooled carousel
  use the same transport, topology, queueing, and cross-traffic model.
- **(a), valuable but secondary:** run a repaired, author-confirmed revision of
  the actual Cloudcast/Skyplane system on real cloud VMs as external-validity
  evidence.
- **(c), not sufficient by itself:** use Nextmini's in-tree mode only as an
  execution backend for exact Cloudcast planner output, or label it honestly as
  `Cloudcast-style static striping` / `weighted striping`.

## 1. Official source and paper-artifact identity

### Paper

The paper is Sarah Wooders et al., *Cloudcast: High-Throughput, Cost-Aware
Overlay Multicast in the Cloud*, NSDI 2024:

- [USENIX paper page](https://www.usenix.org/conference/nsdi24/presentation/wooders)
- [Paper PDF](https://www.usenix.org/system/files/nsdi24-wooders.pdf)

The paper says that Cloudcast is an open-source implementation built as part of
Skyplane, with pluggable multicast-tree algorithms. The paper's stated goal is
to minimize cloud replication cost subject to a user-supplied runtime budget,
using measured inter-region throughput and provider pricing.

### Best available official artifact

The best available official source is:

- Repository: [`skyplane-project/skyplane`](https://github.com/skyplane-project/skyplane)
- Branch: [`nsdi`](https://github.com/skyplane-project/skyplane/tree/nsdi)
- Audited HEAD:
  [`c2197b2ff8da89eb912780439279b6de5f6c8f7b`](https://github.com/skyplane-project/skyplane/commit/c2197b2ff8da89eb912780439279b6de5f6c8f7b)
- Last branch commit: `Various fixes to get planner to work`, authored by
  Sarah Wooders on 2024-02-26.

The `nsdi` branch is six Cloudcast/multicast-related commits on top of the
then-current Skyplane `main`. Those commits add multicast planners, the ILP and
MDST paths, multicast gateway-program generation, and fixes by paper authors.
The authorship, timing, branch name, and content make this a high-confidence
match for the paper implementation.

The identity is nevertheless not publication-grade. There is no Cloudcast
release tag, artifact manifest, paper-specific README, container, exact
evaluation command, or mapping from paper figures to inputs and commits. The
paper does not pin `c2197b2` by hash. Before using the artifact for a published
system comparison, the authors should be asked to confirm:

- the exact evaluation revision;
- the throughput and cost profile files used for each figure;
- the stripe count and approximation settings;
- any uncommitted scripts or patches used in the NSDI evaluation.

### `multicast-paper-demo` is not the runtime

The Skyplane organization also contains
[`skyplane-project/multicast-paper-demo`](https://github.com/skyplane-project/multicast-paper-demo).
It contains an older Streamlit/HTML optimizer demonstration and profile CSVs;
it is not the Cloudcast data plane or a complete paper artifact. Its last
relevant history predates the final Cloudcast implementation, and its profile
schema does not directly match what the `nsdi` planner expects. It is useful as
historical input evidence, not as the system to benchmark.

No separate, newer official Cloudcast repository was found under the
`skyplane-project` organization. The `skyplane/nsdi` branch is therefore the
right source to cite, subject to author confirmation of the evaluation revision.

## 2. What Cloudcast actually implements

### 2.1 Optimization problem

Cloudcast models cloud regions as a directed graph. An edge has at least:

- a profiled effective throughput;
- an inter-region egress cost per unit of data;
- a source and destination provider/region.

Nodes additionally have VM ingress/egress limits, VM-count limits, and VM-time
costs. The planner receives a source region, a set of destination regions,
object size, number of stripes, and a user runtime budget.

For each stripe and directed edge, a binary decision says whether that stripe
uses the edge. Integer variables choose VM counts per region, while flow
variables enforce that each stripe connects the source to every destination.
The objective combines:

- egress cost for every stripe carried on every selected edge; and
- VM cost for the instances needed during the requested time budget.

Capacity constraints account for edge throughput and aggregate per-region VM
ingress/egress capacity. Under a loose deadline the optimizer tends toward a
low-cost Steiner-like multicast tree. Under a tight deadline it may choose
additional waypoints, more VMs, and different trees for different stripes.
Calling it merely a "cost-aware Steiner tree" is therefore incomplete: it
jointly performs placement, resource sizing, and stripe-tree selection.

The checked-in mathematical implementation is in
[`skyplane/planner/planner.py`](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/planner/planner.py#L329-L567).

### 2.2 Trees versus DAGs

The paper-level global distribution structure is a **tree per stripe**. A
transfer consists of a forest of possibly different and possibly
edge-overlapping stripe trees. It is not one arbitrary global DAG, and it is
not one tree whose link weights merely determine a sender-side stripe ratio.

Cloudcast later compiles the selected trees into a local gateway operator graph.
Those per-gateway programs contain receive, fan-out/mux, send, and object-store
operators and are represented as DAGs. The local operator DAG should not be
confused with the global route-selection object.

### 2.3 Chunking and striping

The paper evaluates a small finite stripe count, normally 8--16. It reports that
very small stripe counts restrict the feasible solution space and that gains
diminish beyond roughly ten stripes for its studied scenarios.

The Skyplane data plane divides objects into bounded multipart chunks (normally
up to approximately 64 MiB). Each chunk carries object/range metadata and a
`partition_id`; chunks are assigned round-robin across the configured stripe
IDs. All chunks assigned to a stripe follow that stripe's selected tree. The
public round-robin assignment appears in
[`skyplane/api/transfer_job.py`](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/api/transfer_job.py#L63-L153).

This is ownership striping: a byte belongs to one stripe/tree. A receiver must
obtain all stripes to reconstruct the object.

### 2.4 Transport and relay runtime

Cloudcast rides Skyplane's custom overlay data plane rather than a generic
multicast transport. Skyplane provisions gateway VMs, downloads chunks from the
source object store, and forwards them over pools of parallel, long-lived TCP
connections. Gateways use local/shared storage for chunks and worker processes
for receive, forward, and object-store operations. Optional TLS, compression,
and encryption exist in Skyplane; the paper disables compression and encryption
for the evaluated data-plane results.

Before a TCP sender transmits a chunk, the receiver-side gateway is registered
for it over HTTP. Bounded queues and the registration/admission path are intended
to limit outstanding storage and provide backpressure. Fan-out uses per-child
queues, but insertion is performed serially over the child handles, so a full
child queue can delay admission to later children. The relevant queue is
[`GatewayANDQueue`](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/gateway/gateway_queue.py#L31-L55).

### 2.5 No coding or FEC

Neither the paper algorithm nor the audited code contains erasure coding,
rateless coding, network coding, pooled repair, or an equivalent of BlockAck.
Cloudcast is pure replication of disjoint byte ranges:

- a stripe's chunks are copied along its tree;
- every receiver needs every stripe;
- redundant delivery comes from tree forwarding, not interchangeable coded
  degrees of freedom.

This is precisely why Cloudcast is relevant to a pooled-FEC paper: it is a
strong placement-and-striping policy but retains per-stripe byte ownership.

### 2.6 Stragglers and failures

No Cloudcast-specific online mechanism was found for dynamically rerouting a
stripe, changing stripe ownership, adding repair symbols, or recomputing the
plan around a slow receiver. The deployed plan is static during a transfer.

The implementation instead relies on:

- TCP reliability for segment loss;
- bounded queues and TCP/application backpressure for slow consumers;
- limited reconnect/retry behavior in send and object-store operations; and
- a global transfer error/abort path for persistent gateway failures.

Consequently, a slow branch can propagate pressure through its tree. A lost
gateway or unrecoverable object-store error is not repaired by Cloudcast's
placement algorithm. The paper does not present a separate straggler/failure
protocol comparable to carousel repair.

### 2.7 Intended configuration versus public defaults

The research algorithm conceptually exposes object size, runtime budget,
stripe count, provider filters, VM limit, connection count, profiles, and solver
choice. In the public integration path, several of those inputs are disconnected
or fixed by defaults:

- `Pipeline` defaults to one VM and 32 connections;
- `MulticastILPPlanner` defaults to a 10,000-second budget and one stripe;
- `logical_plan` defaults to a 1 GB transfer;
- the chunker defaults to one partition;
- the ILP defaults to proprietary Gurobi;
- the optional node approximation randomly samples 15 nodes.

These defaults are not the paper's 8--16-stripe evaluation configuration.

## 3. Code-quality and reproducibility assessment

### 3.1 Bottom line

The algorithm is interesting and worthy of comparison. The public artifact is
not a usable reference implementation without repair. This conclusion follows
from source inspection alone; no cloud VMs were provisioned during this
analysis. The static blockers occur before a meaningful end-to-end run.

### 3.2 Concrete blocking defects

The most consequential findings at `c2197b2` are:

1. **Required throughput profile absent.** `Planner.make_nx_graph` defaults to
   `data/throughput.csv`, but that file is not present in the official branch.
   The similarly named demo repository has a different schema. See
   [`make_nx_graph`](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/planner/planner.py#L66-L81).

2. **Successful ILP conversion returns `None`.** `multicast_solution_to_nxgraph`
   constructs and populates `result_g` but has no `return result_g`. The caller
   then hands `None` to topology-plan generation. See
   [the conversion function](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/planner/planner.py#L347-L375).

3. **A throughput constraint uses a stale edge variable.** The loop over
   `edge_i` indexes the source node through `edge[0]`, where `edge` is left over
   from an earlier loop, instead of using `edges[edge_i][0]`. VM capacity can
   therefore be attached to the wrong source node. See
   [the constraint](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/planner/planner.py#L491-L495).

4. **Paper inputs are not plumbed through the public pipeline.** The pipeline
   creates `MulticastILPPlanner(max_instances, num_connections)` without the
   transfer size, deadline, or evaluated stripe count, so the planner receives
   its 1 GB / 10,000 s / one-stripe defaults. The chunker separately defaults
   to one partition. See
   [`Pipeline.__init__`](https://github.com/skyplane-project/skyplane/blob/c2197b2ff8da89eb912780439279b6de5f6c8f7b/skyplane/api/pipeline.py#L30-L107).

5. **The paper's scalable approximation is missing or materially different.**
   The paper describes clustering roughly twenty representative regions,
   restricting waypoint depth, and greedily solving stripes while consuming
   residual edge/node capacity. The public path contains the monolithic ILP and
   a disabled random 15-node sample; it does not contain a faithful,
   deterministic implementation of the described full approximation pipeline.

6. **Other planner paths contain immediate defects.** `MulticastDirectPlanner`
   calls job verification without `multicast=True`, rejecting multiple
   destinations. `MulticastMDSTPlanner` references `self.num_partitions`, while
   the base class defines `self.n_partitions`.

7. **Solver portability is poor.** Gurobi is the default. A CBC branch exists
   with a 60-second limit, but there is no evidence that it reproduces the paper
   solutions, and the repository does not package a paper-specific solver
   environment.

8. **Completion/error semantics are unfinished.** Multicast destination
   tracking contains TODOs, and some pipeline error paths deprovision and return
   without clearly propagating failure to the caller. This needs validation
   before comparing completion times or success rates.

### 3.3 Tests, maintenance, and artifact gap

The branch contains generic Skyplane unit and cloud-integration tests, but no
meaningful optimizer conformance suite that pins the paper's objective,
capacity constraints, expected trees, stripe assignment, or figure inputs.
There is no `nsdi`-branch CI history demonstrating an end-to-end Cloudcast run.

The branch's last Cloudcast commit is from February 2024, and the surrounding
Skyplane code and dependencies are from the same era. Current cloud APIs, VM
types, prices, quotas, and object-store behavior may all require updates. The
Python dependency envelope also predates current Python releases.

This is more than ordinary bit rot: even with the old dependencies, the missing
profile and planner defects prevent the checked-in path from representing the
paper as written.

### 3.4 Work required for a real-cloud head-to-head

A credible system-level comparison would require:

1. Ask the Cloudcast authors for the exact revision, profiles, evaluation
   commands, and any missing approximation code.
2. Build a pinned Python 3.10/3.11-era container and reproduce one known paper
   cell before changing dependencies.
3. Fix the missing return, stale-edge constraint, multicast verification,
   MDST field name, completion tracking, and pipeline-to-chunker stripe plumbing.
4. Restore or regenerate the throughput/cost profiles, preserving the paper's
   units and provider-region names.
5. Validate planner output independently: every stripe must reach every
   destination, remain acyclic/tree-shaped, obey capacity and VM constraints,
   and reproduce the reported objective value.
6. Update cloud provisioning only after the old result is pinned; publish the
   port as a separate patch set so algorithm fixes are distinguishable from
   cloud-API maintenance.
7. Run repeated trials at matched regions, VM types, VM counts, connection
   counts, object-store boundaries, encryption/compression settings, and time
   windows.

The result should be described as a repaired or ported Cloudcast artifact, not
as the untouched official repository.

## 4. Comparison with Nextmini's in-tree Cloudcast mode

### 4.1 Code inspected

The Nextmini implementation was inspected in the sibling worktree
`/Users/winifred/nextmini`, branch `codex/tree-scoped-transports-main`, including
the introduction commit:

- `ecaab07 Added Cloudcast runtime mode.`
- `dataplane/src/node/session/sender/cloudcast.rs`
- `dataplane/src/node/session/receiver/cloudcast.rs`
- `dataplane/src/node/session/runtime.rs`
- `dataplane/src/node/config.rs`

### 4.2 What `ecaab07` implemented

The original mode sent ordinary source blocks exactly once over a set of
controller-provided tree scopes. A smooth weighted round-robin selector used
`fec_default_tree_ids` and `fec_default_tree_weights` to choose the tree for
each next block. After the payload sweep, the sender emitted `SourceDone` and
waited for a `Need` report from every quorum receiver.

It provided no repair. A receiver reporting any missing plain block caused a
protocol error and sender abort.

### 4.3 What the current branch implements

The current code replaces the online weighted selector with an explicit fixed
stripe table:

- `CloudcastRuntimeConfig` holds the used tree IDs and `stripe_tree_ids`;
- an explicit `cloudcast_stripe_tree_ids` table can be configured;
- otherwise, `quantize_cloudcast_tree_weights` converts tree weights into a
  finite stripe allocation using floor plus largest remainders;
- block `id % stripe_count` selects the corresponding tree;
- every plain block is still sent exactly once;
- missing blocks still cause abort rather than retransmission.

This is a reasonable implementation of static weighted striping over trees. It
does not implement the Cloudcast optimizer.

### 4.4 Missing Cloudcast semantics

No in-tree component was found for:

- provider pricing or egress-cost minimization;
- user runtime/SLO constrained optimization;
- throughput-profile ingestion;
- waypoint/relay-region selection;
- per-region VM-count optimization;
- ingress/egress quota constraints;
- per-stripe receiver-covering tree construction;
- Cloudcast's exact or approximate planner;
- Skyplane's object-store chunk and gateway resource model.

The configured trees are assumed to have been supplied by the controller. The
weight quantizer decides only how many static stripe slots use each existing
tree. That is closer to rate-proportional striping than to the full Cloudcast
algorithm.

### 4.5 Faithfulness verdict

The in-tree mode is a **loose homage/execution primitive**, not a faithful
reimplementation. It cannot serve as the named Cloudcast baseline in its
current form.

It can become useful in two narrower roles:

1. As the executor for externally generated Cloudcast plans: run a faithful
   Cloudcast optimizer, install every selected stripe tree in Nextmini, and feed
   the exact stripe-to-tree table into the runtime.
2. As an internal mechanism ablation named `weighted static striping` or
   `Cloudcast-style striping`.

Even in role 1, the paper must say **Cloudcast policy on Nextmini transport**.
This arm deliberately normalizes transport behavior and is not the native
Cloudcast/Skyplane system.

The completion and admission semantics also need care. Native Cloudcast relies
on queue/TCP backpressure. Nextmini's broader runtime supports receiver-boundary
hybrid dropping, while this plain Cloudcast mode cannot repair a dropped block.
A comparison that silently gives the Cloudcast arm a different admission policy
would conflate placement, ownership, backpressure, and FEC.

## 5. Baseline decision: (a), (b), (c), or a combination

### (a) Run their actual system on real clouds

**Verdict: yes, if repaired and author-confirmed; not as the sole baseline.**

Advantages:

- strongest defense against the claim that wansim favors Nextmini;
- captures Skyplane provisioning, object-store I/O, VM/network limits, and
  native gateway behavior;
- is recognizably the external system reviewed by NSDI.

Risks:

- the public artifact is currently broken and incomplete;
- cloud and price drift complicate comparison with 2024 results;
- repairing the artifact can accidentally change the algorithm;
- native transports differ, so a result cannot isolate pooled FEC from
  Skyplane-versus-Nextmini engineering.

Use this as an external-validity experiment after the algorithm-level arm is
validated. Publish the exact fork, patch list, profiles, container, price
snapshot, and commands.

### (b) Reimplement Cloudcast policy in wansim

**Verdict: mandatory and the primary apples-to-apples baseline.**

The arm should implement the paper's optimization surface, not merely weight
existing trees. Its input should include:

- the same physical/overlay region graph used by the scenario;
- directed edge throughput and egress price;
- VM ingress/egress caps, VM price, and per-region instance limit;
- source, receivers, object size, deadline, and stripe count;
- the same admissible waypoint and tree candidate space for every protocol.

Its output must contain deterministic per-stripe receiver-covering trees and
per-region VM/resource choices. For small cells, solve the exact MILP. For
larger cells, implement and separately validate the paper's clustering,
waypoint restriction, and residual-capacity greedy stripe process. The model
must send uncoded owned bytes along each stripe tree, with no carousel repair.

This arm puts Cloudcast and pooled carousel on identical TCP, queue, delay,
cross-traffic, and background-load semantics. It therefore answers the
algorithmic question cleanly: given the same WAN, does interchangeable pooled
DoF beat optimized byte ownership?

Validation should pin at least:

- every stripe reaches all receivers;
- selected structures are trees/acyclic;
- costs and capacity constraints recompute independently;
- deterministic test vectors for small graphs;
- exact MILP versus approximation gaps on tractable cells;
- known limiting cases: loose SLO approaches a low-cost tree, while a tight SLO
  spends more resources or becomes infeasible.

### (c) Use Nextmini's current in-tree mode

**Verdict: no, not by itself.**

Calling the existing weighted/static selector `Cloudcast` in a paper comparison
would omit the strongest part of Cloudcast: its cost/SLO-constrained placement
and resource optimizer. Reviewers would reasonably classify it as a strawman.

It is acceptable only when:

- it executes exact output from the faithful planner in (b), in which case it
  is labeled `Cloudcast policy on Nextmini transport`; or
- it is explicitly presented as a simple weighted-striping ablation rather
  than the external Cloudcast baseline.

### Recommended combination

Use (b) for the main mechanism tables and Pareto curves, (a) for a smaller
real-cloud external-validity section, and (c) only as the normalized execution
vehicle or a renamed ablation. This separates three questions that otherwise
become confounded:

1. Is Cloudcast's placement/striping policy better or worse than pooling?
2. Does the result survive native system implementations?
3. Which gains come from transport/admission engineering rather than coding?

## 6. Cost-axis requirements for a fair paper

Cloudcast's objective is cost minimization subject to a runtime budget. A fair
paper cannot compare only completion time after giving each system arbitrary
resources.

### Required outcome axes

Report at least:

- receiver-local and all-receiver barrier completion time;
- sender completion time where relevant;
- useful throughput and P50/P95 variation;
- egress bytes and egress dollars per provider edge;
- VM count, VM-seconds, and VM dollars per region;
- total monetary cost;
- all FEC repair, retransmission, duplicate, carousel-tail, and control bytes;
- success/abort rate and the reason for failures;
- optimizer/planner runtime, reported separately from data transfer.

The central presentation should be a completion-time versus total-cost Pareto
frontier across deadlines/resource points, not one cherry-picked topology.

### Inputs and resources that must be matched

- source and destination provider/regions;
- object size and object-store read/write boundary;
- VM families, VM counts/limits, NIC caps, and connection counts;
- candidate relay/waypoint regions;
- measured or modeled directed link capacities and RTTs;
- background load and trial timing;
- provider egress and VM price snapshot;
- encryption, compression, checksumming, and persistence semantics;
- completion definition: all bytes durably present at every destination;
- reliability/admission policy.

Cloudcast must be allowed to choose both low-cost and high-throughput plans under
the same declared SLOs. Nextmini's repair/tail bytes must be charged on every
traversed priced edge. Cloudcast waypoint forwarding must likewise be charged
at every provider egress boundary rather than only once at the source.

### Throughput-only scope

A throughput-only comparison is still useful if the research question is
narrowly stated. It must hold topology, VM/resource budget, and cost point fixed
and say explicitly that it compares transport/coding behavior rather than
Cloudcast's full optimization objective. The paper must not turn that result
into a claim that Nextmini is generally cheaper or better than Cloudcast.

### Backpressure/admission confound

Native Cloudcast uses reliable TCP plus bounded backpressure. Nextmini carousel
can use hybrid receiver-inbox drop and repair. For a fair decomposition:

- include a Nextmini blocking arm matching Cloudcast's pressure propagation;
- include production hybrid-drop carousel as a separate system arm;
- report drops, stalls, repair bytes, and healthy-receiver externality;
- do not inject loss at Cloudcast's application boundary without giving it an
  explicitly defined recovery behavior.

This prevents a hybrid-drop improvement from being misreported as a pure
pooling improvement.

## 7. Stronger or complementary external baselines

### SplitStream: strongest conceptual striped-tree baseline

[SplitStream](https://www.microsoft.com/en-us/research/publication/splitstream-high-bandwidth-multicast-in-a-cooperative-environment/)
(SOSP 2003) is the canonical direct baseline for the ownership mechanism under
study. It splits content into stripes and sends each stripe over a different,
ideally interior-node-disjoint multicast tree. Its goal is to distribute
forwarding load across peers and obtain path diversity.

It is older and its original system is unlikely to be a practical modern-cloud
artifact, but a faithful SplitStream-style arm in wansim is arguably more
important than Cloudcast for the narrow theory claim. It cleanly represents
"one stripe owns one tree" without adding Cloudcast's cloud-price optimizer.

Recommended use:

- SplitStream-style disjoint-tree striping as the canonical mechanism baseline;
- Cloudcast-Opt as the contemporary cost-aware cloud baseline.

### QuickCast: straggler-aware tree cohorts

[QuickCast](https://arxiv.org/abs/1801.00837) partitions receivers into cohorts
and selects forwarding trees to reduce the slowest-receiver penalty while
controlling bandwidth overhead. It is relevant if the paper makes strong claims
about receiver heterogeneity or tree-level straggler isolation. Its setting is
more operator-controlled inter-datacenter traffic engineering than public-cloud
customer overlay, so it is complementary rather than a replacement for
Cloudcast.

### CodedBulk: coding-aware WAN baseline

[CodedBulk](https://www.usenix.org/conference/nsdi21/presentation/tseng)
(NSDI 2021) is a stronger baseline if the claim expands from striped multicast
trees to coded bulk transfer in general. It uses network coding with custom
hop-by-hop flow control and reports an end-to-end geo-distributed implementation.
However, it assumes an inter-datacenter environment with more network control
than a public-cloud overlay customer has. It should not be presented as a
strictly interchangeable Cloudcast replacement.

### Necessary internal ablations

Regardless of named external systems, retain:

- best single tree;
- equal split and rate-proportional striping;
- per-stripe continuous FEC, the strongest ownership variant;
- pooled rounds versus pooled carousel;
- blocking versus hybrid-drop receiver admission.

These arms separate placement/path-diversity gain, ownership-waste gain,
coding/pooling gain, feedback-barrier gain, and admission-policy gain. A named
system comparison without these decompositions would not explain why either
system wins.

## 8. Final recommendation for the paper

Cloudcast should be named and compared. It is not safe to omit: it is recent,
directly concerned with public-cloud multicast, and optimizes a dimension
(dollars) that Nextmini's current mechanism studies do not replace.

The defensible experimental package is:

1. A faithful `Cloudcast-Opt` policy arm in wansim, validated against exact
   small-instance solutions and, ideally, author-provided vectors.
2. A SplitStream-style arm as the canonical pure striped-tree baseline.
3. The existing strongest internal ownership and single-tree ablations.
4. A limited repaired-Cloudcast versus Nextmini real-cloud experiment if the
   authors can confirm the artifact and profiles.
5. Completion-time/cost Pareto curves, with all repair and relay egress charged.

Do not use the current in-tree Nextmini mode alone under the label `Cloudcast`.
Until it consumes faithful optimizer output, rename it in paper-facing artifacts
to `weighted static striping` or `Cloudcast-style striping`. If it later executes
the exact planner output, label it `Cloudcast policy on Nextmini transport` and
keep the native-system result separate.

The blunt assessment is therefore:

- **Algorithm quality:** strong and highly relevant.
- **Public artifact quality:** poor and not presently reproducible.
- **Suitability as a named baseline:** yes, provided the policy is represented
  faithfully and the monetary-cost objective is not discarded.
- **Suitability of our existing mode as that baseline:** no.
