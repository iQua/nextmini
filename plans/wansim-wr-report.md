# INTERIM — wansim WR realistic public-cloud envelope

Date: 2026-07-17

Branch: `perfect-fec-runtime`

Baseline: `704e111` (WR plan); implementation evidence revision: `8b3d710`

Scope: WR only; W4 calibration was not started

Evidence class: **model-level realistic-envelope evidence, not a WAN measurement or a reproduction
of AWS, Google Cloud, or DigitalOcean**

> **INTERIM / NO PERFORMANCE CONCLUSIONS.** The final-model evidence run at revision `8b3d710`
> failed closed after 4:38:21. It produced no CSVs because the original harness persisted only
> after every cell succeeded. The CSVs currently committed under
> `results/wansim/wr-realistic/` predate the final temporal-jitter model and were already rejected
> by the calibration audit described below. They are retained only as forensic input for the WR
> triage and MUST NOT be quoted as WR evidence.

## Interim run outcome

The attempted 16-seed, 20-worker run on `xindan@boston.csl.toronto.edu` failed at task 2,895:

```text
slice=straggler
profile=digitalocean-like
placement=west-origin
utilization=70
jitter=true
protocol=carousel
K=8192
seed=15
admission=hybrid-drop
slow_receiver=0
failure=peer 1 remained alive without completion progress
```

The process exited with status 1 after 4:38:21 wall time, 309,986.69 user CPU seconds, and a
12,254,812 KiB peak RSS. This is a triage input, not yet a production-protocol finding: the
follow-up must determine whether rank advanced behind a block-quantized watermark, rank itself
stalled, the acknowledgement path stalled, or the simulator mis-modeled an event.

No rounds-versus-carousel, pooling-versus-path-diversity, A4, cadence, straggler-cost, scaling, or
liveness-margin verdict can be drawn from this interim state.

## What the envelope models

WR re-runs the mechanism questions from W1–W3 in six deterministic, representative public-cloud
scenarios. The profiles use public region identities and public descriptions of VM networking as
context, then synthesize all latency, queue, capacity, jitter, topology, placement, and traffic
parameters inside the envelope declared by [`wansim-plan.md`](wansim-plan.md#stage-wr--realistic-cloud-envelope-added-2026-07-17-user-directive).
No number in a scenario TOML is a provider measurement, guarantee, inferred internal topology, or
claim about a named cloud.

The committed inputs are under [`wansim/scenarios/wr`](../wansim/scenarios/wr). Every file begins
with the same representative-only warning and round-trips through the checked scenario parser.

### Public source notes and synthesized parameters

The public sources establish only region availability and the broad existence/range of VM network
limits and provider backbones:

- AWS: [Regions and Availability Zones](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html)
  and [EC2 instance network bandwidth](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html).
- Google Cloud: [global locations](https://cloud.google.com/about/locations),
  [Compute Engine bandwidth](https://docs.cloud.google.com/compute/docs/network-bandwidth), and
  [Network Service Tiers](https://docs.cloud.google.com/network-tiers/docs/overview).
- DigitalOcean: [regional availability](https://docs.digitalocean.com/platform/regional-availability/)
  and [Droplet network limits](https://docs.digitalocean.com/products/droplets/details/limits/).

The following values are **our representative model inputs**, not values taken from those pages:

| Profile label | Regions | Modeled VM NIC cap / queue | Directed trunk cap / queue | Background reference rate |
|---|---:|---:|---:|---:|
| aws-like | 6 | 2.0 Gbit/s / 4 MiB | 1.2 Gbit/s / 8 MiB | 1.2 Gbit/s |
| gcp-like | 6 | 3.0 Gbit/s / 6 MiB | 1.6 Gbit/s / 10 MiB | 1.6 Gbit/s |
| digitalocean-like | 6 | 2.0 Gbit/s / 3 MiB | 0.8 Gbit/s / 6 MiB | 0.8 Gbit/s |

All profiles use a 1.5 ms intra-region RTT, 2.5–4.5 ms one-way regional access delays, directed
same-continent and intercontinental transit values that produce the plan's 10–60 ms,
70–90 ms, and 100–150 ms RTT regimes, and optional seeded bounded propagation jitter of ±5%.
Small one-millisecond edge effects in an asymmetric direction are part of the synthesized matrix,
not measurement precision.

Each profile has two placements:

| Profile | Placement | Sender | Receivers | Tree 0 relays | Tree 1 relays |
|---|---|---|---|---|---|
| aws-like | east-origin | us-east-1 | us-west-2, eu-west-1, ap-northeast-1 | us-west-2, eu-west-1 | us-east-2, eu-central-1 |
| aws-like | west-origin | us-west-2 | us-east-1, eu-central-1, ap-northeast-1 | us-east-1, eu-central-1 | us-east-2, eu-west-1 |
| gcp-like | east-origin | us-east4 | us-west1, europe-west1, asia-northeast1 | us-west1, europe-west1 | us-central1, europe-west3 |
| gcp-like | west-origin | us-west1 | us-east4, europe-west3, asia-northeast1 | us-east4, europe-west3 | us-central1, europe-west1 |
| digitalocean-like | east-origin | nyc3 | sfo3, lon1, sgp1 | sfo3, lon1 | tor1, fra1 |
| digitalocean-like | west-origin | sfo3 | nyc3, fra1, sgp1 | nyc3, fra1 | tor1, lon1 |

The generator scores relay pairs over the directed backbone and chooses distinct relay pairs where
possible. It does not accept an overlap percentage. Foreground and background flow counts on NICs
and trunks are derived after routing and committed in `sharing-structure.csv`; physical sharing
therefore emerges from placement.

### Event model and transport

The provider backbone is a graph of four transit hubs (North America east, North America west,
Europe, and Asia-Pacific) connected by ten directed trunks. Each region contributes a shared VM
NIC service resource. Deterministic shortest-path routing maps every direction of every TCP flow
onto an ordered list of NIC and trunk resources. Each resource has a finite byte queue, an integer
bit rate, and timestamped serialization service. Propagation is directional. When jitter is
enabled, the counter-based, domain-separated PRF chooses one bounded offset per physical resource,
seed, and 100 ms epoch. A per-resource propagation frontier preserves FIFO arrival order across
epoch changes. This gives time-varying RTT without creating nonphysical reordering from independent
per-packet delay draws.

Every overlay edge is a separate persistent days-derived Reno connection with its own congestion
window, advertised receive window, finite send/receive buffers, cumulative ACKs, retransmission,
and zero-window persist behavior. WR opts those sockets into the fork's window-scaled Reno
constructor: its congestion-window ceiling and initial slow-start threshold follow the negotiated
receive buffer instead of days' legacy 65,535-byte ceiling. W0--W3 retain legacy Reno. Relays
reassemble each complete length-prefixed logical frame before configured-order sequential
admission to independent child TCP connections. A foreground data frame carries 508 symbol bytes
plus a four-byte length prefix; serialization additionally charges the fork's 40-byte TCP/IPv4
overhead per segment. Control frames use independent per-peer TCP connections but share the same
physical regional resources in their forward and reverse directions.

Eight explicit background TCP applications run in every scenario: a paced bulk flow and a seeded
bounded heavy-tailed on/off flow in each direction along each tree's root region pair. The on/off
base durations are 100/150 ms with a geometric power-of-two multiplier capped at 64. Background
applications admit bytes every 10 ms and use a 256 KiB TCP serialization quantum; this batching is
an explicit runtime optimization for synthetic cross-traffic, not a claim about cloud MSS or
offload. Foreground and control traffic retain their real frame sizes. Each root-pair bundle is
scaled from the profile reference to request 30%, 50%, or 70% load. If placement merges both
bundles, their competition emerges naturally. Congestion windows, finite queues, and competition
remain authoritative, so the label is offered load; every CSV separately reports realized maximum
background trunk utilization.

Background flows start one simulated second before the experiment clock. All source, relay,
receiver, and control endpoints start together at the end of that warm-up, avoiding both a
foreground contribution to warm-up and a half-open socket ordering artifact. One second is roughly
7--100 RTTs over the declared matrix. Completion times, liveness gaps, correlations, and utilization
windows are all rebased to the foreground start.

### Protocol and codec abstraction

WR calls the independent wansim endpoints implemented in W1, never a production actor. Carousel
uses cumulative join-semilattice BlockAck, debounce and heartbeat, targeted AckProbe, repeated
best-effort SessionComplete, dual per-peer liveness clocks, work-conserving emission, and a final
completion recheck before submission. The 1x timing now mirrors the production defaults:
8 ms debounce, 300 ms heartbeat, 250 ms probe interval, 3 s silence timeout, 15 s no-progress
timeout, 17 s passive receiver window, and three SessionComplete frames 20 ms apart. Only debounce
and heartbeat scale in the 0.5x/1x/2x cadence slice.

The codec remains the declared ideal DoF abstraction: rank is `min(K, distinct innovative
deliveries)`. WR does not model METTLE/RaptorQ algebra or decoder CPU. For feedback geometry,
K=8,192 maps to 482 cumulative progress units and K=65,536 to 3,856, using the production default
8,500-byte block ceiling and 508-byte symbol payload. This prevents the whole object from looking
like one silent BlockAck block while retaining a codec-independent receiver.

The baselines are:

- pooled rounds: K source departures, SourceDone, receiver deficit reports over real control
  paths, and max-deficit repair rounds;
- per-stripe FEC: the strongest ownership baseline, with rate-proportional global quotas and an
  independent DoF bucket per tree;
- best single tree: both single-tree candidates are run on each coupled seed and the faster is
  selected, while the idle foreground connection sends matched non-useful traffic so flow count
  is not gifted to the baseline.

## Matrix and deterministic discipline

The main matrix is:

```text
3 profiles × 2 placements × 3 offered background utilizations
× jitter {off,on} × 5 protocols × 16 seeds
= 2,880 executions at K=8,192
```

Additional slices add 32 hybrid/blocking straggler executions, 48 cadence executions, 32
one-versus-two-session executions, and 48 K=65,536 scaling executions, for **3,040 simulator
executions**. The slow-receiver and control slices use the digitalocean-like west-origin placement,
70% offered background load, and jitter. The scaling slice is one carousel cell per profile at
east-origin, 50%, and jitter, with the same 16-seed budget as every other reported cell. Every
nexosim simulation has one worker; host parallelism only runs independent indexed tasks. Rows are
reduced in canonical task order.

The two-session slice places two complete carousel sessions, with independent overlay TCP state,
onto the same NICs and trunks. Its metric is the tagged first session under one versus two
concurrent sessions; it is not an aggregate of both sessions.

All seeds drive background transitions, propagation jitter, payload identity, and local tie
resolution through counter-based domain-separated PRFs. The same seed supplies coupled stochastic
inputs across protocol variants. Because the WAN model is endogenous, the resulting TCP delivery
trace is allowed to diverge after a protocol changes queue occupancy; coupling means common random
inputs, not an impossible replay of exogenous opportunities.

### Fail-closed development checks

The first Boston full run at revision `bf00e25` stopped at the K=65,536 carousel spot because WR
had represented the whole object as one BlockAck progress unit. That exposed an eightfold-scaling
harness defect rather than generating partial evidence. Revision `0ba7420` added production-shaped
progress geometry and production timing. A targeted aws-like K=65,536 carousel run then completed
at a 35.392311694 s receiver barrier and 35.472238249 s sender completion.

A later full run at `0ba7420` completed 3,020 tasks and then failed closed on the digitalocean-like
K=65,536 `best-single-tree1` spot at the artificial 60 s simulation horizon. Revision `17b6015`
made the large-K horizon 180 s without changing a protocol timeout; the simulator still exits as
soon as all endpoints finish. The exact failed cell then completed on Boston at a
74.968993229 s receiver barrier and 75.044322431 s sender completion. Neither failed run wrote a
CSV, and neither contributes to the committed statistics.

An initial 3,022-task run at `17b6015` did finish, but a post-run calibration audit rejected all of
its CSVs before commit. Days' legacy Reno ceiling compressed long-RTT flows to a few Mbit/s, making
the 30/50/70 axis largely inert, while independent per-packet jitter created artificial reordering.
After opting WR alone into window-scaled Reno and initially making jitter constant per resource and
seed, a second audit found an ACK-clock micro-packet storm in the paced background application: it
admitted a few new bytes at every ACK and immediately sent them under `TCP_NODELAY`. Revision
`13d8303` makes paced application admission timer-only, pins it with a regression, starts every
foreground endpoint after a common warm-up, and records realized load. Revision `a066a40` replaced
the two-seed scaling fragments with three full 16-seed cells. A final requirement audit rejected
static per-run offset as insufficient for the requested temporal jitter axis; revision `8b3d710`
introduced the seeded 100 ms jitter process and FIFO propagation frontier. The rejected CSVs are
committed in this interim checkpoint solely so the failed-run provenance is not lost. They remain
explicitly non-evidence and are not quoted below; this is a fail-closed calibration history, not
result selection.

## Performance verdicts

**Pending.** The failed run provides no complete or admissible performance matrix.

## Reproduction

The attempted evidence execution was performed on `xindan@boston.csl.toronto.edu`; the local
workstation was used only for source editing and result synchronization. The remote checkout
recorded `source_commit=8b3d710`, used rustc/cargo 1.96.0, and built the lockfile offline after
dependency preparation. The repeat command below was planned but **was not run** after the first
execution failed.

```bash
ssh xindan@boston.csl.toronto.edu
cd ~/wansim-wr-ed31c2a
PATH="$HOME/.cargo/bin:$PATH" CARGO_INCREMENTAL=0 \
  cargo build --locked --offline --release --bin wr_probe --bin wr_realistic

/usr/bin/time -v target/release/wr_realistic \
  16 20 /home/xindan/wr-run-8b3d710/evidence

# NOT RUN: the evidence execution failed before a repeat was justified.
# /usr/bin/time -v target/release/wr_realistic \
#   16 20 /home/xindan/wr-run-8b3d710/repeat

(cd /home/xindan/wr-run-8b3d710/evidence && sha256sum *.csv | sort -k2 > SHA256SUMS)
(cd /home/xindan/wr-run-8b3d710/repeat && sha256sum *.csv | sort -k2 > SHA256SUMS)
diff -u \
  /home/xindan/wr-run-8b3d710/evidence/SHA256SUMS \
  /home/xindan/wr-run-8b3d710/repeat/SHA256SUMS
```

The committed scenarios can be regenerated independently:

```bash
cd wansim
CARGO_INCREMENTAL=0 cargo run --locked --release --bin wr_scenarios -- scenarios/wr
```

## Limits and interpretation boundary

- The cloud labels are mnemonic envelope families. The topology, capacities, queues, RTTs,
  asymmetry, jitter, and traffic are synthesized representative values. No named cloud is claimed
  to be reproduced, ranked, or measured. W4 calibration remains the separate step that would
  locate a real deployment inside or outside this envelope.
- The receiver is an ideal innovative-DoF bucket. Real FEC rank dependence, decoder construction,
  decode CPU, cache effects, and sink I/O are absent except for the configured receiver service
  center. WR can test transport/protocol mechanisms, not codec throughput.
- Transport uses the validated days-derived Reno abstraction, not a provider's kernel build,
  CUBIC/BBR policy, TLS stack, NIC offload, hypervisor scheduling, or per-tenant policer. All of
  those could move a calibrated result.
- Four transit hubs, six regions, two placements, and three receivers cover several latency and
  sharing regimes but not arbitrary geography, route changes, failures, multi-provider transit,
  or provider traffic engineering.
- Background applications are explicit TCP flows, but their bulk and bounded heavy-tail processes
  are synthetic rather than fitted to packet traces. Their 10 ms admission cadence and 256 KiB
  serialization quantum batch background work and cannot support packet-scale burst claims. The
  30/50/70 labels are offered load, and observed utilization is an output of congestion control,
  placement, and finite foreground duration.
- Sixteen seeds give spread, not tail-SLA confidence. The K=65,536 cells are scaling checks only;
  they must not be read as a failure-probability estimate.
- Physical queue loss is recovered by TCP. The model has no independent corruption or provider
  outage process. Receiver application drops remain distinct post-transport events.
- The faster of two single-tree runs is selected per coupled seed. This deliberately oracle-strong
  baseline makes the reported path-diversity benefit conservative relative to choosing one fixed
  tree before observing the run.
- Critical-path counters preserve the W0–W3 event boundaries, but WR does not claim a unique
  decomposition of overlapping TCP, resource-queue, relay, and mailbox waits.

No production code, wire format, manifest surface, or W4 calibration artifact changed in WR.

## Gates

**Pending.** This interim checkpoint records the failed run before harness-resilience and causal
triage work. It is not a completed WR gate.
