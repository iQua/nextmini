# wansim — WAN pipeline simulator for nextmini (master plan)

Date: 2026-07-17
Evidence class: model-level causal evidence ("causal WAN pipeline model" — NOT a digital twin until
calibrated, NOT a WAN measurement). Successor to `plans/pooling-speedup-sim-report.md`, which
established the two speedup mechanisms under an abstract opportunity model; wansim adds the WAN
structure that model deliberately lacked: topology, queues, propagation delay, hop-by-hop reliable
transport with buffered backpressure, relay fan-out, and endogenous tree-rate coupling.

Ultimate purpose (user directive, 2026-07-17): verify whether the theory-driven changes landed on
this branch — the work-conserving carousel, BlockAck join, the receiver inbox drop policy, and the
sequential fan-out behavior they interact with — are actually the right design in a WAN-shaped
system. The simulator therefore mirrors the DEPLOYED semantics (with each deviation from the code
called out), and every experiment reports its verdict against the specific implementation choice it
tests, not only against the abstract theory.

## Foundation decision

Build on **iQua/days** (github.com/iQua/days, AGPL-3.0, pinned git rev) rather than from scratch.
days provides, tested: a deterministic nexosim event engine (seeded, virtual time), segment-level
TCP (cwnd, RFC 6298 RTO, retransmission, ECN; Reno/CUBIC/BBR), switches with finite queues and
FIFO/WRR/RED (optional PFC), arbitrary topologies with custom routing, distribution-driven
background traffic, and config+CSV discipline. What days lacks — and wansim adds — is the nextmini
overlay: in-network replication trees (its Broadcast is N unicast flows), hop-wise TCP relaying,
the §P protocol endpoints, receiver inbox admission policies, and critical-path attribution.

Layout: `wansim/` at the repo root as a **standalone cargo workspace**, added to the root
workspace's `exclude`. The 828-test main gate and root Cargo.lock are untouched; wansim has its own
gate (`cd wansim && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo nextest
run`). wansim MAY path-depend on `../messages` for wire constants (frame sizes, geometry caps) but
MUST implement the §P state machines independently (a simulator that calls production actors shares
their bugs); pin the independent implementation against the existing conformance vectors instead.
License note: days is AGPL; nextmini is an unlicensed internal repo, so no conflict; keeping wansim
excluded keeps it separable if nextmini is ever released permissively.

## Design constraints (binding; distilled from the 2026-07-17 design review)

1. **Two graphs.** An overlay graph (nodes, receiver-covering trees, relay fan-out, control routes)
   mapped onto a physical resource graph (directed link capacity, propagation delay, finite byte
   queues, shared resources: NICs, bottleneck links, relay service). The overlay→physical incidence
   is what makes tree rates endogenous (the controlled A4 violation).
2. **Transport = days TCP per overlay hop.** Relays terminate TCP and re-send on per-child
   connections (store-and-forward at the logical frame level, mirroring tcp.rs:133). Data trees get
   per-tree connections; control uses a separate connection (mirroring scope.rs:5). Backpressure
   must traverse the real buffer chain (inbox ← channels ← TCP rcv buf ← flight ← snd buf ←
   scheduler ← fan-out) — never propagate a receiver stall to the source instantaneously.
3. **Relay semantics mirror the code, configurably.** Sequential per-child admission (the
   processor.rs:1218 behavior) is the default, with a concurrent-admission variant as an A/B knob.
   Per-child bounded queues. Scheduler-capacity release at socket handoff, not delivery
   (writer.rs:75).
4. **Receiver admission is a first-class policy axis**: (a) current hybrid — drop data on full
   inbox, keep control lane; (b) naive blocking; (c) per-receiver isolated credit. Decoder/sink
   service time is an explicit service center (no CPU simulator).
5. **Codec stays DoF-level** (pooled: one rank bucket; striped: per-stripe buckets), with real frame
   sizes and finite initial symbol counts. Selected cells may replay delivery traces offline through
   the real decoder; codec algebra never enters the event loop.
6. **Determinism discipline**: single-threaded per run, parallelism only across (scenario, seed,
   protocol) runs with stable-sorted output; counter-based domain-separated PRF keyed by
   (master_seed, scenario, component, process, draw_index); stable IDs from sorted topology; no
   hash-map iteration order in any output; committed CSVs reproduce byte-for-byte; versioned
   scenario schema + simulator version + seeds recorded in every artifact.
7. **Same-time phase order is explicit policy**: service/credit completions → arrivals → control
   delivery/ack joins → timer expiry → actor decisions/admissions → newly eligible service. An ack
   arriving exactly at a pacing deadline wins (L3 intent).
8. **Do not simulate the conclusion**: no pre-assigned per-tree output rates; no pooled-traffic
   priority; no zero-latency control; no ideal ack for one protocol and queued ack for another; no
   tuning transport parameters against the rounds-vs-carousel result (calibrate only on
   protocol-independent probes); background traffic as explicit flows (occupancy replay only for
   future calibration mode).
9. **Buffers in BDP units.** Queue identity stays distinct (per-child fan-out queue vs scheduler
   queue vs socket buffers vs receiver inboxes) — never one collapsed "buffer size" scalar.
10. **Metrics: critical-path attribution is the point.** Every completion decomposes into
    serialized / propagating / queued / transport-credit-blocked / relay-service-blocked /
    receiver-service-blocked / feedback-waiting time. Plus: per-resource utilization, queue
    occupancy, drop/block counts per queue identity, ack latency, tail emissions.
11. **Seed budget**: 32 screening / 128 main / 512 only for decisive boundary cells; never repeat
    the old 2M-outcome sweep at frame level.

## Stages

- **W0 — vertical slice** (proves the foundation): wansim scaffold, days pinned, one tree
  (sender → relayA → {recv1, relayB}, relayB → {recv2, recv3}) over a small physical graph, hop-wise
  TCP, Relay actor with sequential fan-out + bounded per-child queues, Receiver actor with hybrid
  inbox policy + DoF bucket, no protocol feedback yet (fixed K source frames, completion = K DoF at
  every receiver). Gate: byte-for-byte CSV reproduction across two runs; conservation invariants
  (reliable no-drop config ⇒ every receiver gets exactly K, in order per hop); main-workspace gate
  untouched. Report: `plans/wansim-w0-report.md` incl. an honest assessment of days' extension
  points (any friction ⇒ decide extend-vs-fork before W1).
- **W1 — §P endpoints + experiment 0**: independent rounds + carousel endpoints (BlockAck join,
  debounce/heartbeat, AckProbe, SessionComplete, liveness clocks), striped baselines, then the
  strict-conservation falsifier: no-drop strict-backpressure ⇒ expect zero repair deficits,
  rounds ≈ carousel local completion, carousel pays only its ack tail, pooling still beats striping
  under unequal paths. If carousel's gain survives strict conservation, the simulator (or our
  understanding) is wrong — stop and diagnose. Plus closed-form/metamorphic validation suite
  (tandem link, fork/join, zero/infinite buffer collapse to the opportunity model, edge-disjoint
  non-coupling).
- **W2 — straggler × admission**: the three-way policy A/B crossed with receiver service rate
  {1×, ½×, ⅒×}, buffer budget {0.25, 1, 4} BDP, slow-receiver count, fan-out degree, child order.
  Measures barrier completion AND the externality on fast receivers. Output: a recommendation on
  whether to implement isolated credit in the production runtime.
- **W3 — coupling + control asymmetry**: physical-overlap {0, 25, 50, 100}% between trees with
  aggregate capacity and TCP flow count controlled; report pooling advantage (vs strongest
  per-stripe FEC) SEPARATELY from path-diversity advantage (vs best single tree); TCP flow-count
  bias quantified; control/data asymmetry (ack incast, reverse-path bursts, bufferbloat) against
  BlockAck cadence knobs.
- **W4 (deferred) — calibration**: instrumented ns-lossless runs + Arbutus probes per the
  calibration ladder; fit on protocol-independent probes; blind prediction; only then use the word
  "twin".

Each stage: commits grouped by concern, wansim gate green, results under `results/wansim/`,
stage report in `plans/`, review by Claude before the next stage (same discipline as Stages 0–5).
