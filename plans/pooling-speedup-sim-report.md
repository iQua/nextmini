# Pooled cross-tree FEC speedup simulation report

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Evidence class: **model-level evidence under an ideal-code/DoF abstraction. This is not a real-codec benchmark and not a WAN measurement.**

## Verdict

The simulation separates two independent reasons that pooled, work-conserving FEC finishes sooner:

1. **Pooling removes stripe ownership.** A successful delivery on any tree advances the receiver's one shared DoF bucket. With striping, capacity on a completed stripe cannot pay a deficit on another stripe. Static heterogeneity mainly punishes a wrong split; time-varying rates and burst loss punish even the strongest continuously coded per-stripe baseline.
2. **The carousel removes repair barriers.** Pooled rounds and pooled carousel have identical ownership semantics, but rounds idle every tree while waiting for deficit feedback. The rounds-to-carousel completion gap has Pearson `r = 0.961130411` with `RTT * stationary loss` across 810 cell means.

The controlled contrasts are deliberately separate:

```text
per-stripe continuous ideal FEC  --remove ownership-->  pooled carousel
pooled rounds                    --remove barriers-->   pooled carousel
```

The predictions are not all confirmed:

- **P-a: pass, with a useful refinement.** Homogeneous no-loss rate-proportional striping and pooling match exactly: zero mean and zero maximum absolute paired gap over 4,608 trials. Known static heterogeneity can also be apportioned almost perfectly; the large gap appears with a wrong static split or with time variation that a fixed split cannot follow.
- **P-b: structural mechanism supported; strong statistical wording rejected.** Ownership waste correlates positively with the strongest-striping completion gap (`r = 0.689172597`), but emission gap correlates more strongly (`r = 0.886551973`). The model establishes ownership as the reason a delivery is non-innovative, but the proposed “not emission count” correlation test does not hold over the crossed matrix.
- **P-c: timing clause passes; tail clause is conditional.** The timing gap grows strongly with RTT and loss. Sender tail emissions are small at RTT 8 and 64—mean 0.214% and 1.855% of carousel emissions—but rise to 12.734% at RTT 512. The tail is approximately loss-invariant at a fixed RTT, not roughly constant across RTTs.

No production code, wire format, manifest surface, or runtime selection changed.

## Model and coupling

### State variable

The simulator uses the theory's own state variable. Receiver `i` has an integer pooled rank

```text
r_i(t) = min(K, distinct innovative pooled deliveries through t),  K = 8192.
```

For an ownership protocol, receiver `i` instead has one bucket `r_i,j` for every tree/stripe `j`, capped at that stripe's exact integer quota. The object completes only when every stripe bucket reaches its quota. Every successfully delivered fresh ideal-coded frame is innovative until its eligible bucket is full. There is no codec, peeling graph, coding overhead, or rank deficiency in this model.

“Emitted,” “delivered,” “decoded,” cumulative completion feedback, rounds, and carousel follow the vocabulary in [section P of the normative plan](perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages). This simulator does not reproduce the full section-P state machines; it isolates ownership and feedback timing.

### Sample-path coupling

For each `(m, rate profile, loss profile, seed)`, one trace lazily generates potential `(tick, tree, receiver)` delivery opportunities. All five protocols query that same trace from tick 1:

- the same tree has an opportunity at the same tick;
- the same receiver either receives or loses that opportunity;
- a protocol waiting at a barrier does not emit, but the exogenous trace and Gilbert-Elliott state still advance;
- multicast emissions count once at the sender even when several receivers deliver them;
- receiver/tree channel streams are independently seeded, while all protocols share each realized stream.

Thus comparisons within a seed are paired sample paths, not independent redraws that could give one protocol a luckier channel.

### Rate profiles

Rates use exact eighths of one opportunity per tick.

- Static `q:1`, `q in {1,2,4,8}`: tree 0 emits at `8/8`; every other tree emits at `1/q`. The label is fast-tree rate to each slow-tree rate.
- Periodic fast/slow alternation: one tree emits at `8/8`, the rest at `1/8`; the fast role rotates every 256 ticks.
- Seeded bounded random walk: every tree starts at `4/8`; every 64 ticks a domain-separated deterministic draw changes its rate by `-1`, `0`, or `+1`, bounded to `[1/8, 8/8]`.

Equal quotas split `K` by exact quotient/remainder. Static rate-proportional quotas use largest-remainder apportionment from the known rates and sum exactly to `K`. The two time-varying profiles use equal nominal quotas because no fixed future proportion exists.

### Loss profiles

- no loss;
- BEC 0.5%;
- BEC 2%;
- short-burst GE: `P(good->bad)=1/1000`, `P(bad->good)=1/10`;
- long-burst GE: `P(good->bad)=1/10000`, `P(bad->good)=1/100`.

Both GE profiles start in their stationary state, erase with probability `1/1000` in the good state and 1 in the bad state, and therefore have the same exact stationary erasure rate `11/1010 = 1.0891089%`. These are the Stage 3 channel definitions; only burst duration differs.

### Five protocols

1. **Equal-split striping.** Send the exact equal quotas, wait one RTT for feedback, then send the largest reported remaining deficit for each stripe. Repairs are generously treated as fresh ideal DoFs within that stripe.
2. **Rate-proportional striping.** The same feedback protocol with exact proportional quotas for static profiles and equal nominal quotas for dynamic profiles.
3. **Per-stripe FEC.** The strongest ownership-only baseline: every tree continuously sends fresh ideal stripe-local DoFs, with no repair barrier and oracle-immediate stop when the last receiver finishes the object. This deliberately favors ownership over the deployed protocol.
4. **Pooled rounds.** Send `K` fresh global DoFs work-conservingly, wait one RTT for every receiver's deficit, send the maximum deficit, and repeat. The last local receiver completion defines the barrier metric; the final feedback arrival is retained separately as sender-stop time.
5. **Pooled carousel.** Continuously emit fresh pooled DoFs until every receiver has reached `K` and its cumulative completion acknowledgement arrives one RTT later. The simulator reports both successful deliveries after each receiver's local completion and sender emissions after the last receiver's local completion.

The first two baselines need feedback to recover loss; protocol 3 removes that weakness so that its contrast with protocol 5 isolates ownership alone. Protocols 4 and 5 both pool, so their contrast isolates repair barriers.

## Matrix, metrics, and artifacts

The crossed matrix is:

```text
m              = {2, 4, 8}
rate profiles  = {1:1, 2:1, 4:1, 8:1, periodic, bounded random walk}
loss profiles  = {none, BEC 0.5%, BEC 2%, GE short, GE long}
receivers      = {1, 3, 8}
feedback RTT   = {8, 64, 512} ticks
K              = 8192
seeds/cell     = 512
```

This produces 810 parameter cells, 4,050 protocol cells, 414,720 parameter-cell trials, and 2,073,600 protocol outcomes. All aggregates retain exact integer sums. P95 is nearest-rank `ceil(0.95*n)`; decimal means are produced only at CSV output.

Artifacts under `results/speedup-sim/`:

| File | Data |
| --- | --- |
| `summary.csv` | 4,050 rows: flattened receiver mean/P95, per-trial barrier `max(receiver completion)` mean/P95, sender stop, emissions, ownership waste, receiver-counted tail deliveries, sender-counted tail emissions, weighted utilization |
| `receivers.csv` | 16,200 rows: exact sum, mean, and P95 completion tick for each receiver index in every cell |
| `tree-utilization.csv` | 18,900 rows: per-tree emitted/available exact sums and utilization |
| `decomposition.csv` | 810 rows: paired strongest-striping-minus-carousel and rounds-minus-carousel gaps, P95s, waste, emissions, and tail accounting |
| `prediction-tests.csv` | Frozen P-a/P-b/P-c statistics |
| `autoresearch.{md,jsonl,sh}` | Experiment ledger and reproduction entry point |

Every row labels itself `MODEL-LEVEL EVIDENCE - ideal DoF - NOT WAN measurement`.

## Representative complete cell

This diagnostic cell uses `m=4`, static `8:1`, BEC 2%, three receivers, RTT 64, and 512 coupled seeds. Receiver completion columns combine the three receiver indices; `receivers.csv` preserves each index separately. Barrier completion is the maximum receiver completion in each trial.

| Protocol | Mean receiver tick | P95 receiver | Mean barrier tick | P95 barrier | Mean emissions | P95 emissions | Mean ownership waste | Mean tail deliveries | Mean tail emissions | Weighted utilization |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Equal split | 16,875.926 | 16,968 | 16,918.326 | 16,992 | 8,381.242 | 8,397 | 65.053 | 0 | 0 | 35.899% |
| Rate proportional | 6,211.490 | 6,284 | 6,252.518 | 6,310 | 8,378.945 | 8,396 | 57.432 | 0 | 0 | 96.504% |
| Per-stripe FEC | 6,108.488 | 6,152 | 6,127.777 | 6,168 | 8,424.668 | 8,480 | 191.744 | 0 | 0 | 100.000% |
| Pooled rounds | 6,170.072 | 6,224 | 6,217.887 | 6,273 | 8,370.318 | 8,387 | 0 | 0 | 0 | 96.942% |
| Pooled carousel | 6,080.652 | 6,096 | 6,088.641 | 6,101 | 8,456.912 | 8,474 | 0 | 286.516 | 86.488 | 100.000% |

The same cell shows both mechanisms without relying on an aggregate correlation. Equal ownership badly underuses the fast tree. The strongest per-stripe protocol is still 39.137 ticks slower than pooling despite emitting 32.244 fewer frames than carousel, because carousel's additional frames are its in-flight ACK tail rather than ownership repair.

For the no-loss `m=4`, static `8:1` cell, equal split uses only 12.452% of fast-tree opportunities while each slow tree is about 99.659% utilized. Carousel uses 100% of every tree. Exact per-tree values for every protocol and cell are in `tree-utilization.csv`.

## Decomposition 1: stripe ownership

### P-a and static allocation

At `m=4`, three receivers, no loss, and RTT 64, the local barrier gaps against carousel are:

| Rate profile | Equal-split gap | Rate-proportional gap | Strongest per-stripe gap | Strongest ownership-wasted deliveries |
| --- | ---: | ---: | ---: | ---: |
| Static 1:1 | 0.000 | 0.000 | 0.000 | 0.000 |
| Static 2:1 | 818.000 | 0.000 | 0.000 | 3.000 |
| Static 4:1 | 3,510.000 | 2.000 | 2.000 | 9.000 |
| Static 8:1 | 10,424.000 | 0.000 | 0.000 | 6.000 |
| Periodic alternation | 120.000 | 120.000 | 120.000 | 504.000 |
| Bounded random walk | 1,691.438 | 1,691.438 | 1,691.438 | 11,253.346 |

This is the important refinement to P-a. There is no irreducible pooling constant in a homogeneous deterministic channel, and even known constant heterogeneity is almost entirely removable by the right fixed split. Equal split makes the static heterogeneity gap grow dramatically. Fixed proportional split cannot predict time variation, so periodic and random-walk profiles restore an ownership gap.

The frozen P-a statistic covers `3 tree counts * 3 receiver counts * 512 seeds = 4,608` paired homogeneous/no-loss trials at RTT 64. Mean gap and maximum absolute gap are both exactly zero.

### Strongest ownership baseline by profile

These are arithmetic means of 45 cell means per profile at RTT 64, crossing all tree counts, loss profiles, and receiver counts:

| Profile | Mean per-stripe gap (ticks) | Mean ownership-wasted deliveries | Mean per-stripe minus carousel emissions |
| --- | ---: | ---: | ---: |
| Static 1:1 | 30.565 | 987.280 | -139.054 |
| Static 2:1 | 53.818 | 1,071.472 | -7.250 |
| Static 4:1 | 90.081 | 1,197.731 | 72.440 |
| Static 8:1 | 144.812 | 1,420.443 | 136.179 |
| Periodic alternation | 253.115 | 1,993.125 | 317.386 |
| Bounded random walk | 1,548.768 | 16,799.304 | 3,984.016 |

The continuously coded baseline has no repair wait. Its remaining loss is therefore ownership itself: finished tree/receiver buckets keep receiving service while some other stripe is deficient. The dynamic profiles amplify that mismatch.

### Strongest ownership baseline by loss

These are arithmetic means of 54 cell means per loss profile at RTT 64, crossing all tree counts, rate profiles, and receiver counts. “Mean cell P95” is the mean of each cell's own 512-seed P95, not a pooled quantile.

| Loss | Mean gap (ticks) | Mean cell P95 gap | Mean ownership waste | Mean emission gap |
| --- | ---: | ---: | ---: | ---: |
| None | 284.010 | 585.000 | 2,875.059 | 560.538 |
| BEC 0.5% | 290.559 | 598.722 | 2,980.601 | 579.798 |
| BEC 2% | 304.808 | 608.315 | 3,138.432 | 620.708 |
| GE short | 346.882 | 696.963 | 3,811.599 | 717.047 |
| GE long | 541.374 | 1,387.630 | 6,752.105 | 1,158.340 |

The two GE profiles have the same stationary erasure rate, yet long bursts produce much more ownership waste and a much larger completion tail. Variance and burst duration matter independently of mean loss.

More receivers and trees also increase the ownership extreme: at RTT 64 the mean strongest-stripe gap rises from 314.589 to 349.544 to 396.446 ticks for 1, 3, and 8 receivers, while mean ownership waste rises from 791.826 to 2,697.338 to 8,245.514 deliveries. Across `m=2,4,8`, the corresponding gap is 267.961, 374.509, and 418.109 ticks.

### P-b verdict

Across the 270 RTT-64 cell means:

```text
corr(completion gap, ownership waste) = 0.689172597
corr(completion gap, emission gap)    = 0.886551973
corr(ownership waste, emission gap)   = 0.773195847
```

A descriptive partial correlation gives `corr(gap, waste | emission gap) = 0.012591997`, versus `corr(gap, emission gap | waste) = 0.769693941`. These are post-hoc summaries over a designed, heterogeneous grid—not causal estimates or independent observations.

Therefore P-b's weaker claim passes: ownership waste is positive and grows with the gap. Its stronger “not emission count” claim fails under the specified correlation test. Mechanistically, ownership defines why delivered work cannot cross buckets; empirically, aggregate emissions are the better direct proxy for elapsed service over this work-conserving model. The representative cell above also shows that emissions alone are not sufficient, because carousel may emit more due to its ACK tail and still complete locally sooner.

## Decomposition 2: rounds barriers

The following mean rounds-minus-carousel local barrier gaps average 54 equal-weight cell means per `(loss, RTT)` across tree count, rate profile, and receiver count:

| Loss | RTT 8 gap | RTT 64 gap | RTT 512 gap |
| --- | ---: | ---: | ---: |
| None | 0.000 | 0.000 | 0.000 |
| BEC 0.5% | 8.789 | 77.280 | 622.510 |
| BEC 2% | 14.637 | 129.750 | 1,045.131 |
| GE short | 9.316 | 80.448 | 650.600 |
| GE long | 10.483 | 81.669 | 632.489 |

No-loss objects finish in the initial `K` emissions, so neither pooled protocol needs a repair round and their local barrier ticks match. Once repair is needed, rounds leave all trees idle for feedback. BEC 2% grows from 14.637 ticks at RTT 8 to 1,045.131 ticks at RTT 512. The global descriptive correlation of mean rounds gap with `RTT * stationary loss` is `0.961130411` over all 810 cells.

The two GE means are close because they share stationary loss, but long GE has a higher mean cell P95 at RTT 64 (148.778 versus 130.667 ticks). Bursts change the tail even when the mean barrier penalty is governed mainly by RTT and average repair demand.

Carousel pays for work conservation with frames already in flight before the cumulative ACK arrives:

| RTT | Mean rounds gap | Mean rounds minus carousel emissions | Mean carousel tail deliveries | Mean carousel tail emissions | Mean sender tail-emission fraction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 8.645 | -17.894 | 265.283 | 17.856 | 0.214% |
| 64 | 73.830 | -158.499 | 821.682 | 158.241 | 1.855% |
| 512 | 590.146 | -1,280.232 | 5,266.213 | 1,279.737 | 12.734% |

Tail deliveries and tail emissions are deliberately different units. A multicast tail emission is counted once at the sender; it can be delivered to several already-complete receivers. The near equality between negative rounds/carousel emission gap and carousel tail emissions shows the decomposition directly: rounds usually send about the same useful repair volume, but trade the extra in-flight carousel tail for long idle barriers.

At a fixed RTT, mean tail emissions are nearly invariant across loss profiles: about 17.8, 158.2, and 1,280 frames. Across RTT, however, they scale with the feedback flight. Thus P-c's “roughly constant tail” is true only with respect to loss at fixed RTT, and the “small” label is not credible at RTT 512.

## Falsifiable-prediction summary

| Prediction | Verdict | Evidence |
| --- | --- | --- |
| P-a: homogeneous no-loss proportional striping matches pooling; gap grows with mismatch/variance | **Pass, refined** | Exact zero over 4,608 paired trials. Static equal-split gap reaches 10,424 ticks at 8:1, while correct proportional allocation stays within 0–2 ticks; random-walk strongest-ownership gap is 1,691.438 ticks in the diagnostic slice. |
| P-b: ownership waste, not emission count, explains striping gap | **Mixed; reject strong wording** | Positive waste correlation `0.689`, but emission-gap correlation `0.887` and partial results favor emissions as the direct proxy. Ownership remains the structural cause of unusable delivered DoFs. |
| P-c: rounds gain grows with RTT x loss; emission overhead is a small constant tail | **Timing pass; tail qualified** | Timing correlation `0.961`. Tail is loss-stable at fixed RTT and 0.214%/1.855% at short RTTs, but 12.734% at RTT 512 and therefore not constant across RTT. |

## Determinism and conformance tests

`mettle/tests/pooling_speedup_sim.rs` adds seven tests:

- `homogeneous_no_loss_rate_proportional_striping_matches_pooling`
- `coupled_seed_reproduces_every_protocol_metric_exactly`
- `ownership_waste_appears_under_static_misallocation`
- `carousel_feedback_tail_grows_with_rtt_but_not_local_completion`
- `integer_accounting_reconciles_for_every_protocol`
- `burst_profiles_reuse_stage_three_stationary_loss_definition`
- `coupled_trace_has_a_stable_committed_metric_vector`

The stable vector pins receiver ticks, barrier and sender-stop ticks, total emissions, ownership waste, tail deliveries, tail emissions, and per-tree emissions for all five protocols on one periodic/long-GE seed. A second complete 512-seed run reproduced every CSV byte-for-byte.

Committed SHA-256 digests:

```text
c034ba8bbdd5030b9f0b26438ab13c13b0b9c4c6811fcc0152288109e2fd9cbb  decomposition.csv
3c2dcf7c20750bf3ff64ca12da17e5be54f9efe38aeb277fdb276c25dcfe2d8d  prediction-tests.csv
b04965e3a17445eb336e005a6fbb37cbc7277935205b2094b660f13bb2ad64b8  receivers.csv
82be6c6a1ace92cef30198738a51b1ae09c8a93feb35129cecae064e119e6748  summary.csv
670647e380e48a7e30a94bcd0396f732c92c7313f6e45b92e0a401ced5784407  tree-utilization.csv
```

## Limitations

- This is a fluid, ideal-innovation abstraction. It does not execute METTLE, RaptorQ, or a peeling decoder, and does not model finite-code failure, coding overhead, CPU cost, memory pressure, or construction latency.
- No real-codec cross-check was added. The optional Stage 3-style peeling comparison would test a particular graph, not the ideal-innovation premise used to isolate these scheduling mechanisms; the result should not be presented as validating real METTLE behavior.
- There is no CPU/queueing contention, packet serialization, congestion control, correlated tree failure, cross-receiver loss correlation, or WAN reordering. Tree rates are exogenous service opportunities.
- Control messages are reliable and arrive after a fixed RTT. ACK loss, `AckProbe`, completion-handshake races, liveness timeouts, and section-P retransmission rules are outside this model.
- Static proportional striping knows the nominal static rates exactly. Dynamic profiles intentionally have only an equal fixed nominal split; a predictive/adaptive stripe allocator could narrow their gap.
- GE state advances on every potential service opportunity, including when a protocol is idle. This represents an exogenous time-varying channel rather than a transmission-triggered channel.
- Results cover `K=8192` and the stated rate/loss matrix. There are 512 paired seeds per cell, but no claim that these synthetic profiles span WAN behavior. P95s have finite-sample resolution, and the across-cell Pearson values are descriptive over a designed grid.
- The two gaps should not be added as if they were linear components. They are independent controlled contrasts with interactions through loss, receiver extremes, and the carousel ACK tail.

These limitations are why every artifact is labeled **model-level evidence, not WAN measurement**.

## Reproduction

Measured host: Apple M3 Pro (11 logical CPUs), Darwin arm64 24.6.0, Rust 1.96.0, cargo-nextest 0.9.95. The recorded full sweep invocation took 21.58 s wall, 47.78 s user, and 9.28 s system; runtime is incidental and is not a benchmark claim.

From the repository root:

```sh
export CARGO_INCREMENTAL=0
export PYO3_PYTHON=/opt/homebrew/bin/python3.13

# Exact committed sweep
results/speedup-sim/autoresearch.sh 512 results/speedup-sim

# Equivalent direct command
cargo run --release -p mettle --example pooling_speedup_sim -- \
  sweep 512 results/speedup-sim

# Independent deterministic reproduction
cargo run --release -p mettle --example pooling_speedup_sim -- \
  sweep 512 /tmp/pooling-speedup-reproduction
for file in decomposition.csv prediction-tests.csv receivers.csv summary.csv tree-utilization.csv; do
  cmp "results/speedup-sim/$file" "/tmp/pooling-speedup-reproduction/$file"
done

# Harness gate
cargo fmt --all -- --check
cargo clippy -p mettle --all-targets -- -D warnings
cargo nextest run -p mettle

# Full workspace gate
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run
```

Final gate: formatting clean; workspace clippy clean under `-D warnings`; **828 tests passed, 0 failed, 17 intentional skips**. The prior branch baseline was 821 passing tests, so the increase is exactly the seven simulator conformance tests.
