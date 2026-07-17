# Stage 3.0/3.1 reservoir simulation report

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Scope: Stage 3.0 and 3.1 only. Stage 3.2 was not implemented.

## Verdict

This reservoir construction is an extension **BEYOND the METTLE paper**. The paper supports
feedback and rate adaptation; it does not describe this puncturing/reserve scheme.

**Mechanical Gate 3: PASS. Research efficacy gate: FAIL. Recommendation: do not implement Stage
3.2.**

The exact reserve selection, stable vectors, sender memory bound, global emit-once invariant,
deterministic simulation, result artifacts, session tests, and decoder-cap measurements all satisfy
the plan's enumerated Gate 3 criteria. The best tested configuration nevertheless completed only
91.040% of short-burst GE trials and 64.868% of long-burst GE trials. Integrating a new wire contract
for that behavior is not justified.

## 3.0 deterministic puncturing prototype

### Exact finite geometry

For source count `N`, requested initial overhead `c_wire`, and requested reserve overhead
`c_reserve`, the prototype computes:

```text
B_wire  = ceil(N * (1 + c_wire))
B_total = ceil(N * (1 + c_wire + c_reserve))
R       = B_total - B_wire
```

It then uses the corrected Stage 2.6 binary solver to choose the interior graph expansion `c` whose
compressed finite terminal departure count is exactly `B_total`. Thus the actual initial, reserve,
and total counts are integer counts before any probability or memory metric is calculated. The
prototype reproduces the Stage 2.6 `N=100,000`, 5.5% row exactly: 105,500 terminal bins.

Tail rounding matters. At `N=8,192`, `c_wire=8%`, and `c_reserve=0.1%`, the exact counts are
`B_wire=8,848`, `B_total=8,856`, and `R=8`; independently rounding `N*c_reserve` would incorrectly
produce 9.

### Eligible positions and PRF version 1

Let `TLE(s) = floor((1 + c) * s)` for `0 <= s < N`, using the solved interior `c`. A terminal bin
position `b` is protected iff `b = TLE(s)` for any source. This is deliberately positional:

- a protected TLE position remains protected even if one or more other sources contribute non-TLE
  edges to the same equation;
- every other `b` in `[0, B_total)` is eligible, including a zero-degree equation position;
- TLE positions are injective, so the exact eligible cardinality is `B_total - N`.

PRF version 1 assigns each eligible `u128` bin position a deterministic `u64` score:

```text
low_hash = splitmix64(seed ^ 0x5245534552564531 ^ low64(bin_id))
score    = splitmix64(low_hash ^ 0x4245594f4e445031 ^ high64(bin_id))
```

The reserve is the lowest `R` lexicographic `(score, bin_id)` pairs. This SplitMix64-based keyed
score is a versioned deterministic experiment function, not a cryptographic claim. Selection keeps
only `R` candidates in a max-heap, giving one precise reconstruction definition if a later research
revision ever introduces negotiation.

Committed vector for `N=8,192`, `c_wire=8%`, `c_reserve=0.1%`, and reserve seed
`0x5354414745330001`:

```text
PRF emission order: [8754, 8756, 8759, 924, 7684, 2139, 8553, 8777]
ascending ids:       [924, 2139, 7684, 8553, 8754, 8756, 8759, 8777]
```

Tests also find a real bin position that is both some source's TLE and another source's non-TLE
edge, then prove it is excluded even when every eligible position is reserved.

### Freshness invariant

`FreshReserveEmitter` owns one emitted-id set shared across peer reports. Overlapping candidate
lists from three peers are supplied in different orders and with repetitions; the union returns
every reserve id exactly once before exhaustion. Loss of an already emitted reserve id is not made
fresh again—it remains separately classified Stage 2.4 retransmission traffic.

### Sender storage comparison

Measurements used the cap geometry `N=65,536`, `T=1,400`, `c_wire=2%`, `c_reserve=5%`:

- object payload: 91,750,400 bytes;
- exact terminal bins: 70,124;
- initial wire bins: 66,847;
- reserve bins: 3,277;
- reserve payload: 4,587,800 bytes;
- independent reserve-payload budget: 8,388,608 bytes.

| Strategy | Logical RAM for strategy | Spill bytes | Initial encode/store | Fetch all reserves | Process peak RSS | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Retain reserve payloads | 4,745,096 B | 0 | 20.360 ms | 5.472 ms | 94.938 MiB | Chosen |
| Direct recompute | 1,400 B | 0 | 20.294 ms | 139.887 ms | 90.938 MiB | Reject on latency |
| Spill | 106,264 B | 4,587,800 B | 32.799 ms | 8.041 ms | 91.156 MiB | Viable fallback |

All strategies generated the same digest, `ba1631c0cde1e383`. Direct recompute examined 1,877,884
candidate source/bin relationships and found 9,266 actual touching sources. It was 25.6 times slower
than retention for the complete reserve, confirming that regenerating an equation is not the
normal per-source `O(l)` streaming encoder path.

The largest swept configuration was separately checked at the cap: `c_wire=4%`,
`c_reserve=7%`, 4,587 reserve bins, 6,421,800 payload bytes, and 6,641,976 bytes including the
prototype index. It remains below the independent 8 MiB budget. This sender bound is separate from
the receiver's 192 MiB/four-permit decoder pool and does not solve the broader process-wide sender
cache admission question.

Prototype choice if research resumes: retain only reserve payloads under the checked 8 MiB bound;
keep spill as an explicit pressure mode; do not recompute on the repair path.

## 3.1 simulation methodology

The simulator is also an extension **BEYOND the METTLE paper**. It uses production
`edge_bin_ids_with_terminal_source_count` graph generation and the exact finite geometry above. It
performs payload-free degree peeling for speed; a test compares completion against the real
terminated encoder/rolling decoder on the same graph and delivered-bin set.

### Matrix and trials

- `N=8,192`; production sender-memory accounting uses `T=1,400`.
- `c_wire in {1%, 2%, 4%}`.
- `c_reserve in {3%, 5%, 7%}`.
- 512 independent trials for each of 63 grid cells: 32,256 channel/case trials.
- 4,096 independent trials for each of seven channels at the best grid point: 28,672 additional
  channel/case trials.
- Graph, reserve, and channel seeds use separate fixed domains and change on every trial.

Channels:

- BEC erasure probabilities: 0.1%, 0.5%, 1.0%, 1.5%, and 2.0%.
- Short-burst GE: `P(good->bad)=0.001`, `P(bad->good)=0.1`, good-state erasure 0.001,
  bad-state erasure 1.0; stationary erasure 1.0891%, mean bad-state run 10 symbols.
- Long-burst GE: `P(good->bad)=0.0001`, `P(bad->good)=0.01`, the same state erasures;
  stationary erasure 1.0891%, mean bad-state run 100 symbols.

Each trial sends all non-reserve positions through the channel, peels, and—only if stalled—emits
reserve ids in deterministic PRF order until completion or exhaustion. A reserve loss is counted
but not retransmitted in this phase. Duplicate traffic means an already delivered bin was submitted
again; it measured zero in every case because first global reserve emissions are emit-once. A future
multi-peer integration simulation would have to add post-emission Stage 2.4 retransmissions and
their duplicates as a separate metric.

Completion intervals are exact two-sided 95% Clopper-Pearson intervals, computed by inverting
binomial tails. Tests pin the known 5/10 interval and the 4,096/4,096 boundary; the latter has lower
bound 99.90998%.

### Grid summary

The full 63-row grid is in `results/stage3/reservoir-sweep.csv`. These are the three hardest/most
diagnostic channels for each rate pair:

| `c_wire` | `c_reserve` | Actual total overhead | BEC 2% | GE short | GE long | Sender reserve payload |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1% | 3% | 4.0039% | 0.00% | 0.20% | 7.42% | 344,400 B |
| 1% | 5% | 6.0059% | 0.00% | 13.67% | 50.98% | 574,000 B |
| 1% | 7% | 8.0078% | 51.17% | 50.98% | 56.25% | 803,600 B |
| 2% | 3% | 5.0049% | 0.00% | 2.73% | 47.46% | 344,400 B |
| 2% | 5% | 7.0068% | 2.34% | 30.47% | 53.13% | 574,000 B |
| 2% | 7% | 9.0088% | 92.19% | 71.29% | 58.01% | 803,600 B |
| 4% | 3% | 7.0068% | 1.95% | 31.64% | 53.32% | 344,400 B |
| 4% | 5% | 9.0088% | 93.75% | 70.12% | 59.77% | 574,000 B |
| 4% | 7% | 11.0107% | 99.61% | 90.63% | 64.45% | 803,600 B |

The best point is the largest one, so the sweep shows no lower-memory/lower-overhead point that
rescues burst behavior.

### 4,096-trial confirmation at the best point

At `c_wire=4%`, `c_reserve=7%`, the exact counts are 8,520 initial bins, 574 reserve bins, and
9,094 total bins. Actual initial overhead is 4.0039%; actual total overhead is 11.0107%.

| Channel | Initial completion | Final completion | Exact 95% CI | Mean repair emissions on repair success | P95 repair emissions | Mean reserve losses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BEC 0.1% | 93.652% | 100.000% | 99.910–100.000% | 122.804 | 331 | 0.008 |
| BEC 0.5% | 68.091% | 100.000% | 99.910–100.000% | 127.630 | 345 | 0.204 |
| BEC 1.0% | 23.950% | 99.976% | 99.864–99.999% | 124.656 | 344 | 0.938 |
| BEC 1.5% | 1.318% | 99.927% | 99.786–99.985% | 170.955 | 369 | 2.501 |
| BEC 2.0% | 0.024% | 99.731% | 99.520–99.866% | 245.583 | 408 | 4.910 |
| GE short | 12.695% | 91.040% | 90.124–91.897% | 241.631 | 498 | 2.760 |
| GE long | 45.142% | 64.868% | 63.384–66.331% | 256.697 | 526 | 3.152 |

The reservoir is effective on memoryless loss at a high total overhead, but it does not provide a
credible completion guarantee on the required burst traces. The long-burst case leaves more than a
third of objects incomplete even after all fresh reserves are available.

## Manual decoder release spike

Gate 3 requires the Stage 2 dense decoder measurements to be rerun at the cap geometry. These
measurements are independent of the beyond-paper reservoir sender budget.

| Scenario | Construction | Process peak RSS | Buffered-payload upper bound | Max synchronous push |
| --- | ---: | ---: | ---: | ---: |
| Construct | 9.991 ms | 13.844 MiB | 0 MiB | 0 ms |
| Terminal jump | 7.854 ms | 13.844 MiB | 0.342 MiB | 0.0079 ms |
| Prefix stall | 7.716 ms | 116.109 MiB | 87.839 MiB | 0.0132 ms |

All construction rows remain below 25 ms and the worst RSS remains below the 192 MiB decoder
reservation.

## Gate 3 assessment

| Criterion | Result | Evidence |
| --- | --- | --- |
| 3.1 report recorded under `results/` | PASS | Raw sweep, storage, release-spike, and experiment-log artifacts are committed under `results/stage3/`. |
| Storage within a hard sender budget | PASS | 8 MiB independent budget; worst swept cap retention plus index is 6,641,976 bytes. |
| Session tests green | PASS | Full workspace nextest: 821 passed, 0 failed, 17 intentional skips. |
| Reserve id emitted at most once before exhaustion under reordered multi-peer reports | PASS | Deterministic three-peer overlap/reorder test; emitted set cardinality equals reserve cardinality. |
| Decoder cap measurements | PASS | 9.991 ms maximum construction and 116.109 MiB maximum RSS. |
| Research efficacy sufficient for 3.2 | **FAIL** | Best point: GE short 91.040%, GE long 64.868% completion. |

The first four rows are the plan's literal Gate 3 criteria and pass. The stage is explicitly
research-gated, however, and the required burst evidence fails. Therefore the overall integration
decision is **NO-GO**.

## Reproduction commands

Every Cargo command used `CARGO_INCREMENTAL=0` and
`PYO3_PYTHON=/opt/homebrew/bin/python3.13`.

```sh
export CARGO_INCREMENTAL=0
export PYO3_PYTHON=/opt/homebrew/bin/python3.13

cargo build --release -p mettle --examples

target/release/examples/reservoir_storage_spike retain 65536 1400 2 100 5 100
target/release/examples/reservoir_storage_spike recompute 65536 1400 2 100 5 100
target/release/examples/reservoir_storage_spike spill 65536 1400 2 100 5 100
target/release/examples/reservoir_storage_spike retain 65536 1400 4 100 7 100

target/release/examples/reservoir_simulation sweep 512 8192 1400
target/release/examples/reservoir_simulation case confirmation-best 4096 8192 1400 4 100 7 100

target/release/examples/decoder_layout_spike dense construct 65536 1400
target/release/examples/decoder_layout_spike dense terminal-jump 65536 1400
target/release/examples/decoder_layout_spike dense prefix-stall 65536 1400
```

`results/stage3/autoresearch.sh` reproduces the storage and simulation CSVs.

Final verification:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run
```

Results: formatting passed; strict workspace Clippy passed; nextest ran 821 tests, with 821 passed,
0 failed, and 17 intentional skips. No selected controller test was skipped for unavailable
PostgreSQL.

## Commits

| Concern | Commit | Message |
| --- | --- | --- |
| Exact finite geometry, PRF selection, vectors, and freshness | `fe3e1d5` | `Add experimental METTLE reservoir prototype.` |
| Storage and simulation harnesses | `4b76df9` | `Add deterministic reservoir experiment harnesses.` |
| Raw sweep, confirmation, storage, and experiment results | `7552878` | `Record reservoir simulation sweep results.` |
| Combined reserve strategy budget enforcement | `cb0e863` | `Enforce the reservoir strategy memory budget.` |

## Recommendation

Do not start Stage 3.2 and do not add `c_total`, `c_wire`, reserve cardinality, PRF version, or seed
derivation to the manifest from this experiment. Preserve the current wire and runtime behavior.

If the reservoir idea is revisited, first define an explicit burst-channel completion target and
test a changed research design—such as adaptive reserve departure or a burst-aware construction—in
simulation. That future work must remain labeled an extension **BEYOND the METTLE paper**, retain an
independent process-wide sender memory budget, and separately account for post-emission reserve
retransmissions and their multi-peer duplicates.
