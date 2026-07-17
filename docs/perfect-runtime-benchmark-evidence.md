# Perfect runtime benchmark evidence

This document consolidates the final benchmark evidence for the perfect-FEC branch. The numbers are
**evidence, not CI performance gates**. Correctness and admission policy are asserted separately in
[`perfect-runtime-invariants.md`](perfect-runtime-invariants.md).

Two different kinds of evidence are recorded:

1. a deterministic logical-trace comparison of Rounds and Carousel; and
2. host-sensitive release measurements of the selected dense METTLE decoder at the negotiated
   prefix cap.

They must not be combined into a wall-clock throughput claim. The trace has no wall clock, decoder,
or real queueing; the decoder spike does not run the end-to-end Rounds/Carousel protocol.

## Interpretation and tolerances

- The Rounds/Carousel trace treats differences within **5% emitted data symbols** or **10%
  completion ticks** as equivalent when interpreting this synthetic model. These are interpretation
  bands, not assertions in CI. The test checks that a fixed seed is deterministic, but it does not
  require Carousel to dominate Rounds or pin a relative-performance threshold.
- The decoder spike is manually compared with the selected **25 ms construction budget** and
  **192 MiB per-decoder reservation**. Those are admission/engineering budgets, not expected-value
  tolerances and not cross-platform guarantees.
- Wall-clock and RSS values depend on host load, allocator state, toolchain, and operating-system
  accounting. Small differences between reruns are not regressions by themselves. The accepted
  policy is manual release remeasurement at stage gates, documented under
  [Automated performance thresholds](../plans/perfect-fec-runtime-questions.md#automated-performance-thresholds-review-item-9).

## Rounds versus Carousel logical trace

### Method

The executable harness is
`dataplane/tests/fec_carousel_conformance.rs::rounds_vs_carousel_matched_seed_trace_benchmark_evidence`.
The original evidence is preserved in
[`plans/stage1-rounds-carousel-benchmark-evidence.md`](../plans/stage1-rounds-carousel-benchmark-evidence.md).

- Each run models 24 RaptorQ blocks with `K=8` and three peers.
- Backend overhead `h in 0..=2` is a deterministic function of `(seed, block, peer)`.
- Delivery is a pure function of `(seed, loss profile, block, ESI, peer)`, so both protocols see the
  same result whenever they emit the same symbol.
- Rounds emits the exact source prefix, waits an eight-tick report barrier, and then sends the
  maximum reported per-block deficit in each repair round.
- Carousel emits the same source prefix, then one fresh repair for each globally incomplete block.
  It joins cumulative acknowledgements every seven ticks, with one in five acknowledgement
  opportunities deterministically lost.
- `emitted` counts data symbols submitted to the model. `ticks` count data emissions plus modeled
  control delay. Neither metric is bytes, CPU time, or wall-clock latency.

### Recorded results

| Loss | Seed | Rounds emitted | Rounds ticks | Carousel emitted | Carousel ticks | Emission delta | Tick delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5% | `0x5eed0001` | 253 | 277 | 259 | 259 | +2.37% | -6.50% |
| 5% | `0x5eed0002` | 252 | 276 | 259 | 259 | +2.78% | -6.16% |
| 5% | `0x5eed0003` | 253 | 277 | 259 | 259 | +2.37% | -6.50% |
| 15% | `0x5eed0001` | 280 | 312 | 287 | 287 | +2.50% | -8.01% |
| 15% | `0x5eed0002` | 282 | 322 | 287 | 287 | +1.77% | -10.87% |
| 15% | `0x5eed0003` | 283 | 315 | 287 | 287 | +1.41% | -8.89% |
| 30% | `0x5eed0001` | 343 | 383 | 350 | 350 | +2.04% | -8.62% |
| 30% | `0x5eed0002` | 349 | 397 | 357 | 357 | +2.29% | -10.08% |
| 30% | `0x5eed0003` | 354 | 402 | 357 | 357 | +0.85% | -11.19% |
| **Aggregate** | nine traces | **2,649** | **2,961** | **2,702** | **2,702** | **+2.00%** | **-8.75%** |

Within this model, Carousel consistently removes the explicit round barrier at the cost of slightly
more emission. Per-row completion-tick reductions range from 6.16% to 11.19%; three rows cross the
10% interpretation band, while the 8.75% aggregate reduction remains inside it. The evidence
therefore motivates real-network measurement but does not establish a general performance win.

The harness omits real queueing, packet sizes, RaptorQ's decoder-overhead distribution, CPU cost,
correlated loss, and control/data path latency. No production sizing or SLO should be inferred from
these logical ticks.

## Dense METTLE decoder release spike

### Method

The manual harness is `mettle/examples/decoder_layout_spike.rs`. The Stage 3 gate repeated the three
required dense-layout scenarios at the deployed cap geometry:

- `N=65,536` source symbols;
- `T=1,400` payload bytes;
- interior `c=1/20`; and
- one isolated release-profile process per scenario.

`construct` builds the checked dense graph and submits no bin. `terminal-jump` submits an actual edge
bin of the terminal source first, exercising extreme valid reorder. `prefix-stall` permanently drops
every edge bin for source zero and delivers the rest, forcing decoded future payload to accumulate.
The original instrumentation and host methodology are described in
[`plans/stage2-spike-report.md`](../plans/stage2-spike-report.md); the final branch remeasurement and
raw command ledger are in [`plans/stage3-sim-report.md`](../plans/stage3-sim-report.md) and
[`results/stage3/decoder-release-spike.csv`](../results/stage3/decoder-release-spike.csv).

### Stage 3 remeasurement

| Scenario | Dense construction | Process peak RSS | Buffered-payload upper bound | Maximum synchronous `push_bin` |
| --- | ---: | ---: | ---: | ---: |
| Construct only | 9.991 ms | 13.844 MiB | 0 MiB | 0 ms |
| Terminal-source bin first | 7.854 ms | 13.844 MiB | 0.342 MiB | 0.0079 ms |
| Permanent leading stall | 7.716 ms | 116.109 MiB | 87.839 MiB | 0.0132 ms |

The maximum observed construction time used 39.96% of the 25 ms budget. The worst observed RSS used
60.47% of the 192 MiB reservation. These results support dense construction on a blocking worker
before `Ready`, one decoder per session at a time, and the four-permit default. CI asserts those
control-flow and permit rules; it does not assert the RSS or wall-clock values.

These measurements cover the deployed cap geometry only. They do not validate unbounded streams,
more than 65,536 sources per prefix, payload geometry above the negotiated 96 MiB source-prefix cap,
or a different allocator/platform.

## Reproduction commands

Every Cargo command uses the branch-standard environment:

```sh
export CARGO_INCREMENTAL=0
export PYO3_PYTHON=/opt/homebrew/bin/python3.13
```

Deterministic trace:

```sh
cargo nextest run -p dataplane --test fec_carousel_conformance \
  -E 'test(rounds_vs_carousel_matched_seed_trace_benchmark_evidence)'
```

Manual dense release spike:

```sh
cargo build --release -p mettle --example decoder_layout_spike
target/release/examples/decoder_layout_spike dense construct 65536 1400
target/release/examples/decoder_layout_spike dense terminal-jump 65536 1400
target/release/examples/decoder_layout_spike dense prefix-stall 65536 1400
```

The trace command may run in ordinary CI because it is deterministic and fast, but it carries no
performance threshold. The release commands remain manual evidence under the remeasurement policy.
