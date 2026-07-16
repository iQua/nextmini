# Stage 1 rounds-versus-carousel benchmark evidence

This is benchmark evidence, not a correctness gate or a claim that carousel universally dominates
rounds. The executable source is
`dataplane/tests/fec_carousel_conformance.rs::rounds_vs_carousel_matched_seed_trace_benchmark_evidence`.

## Method

- Deterministic matched traces: 24 blocks, `K=8`, three peers, and backend overhead `h in 0..=2`
  derived from `(seed, block, peer)`.
- A data delivery decision is a pure function of `(seed, loss profile, block, ESI, peer)`, so both
  protocols see the same outcome whenever they emit the same symbol.
- Rounds sends the exact `K` source prefix, waits an eight-tick report barrier, and sends the maximum
  reported per-block deficit in each repair round.
- Carousel sends the same source prefix, then one fresh repair per globally incomplete block. It
  joins cumulative acknowledgements every seven ticks; one in five acknowledgement opportunities is
  deterministically lost.
- `emitted` counts data symbols handed to the simulated network. `ticks` count data emissions plus
  modeled control delay. They are logical trace units, not wall-clock measurements.
- Re-running a seed must reproduce exact counts (the test enforces this). For interpretation across
  future, more realistic harnesses, differences within 5% emitted symbols or 10% completion ticks are
  treated as equivalent. No relative-performance threshold is asserted by CI.

## Recorded results (2026-07-16)

| Loss | Seed | Rounds emitted | Rounds ticks | Carousel emitted | Carousel ticks |
|---:|---:|---:|---:|---:|---:|
| 5% | `0x5eed0001` | 253 | 277 | 259 | 259 |
| 5% | `0x5eed0002` | 252 | 276 | 259 | 259 |
| 5% | `0x5eed0003` | 253 | 277 | 259 | 259 |
| 15% | `0x5eed0001` | 280 | 312 | 287 | 287 |
| 15% | `0x5eed0002` | 282 | 322 | 287 | 287 |
| 15% | `0x5eed0003` | 283 | 315 | 287 | 287 |
| 30% | `0x5eed0001` | 343 | 383 | 350 | 350 |
| 30% | `0x5eed0002` | 349 | 397 | 357 | 357 |
| 30% | `0x5eed0003` | 354 | 402 | 357 | 357 |

Within this model, carousel trades roughly 2% more data emission for removal of the round barrier;
completion takes 6–10% fewer logical ticks. This only supports the narrow claim that the implemented
protocol is worth measuring under the planned network benchmark. It does not model queueing,
RaptorQ's full decoder distribution, CPU cost, correlated loss, or real control/data path latency.
