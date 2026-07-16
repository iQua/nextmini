# Stage 2.0 METTLE decoder memory/layout spike

Date: 2026-07-16

Branch: `perfect-fec-runtime`

Harness commit: `779d199` (`Add METTLE decoder layout spike harness.`)

Scope: Stage 2.0 measurement and recommendation only. No Stage 2.1 implementation, production
decoder-selection change, or wire/manifest change is included.

## Decision

Use **negotiated mega-prefix streams with the dense precomputed decoder** for paper-native
`Carousel + METTLE`.

- An object that fits one negotiated prefix is still one stream. Larger objects are split into
  deterministic, manifest-negotiated prefixes; prefix geometry cannot be selected locally after
  Ready.
- Cap each prefix at `65_536` source symbols and additionally require
  `source_count * symbol_bytes <= 96 MiB`, using checked arithmetic. The byte cap controls larger
  legal symbol sizes; the source-count cap controls graph construction and tail scope.
- Reserve **192 MiB per active decoder** and enforce a **four-decoder / 768 MiB process aggregate**
  admission budget. A receiver session constructs at most one prefix decoder at a time and destroys
  it before constructing its successor.
- Construct the dense decoder on a blocking worker **before Ready**. Never construct it or lazily
  materialize an unbounded rolling graph on the async receive loop.

The current mode-matrix decision remains unchanged: `Rounds + METTLE` is the regression-frozen
finite-block adaptation; paper-native object/prefix streaming exists only under Carousel.

## Harness and method

The harness is `mettle/examples/decoder_layout_spike.rs`. It runs exactly one layout/scenario per
process so peak RSS is isolated. The rolling case reaches the existing internal
`MettleDecoder::new_terminated` through the hidden test-support adapter. The dense case reaches
`new_terminated_with_precomputed_graph` through a measurement-only test-support constructor.
Production `stream::Decoder::new_terminated` and all production selection paths are unchanged.

Fixed inputs:

- METTLE overhead `c = 1/20`, seed `0x5EED_2A00_2026_0716`, four-edge paper profile, coupling window
  600.
- Source counts `2^16`, `2^18`, `2^20`, and `2^21` where construction/reorder scaling was needed.
- Symbol sizes 266 bytes (current `ceil(8500 / 32)` default geometry) and 1400 bytes (the configured
  production MTU scale).
- Source bytes are deterministic nonzero xorshift output, not zero pages, so payload RSS is not an
  artifact of untouched or trivially compressible buffers.

Scenarios:

| Scenario | Deterministic workload |
| --- | --- |
| `construct` | Construct the terminated decoder and submit no bins. |
| `ordered` | Encode and deliver the complete terminated departure in bin order. |
| `loss-reorder` | Drop a stable hash-selected 1% of bins and reverse each batch of 64 accepted bins. At `2^21`, 22,138 of 2,202,324 emitted bins were dropped and both layouts released all sources. |
| `prefix-stall` | Permanently drop every edge bin of source zero, then deliver the rest of the departure. This holds the release watermark at zero while later sources decode into the future-source buffer. |
| `terminal-jump` | Deliver an actual edge bin of the terminal source as the first bin. This models an extreme but valid cross-tree reorder jump and forces rolling graph indexing to the far position. |

Metrics:

- Construction latency and per-bin event-loop blocking are wall-clock durations from `Instant`.
  Event-loop blocking is the maximum synchronous `push_bin` call; ordinary-path maxima can include
  OS preemption, while the monotone terminal-jump scaling identifies algorithmic blocking.
- Current RSS is sampled with `ps`; steady RSS is the maximum sample in the middle 25–75% of source
  progress. Peak process RSS comes from `getrusage(RUSAGE_SELF)`. MiB below means `2^20` bytes.
- Buffered-payload bytes count received equations, decoded future sources, and the retained decoded
  prefix. Rolling is observed after every bin. Dense observation scans its dense slots at intervals
  of `ceil(source_count / 256)` bins; tables use the conservative upper bound of observed peak plus
  one sampling interval of payload.
- The bounded streaming encoder runs in the same process and can add roughly one coupling window of
  fixture state, making RSS slightly conservative for a decoder-only process.

Measurement host:

| Item | Value |
| --- | --- |
| Hardware | Apple M3 Pro, 11 cores, 18 GiB RAM |
| OS | macOS 15.7.4 (Darwin 24.6.0, arm64) |
| Rust | `rustc 1.96.0 (ac68faa20 2026-05-25)`, LLVM 22.1.2 |
| Cargo | `cargo 1.96.0 (30a34c682 2026-05-25)` |
| Build | Cargo release profile, `CARGO_INCREMENTAL=0` |

## Results

### Construction scaling

RSS is shown as steady/peak for the one-process construction case. Symbol size does not affect graph
construction; these runs used 266 bytes.

| Sources | Dense construction | Dense RSS MiB | Rolling construction | Rolling RSS MiB |
| ---: | ---: | ---: | ---: | ---: |
| 65,536 | 8.423 ms | 13.66 / 13.67 | 2.042 us | 1.47 / 1.48 |
| 262,144 | 39.452 ms | 50.11 / 50.11 | 1.875 us | 1.47 / 1.47 |
| 1,048,576 | 136.904 ms | 194.64 / 194.64 | 2.709 us | 1.67 / 1.67 |
| 2,097,152 | 276.873 ms | 387.52 / 387.52 | 2.417 us | 1.56 / 1.56 |

Dense construction is linear and predictable, at about 190–200 bytes of resident structural state
per source on this host. Rolling construction is effectively empty.

### Extreme valid reorder

Peak RSS includes construction plus the first terminal-source edge. `push` is the maximum (and only)
decoder call.

| Sources | Dense push | Dense peak RSS MiB | Rolling push | Rolling peak RSS MiB |
| ---: | ---: | ---: | ---: | ---: |
| 65,536 | 0.025 ms | 13.84 | 18.057 ms | 8.61 |
| 262,144 | 0.001 ms | 50.03 | 95.118 ms | 29.84 |
| 1,048,576 | 0.004 ms | 194.75 | 330.581 ms | 112.97 |
| 2,097,152 | 0.003 ms | 387.45 | 672.635 ms | 224.09 |

The rolling constructor does not remove graph-construction cost; it moves it onto the first
far-ahead receive event. A valid reordered bin blocked the synchronous decoder call for 673 ms at
`2^21`, and still blocked for 18 ms at the proposed prefix cap. Dense mode moves a smaller, bounded
8.4 ms cost at that cap to pre-Ready construction and makes the reordered receive event negligible.

### Ordered and realistic loss/reorder at `2^21`

`RSS` is steady/peak. `Payload` is the peak logical buffered-payload upper bound. Both scenarios
released all 2,097,152 sources.

| Scenario | Symbol bytes | Layout | RSS MiB | Payload MiB | Max push ms |
| --- | ---: | --- | ---: | ---: | ---: |
| Ordered | 266 | Dense | 388.83 / 388.84 | 2.23 | 1.619 |
| Ordered | 266 | Rolling | 4.50 / 4.64 | 0.15 | 0.239 |
| Ordered | 1400 | Dense | 397.95 / 397.95 | 11.74 | 1.842 |
| Ordered | 1400 | Rolling | 11.66 / 11.67 | 0.80 | 0.060 |
| 1% loss + reorder 64 | 266 | Dense | 394.88 / 394.88 | 2.72 | 1.693 |
| 1% loss + reorder 64 | 266 | Rolling | 9.52 / 9.61 | 0.85 | 2.161 |
| 1% loss + reorder 64 | 1400 | Dense | 413.48 / 414.38 | 14.32 | 1.718 |
| 1% loss + reorder 64 | 1400 | Rolling | 29.98 / 30.08 | 4.46 | 3.542 |

Rolling is clearly more memory-efficient when progress remains healthy and reorder is bounded. That
advantage is not sufficient for protocol selection because neither the network nor a peer-provided
bin order can guarantee that the first observed bin is near the current rolling frontier.

### Permanent leading stall

`RSS` is steady/peak. Dense payload is its conservative sampled upper bound; rolling payload is
exact. The decoded release count remains zero while `N - 1` future source payloads accumulate.

| Symbol bytes | Sources | Dense RSS MiB | Dense payload MiB | Rolling RSS MiB | Rolling payload MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 266 | 65,536 | 30.89 / 36.67 | 16.69 | 21.47 / 28.11 | 16.62 |
| 266 | 262,144 | 117.39 / 139.91 | 66.76 | 80.47 / 106.53 | 66.50 |
| 266 | 2,097,152 | 926.59 / 1,106.16 | 534.08 | 627.86 / 836.48 | 532.00 |
| 1400 | 65,536 | 91.25 / 116.38 | 87.84 | 80.91 / 106.67 | 87.50 |
| 1400 | 262,144 | 356.20 / 376.34 | 351.36 | 316.02 / 419.89 | 350.00 |
| 1400 | 2,097,152 | 2,420.36 / 2,434.33 | 2,810.93 | 2,500.36 / 3,332.72 | 2,800.00 |

At `2^21 × 1400`, one missing leading source retains 2.8 GiB of logical payload before container,
allocator, graph, and runtime overhead. macOS can compress resident pages under pressure, which is
why one dense RSS peak is below the logical payload count; the logical count, not compressed RSS,
must govern admission.

At the proposed `65,536 × 1400` cap, dense mode peaks at 116.38 MiB, including the co-resident
fixture encoder. A 192 MiB reservation leaves about 65% of measured peak again as headroom for
allocator variance, runtime queues, and platform differences. Four reservations bound decoder state
to 768 MiB process-wide.

## Recommendation and Stage 2.1 constraints

### Object stream versus negotiated mega-prefixes

Do not negotiate one unbounded stream per object. The permanent-leading-stall result demonstrates
an unavoidable `O(N * T)` future-payload retention path in both layouts. Rolling graph state cannot
remove that payload ownership, and dense graph state adds another `O(N)` baseline.

Use deterministic mega-prefix streams. Stage 2.1 should negotiate, for every stream:

1. Stream/source count no greater than 65,536.
2. Checked `source_count * symbol_bytes` no greater than 96 MiB.
3. Deterministic prefix count and final-prefix source count in the manifest so every receiver builds
   the same graph.
4. Sequential decoder lifetime per session; do not preconstruct the next prefix while the current
   prefix can still retain payload.

For the current 1400-byte scale, a full prefix carries 87.5 MiB of source payload. For larger legal
symbol sizes, the 96 MiB byte limit reduces source count below 65,536. This preserves one stream for
small objects while bounding large objects without changing the Rounds finite-block adaptation.

### Dense versus rolling

Choose dense within the bounded prefix:

- Dense costs 8.4 ms and 13.7 MiB to construct at 65,536 sources, then handles a terminal-source
  first packet in 0.025 ms.
- Rolling saves about 10 MiB in the measured 1400-byte prefix-stall peak, but a valid terminal-source
  first packet blocks the receive call for 18 ms at the cap and 673 ms at `2^21`.
- Dense construction is predictable and can be placed before Ready on a blocking worker. Rolling
  exposes an input-order-dependent allocation/CPU cliff inside `push_bin`, exactly where the async
  runtime needs bounded work.

The rolling decoder remains useful for experiments and for workloads with an externally enforced
frontier, but the current Carousel protocol permits cross-tree reorder and therefore cannot rely on
such a frontier.

## Construction and clean rejection contract

Stage 2.1 must use this order:

1. Decode and structurally validate the manifest.
2. Compute stream count, per-stream `N`, `N * T`, dense bin count, and a conservative allocation
   estimate with checked integer arithmetic.
3. Reject geometry above either hard prefix limit and acquire one 192 MiB decoder-budget permit;
   reject when the four-permit aggregate is exhausted.
4. On a blocking worker, build a **fallible** dense decoder before sending Ready. Current infallible
   `Vec::with_capacity`/`vec!` graph allocation is not sufficient: the Stage 2.1 constructor must use
   `try_reserve`/`try_reserve_exact` (or equivalent fallible containers) for all `O(N)` structures.
5. Install the manifest and send Ready only after construction succeeds.

Budget exhaustion, checked-size failure, `TryReserveError`, or blocking-task failure maps to an
internal manifest-install rejection such as `DecoderBudgetExceeded` or `DecoderAllocationFailed`.
The receiver logs session id and requested geometry, sends no Ready, releases any permit, and exits
that receiver session as `Aborted`; the sender then follows the existing Ready-quorum timeout path.
There is no panic, process OOM abort, partial manifest install, or partially initialized decoder.
An explicit wire rejection could be added in a separately reviewed protocol change, but is not
required to make local rejection clean and was not added by this spike.

## Exact commands

Build the isolated release harness:

```sh
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo build --release -p mettle --example decoder_layout_spike
```

Construction and terminal-jump matrix (one child process per row):

```sh
for layout in dense rolling; do
  for n in 65536 262144 1048576 2097152; do
    target/release/examples/decoder_layout_spike "$layout" construct "$n" 266
    target/release/examples/decoder_layout_spike "$layout" terminal-jump "$n" 266
  done
done
```

Full-stream ordered and loss/reorder matrix:

```sh
for layout in dense rolling; do
  for pattern in ordered loss-reorder; do
    for symbol_bytes in 266 1400; do
      target/release/examples/decoder_layout_spike \
        "$layout" "$pattern" 2097152 "$symbol_bytes"
    done
  done
done
```

Permanent-stall matrix:

```sh
for layout in dense rolling; do
  for n in 65536 262144 2097152; do
    for symbol_bytes in 266 1400; do
      target/release/examples/decoder_layout_spike \
        "$layout" prefix-stall "$n" "$symbol_bytes"
    done
  done
done
```

Touched-crate verification (all passed):

```sh
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo fmt --all
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo clippy -p mettle --all-targets -- -D warnings
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run -p mettle
```

`cargo nextest run -p mettle` result: 75 passed, 0 failed, 14 pre-existing ignored manual
benchmark/report tests.

## Limitations and open questions

- This is one deterministic seed on one ARM64 macOS host, not a cross-platform allocator study.
- RSS includes allocator behavior and macOS compression; the separate logical-payload bound is the
  budget authority.
- Max `push_bin` wall time is an event-loop risk bound, not a CPU-only microbenchmark, and includes
  possible scheduler preemption. The terminal-jump trend is monotone and much larger than normal
  noise.
- The harness measures codec state, not future Stage 2.1 wire framing or sink I/O.

There are no blocking open questions for Stage 2.1: the spike selects negotiated mega-prefixes,
dense decoding, the above budgets, and pre-Ready off-loop construction. No entry was added to
`plans/perfect-fec-runtime-questions.md`.
