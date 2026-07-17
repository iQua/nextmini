# METTLE Paper Notes

Source:
- Qianru Yu, Tianji Yang, Jingfan Meng, Jun Xu, "METTLE: Efficient Streaming Erasure Code with Peeling Decodability", arXiv:2602.10020.
- Extended PDF: `https://sites.cc.gatech.edu/home/jx/reprints/METTLE_isit_extended.pdf`

The first sections of this note record the paper semantics that the implementation must match. The
final section records the runtime's integration modes and deviations so that implementation evidence
is not mistaken for a claim made by the paper.

## Top-Level Semantics

METTLE is a packet-level streaming erasure code. The paper uses "symbol" and "packet" interchangeably: one source symbol and one codeword symbol are both packets.

The METTLE described and evaluated in the paper is non-systematic. The paper states that the scheme has a systematic variant, but that variant is not the one used in the paper results.

Therefore, the paper-native wire meaning is:

$$
B_z = \bigoplus_{x : z \in E(x)} P_x
$$

where:
- `P_x` is the raw source packet at source position `x`.
- `B_z` is the coded packet for bin `z`.
- `E(x)` is the set of bins touched by source packet `x`.

There is no separate class of raw source packets in the paper-native non-systematic code. A TLE bin can be degree-one under lossless progress and therefore may equal one source packet in common cases, but it is still a coded bin in the Tanner graph, not a separate systematic source symbol.

## Parameters

The main parameters are:

$$
c > 0
$$

coding overhead ratio.

$$
w
$$

time-coupling window size, measured in source-packet positions.

$$
l
$$

number of edges from each source packet to coded bins.

The paper evaluation uses:

$$
l = 4
$$

and:

$$
w = 600
$$

The non-TLE edge profile is:

$$
\eta_i \sim \mathrm{Binomial}((1+c)w, 2^{-(i-1)}), \quad i = 2, 3, 4
$$

so the three non-TLE probabilities are:

$$
(1/2,\ 1/4,\ 1/8)
$$

The paper does not define a single fixed "paper default" overhead of `c = 1/20`. It describes METTLE as a continuous-rate code and chooses `c` according to the target erasure/channel condition. Example evaluated values include `5.5%` for `BEC(0.01)`, `8%` for `BEC(0.02)`, and higher values for heavier loss.

## K / Block Size

METTLE is not a normal fixed `(n, k)` block code.

The paper uses `k` when discussing other block/fountain codes and when comparing against RaptorQ or LT. For METTLE, the source stream can be arbitrarily large and may not be known in advance:

$$
k \to \text{large stream/source prefix size}
$$

not:

$$
k = \text{small protocol FEC block size}
$$

The evaluation uses large METTLE streams, e.g. `k = 10^5` source symbols for latency/effectiveness measurement, and calls the corresponding coded stream a mega-codeword of size:

$$
N = 10^5(1+c)
$$

This matters for implementation: wrapping METTLE in many small independent finite blocks changes the paper model, because the paper's decoding latency is driven by `w`, not by a small block boundary.

## Time Coupling

For a source packet at time/order position:

$$
x = 0, 1, 2, \ldots
$$

the source position is its sequence number, not a hash-derived spatial position.

The source packet's edge support is inside its time-coupling window:

$$
[(1+c)x,\ (1+c)(x+w))
$$

The paper avoids floor/ceiling notation for readability. A concrete integer implementation must choose deterministic rounding, but the mathematical support is the half-open interval above.

## Touch-Less Leading Edge

The first edge is deterministic:

$$
h_1(x) = (1+c)x
$$

Equivalently, in the paper's right-boundary distance notation:

$$
\eta_1 = (1+c)w
$$

This edge is touch-less because distinct source positions have distinct leading-edge bin positions.

The TLE property also allows immediate release: after source packet `x` arrives, the leading-edge bin for `x` is finalized because no future source packet can touch it.

## Multi-Edge Type Non-TLE Edges

For the non-TLE edges, define:

$$
\eta_i = (1+c)(x+w) - h_i(x)
$$

where `eta_i` is the distance from the right boundary of source `x`'s coupling window.

METTLE chooses independent but non-identically distributed edges:

$$
\eta_i \sim \mathrm{Binomial}((1+c)w,\ 2^{-(i-1)}), \quad i = 2,\ldots,l
$$

For the paper evaluation:

$$
l = 4
$$

so:

$$
\eta_2 \sim \mathrm{Binomial}((1+c)w,\ 1/2)
$$

$$
\eta_3 \sim \mathrm{Binomial}((1+c)w,\ 1/4)
$$

$$
\eta_4 \sim \mathrm{Binomial}((1+c)w,\ 1/8)
$$

and:

$$
h_i(x) = (1+c)(x+w) - \eta_i
$$

The result is an exponentially decaying set of edge landing positions that move closer to the right boundary for larger `i`.

## Encoding

The encoder maintains coded bins indexed by `z`. For each source packet `P_x`:

1. Compute the four bin ids:

$$
E(x) = \{h_1(x), h_2(x), h_3(x), h_4(x)\}
$$

2. XOR the raw source payload into every touched bin:

$$
B_z \leftarrow B_z \oplus P_x,\quad z \in E(x)
$$

3. Emit finalized bins in increasing bin-id order as the departure frontier advances.

The target streaming departure rate is:

$$
(1+c)t
$$

coded packets after `t` source packet arrivals, ignoring integer rounding.

## Decoding

The decoder receives coded bins identified by bin id `z`. It reconstructs the same Tanner graph from the shared seed and packet positions.

Peeling rule:

1. For each received bin, compute which not-yet-decoded source packets still touch it.
2. If a received bin has exactly one unknown toucher, recover that source packet by taking the bin payload after XORing out already decoded neighbors.
3. XOR the recovered source packet out of all other received bins it touches.
4. Repeat until no degree-one bin remains.

The process mostly advances left-to-right, but later bins can help peel earlier erasures.

## Decoding Latency

For source packet `x`, the first coded packet containing it is the TLE bin:

$$
(1+c)x
$$

If `x` is decoded when bin `z` is received, with:

$$
z \ge (1+c)x
$$

then the paper defines source-symbol-time decoding latency as:

$$
\ell(x) = \frac{z - (1+c)x}{1+c}
$$

equivalently:

$$
\ell(x) = \frac{z}{1+c} - x
$$

Unlike LT/RaptorQ block codes, METTLE's latency is generally not a function of a fixed block size `k`; it is governed mainly by the coupling window `w` and channel conditions.

## Tail Loss

Spatial-coupling termination causes tail rate loss of roughly:

$$
(1+c)w
$$

The paper says this can be reduced by a tail compression technique to:

$$
\frac{(1+c)w}{2}
$$

and that the evaluation accounts for tail loss. Tail compression is therefore part of paper-aligned finite-stream termination, but it should not be confused with converting METTLE into a small independent block code.

## Paper-Alignment Requirements

A paper-aligned implementation should satisfy:

1. The default METTLE path is non-systematic.
2. Source payloads are XORed directly into all touched bins; no hidden triangular `q_x` source transform is applied unless explicitly implementing the systematic variant.
3. The public encoded symbols are coded bins keyed by bin id, including TLE bins.
4. `c` is explicit/configurable. A hard-coded `1/20` default is not paper-equivalent for all evaluated channels.
5. `k` is not treated as a required small block size. If the lossless protocol needs a finite transfer boundary, it should map the object or a large source prefix to one terminated METTLE stream, not many unrelated small METTLE blocks.

## Runtime Integration Modes and Deviations

The deployed behavior is deliberately mode-specific. The distinction is negotiated on the wire; an
existing Rounds configuration does not silently acquire object-stream semantics.

| Runtime mode | Segmentation and completion | Relationship to the paper |
| --- | --- | --- |
| `Rounds + METTLE` | Legacy independent finite blocks. Each block's initial phase sends its full terminated codeword and uses the Rounds `SourceDone`/`Need` contract. | **Finite-block METTLE adaptation.** Repeated independent block boundaries and repeated termination tails are not the paper's large time-coupled stream. This path remains regression-frozen. |
| `Carousel + METTLE` | One non-systematic terminated METTLE stream per deterministic, manifest-negotiated object prefix. Bins depart in bin-id order; decoded global sources are mapped directly to object offsets and committed before their watermark advances. | **Paper-native object/prefix stream path**, with an explicit deployment bound: an arbitrary object may be split into multiple negotiated prefixes. The prefix cap is an engineering deviation needed to bound decoder ownership. |

For Carousel, each prefix has at most 65,536 source symbols and checked
`source_count * symbol_bytes <= 96 MiB`. Prefix geometry is deterministic and negotiated; receiver
decoders live sequentially. These caps define the verified deployment envelope and are not limits
claimed by the paper. The mode-matrix tests are indexed in
[`perfect-runtime-invariants.md`](perfect-runtime-invariants.md).

### Finite termination tail and the small-K floor

Tail compression reduces the termination penalty but does not remove its discrete finite-stream
cost. For a terminated stream with `K` source symbols, the transmitted total overhead is:

$$
c_{\mathrm{actual}} = \frac{\mathrm{terminal\_symbol\_count}}{K} - 1
$$

It is not generally equal to the graph's interior expansion parameter `c`. At small `K`, even the
smallest interior expansion still produces a compressed termination tail whose integer symbol count
can exceed a requested total. Such a target is unattainable for that `K`; silently reporting the
requested percentage would undercount actual traffic.

The committed negative fixture applies the paper's 5.5% total-overhead target at `K=256`; that target
is below the finite tail floor and is rejected. The source-count sweep at `K=8,192`, `16,384`, `32,768`, and
`65,536` shows why the Carousel object-prefix design removes the earlier `K=256` pathology: one
prefix pays one compressed tail instead of paying one tail per small independent block. The CI
assertions are `small_prefix_target_below_tail_floor_is_explicitly_rejected` and
`object_stream_sweep_avoids_repeated_k256_termination_tails` in
[`mettle/tests/paper_coding_efficiency.rs`](../mettle/tests/paper_coding_efficiency.rs).

### Corrected finite-overhead accounting

The paper's table targets are interpreted as **total transmitted overhead including the compressed
tail**. The harness first computes the integer target:

$$
B_{\mathrm{target}} = \left\lceil K(1 + c_{\mathrm{target}}) \right\rceil
$$

and then deterministically solves for an interior `c` whose terminated departure contains exactly
`B_target` symbols. Reports expose the target total, solved interior `c`, terminal count, and actual
total overhead. At `K=100,000`, the 5.5% row therefore means exactly 105,500 transmitted symbols.
`table_iv_targets_are_solved_against_actual_finite_transmission_counts` pins this accounting in CI.

The manual Table-IV harness defaults to 4,096 trials. If it observes zero failures, the exact
one-sided 95% upper bound is:

$$
1 - 0.05^{1/4096} = 0.000731113\ldots
$$

That is enough to resolve a `10^{-3}` target, but no smaller failure probability claimed by the paper
is independently verified here. Claims below roughly `7.3e-4` are below this experiment's resolution.
The implementation's graph/profile tests verify our translation of the construction; they do not
reproduce the paper's large-stream effectiveness or latency results. See the full classification in
[`perfect-runtime-invariants.md`](perfect-runtime-invariants.md).

### Reservoir repair was a rejected beyond-paper experiment

Reservoir puncturing was designed and evaluated as an extension **BEYOND the METTLE paper**. The
paper endorses feedback and rate adaptation; it does not specify the deterministic PRF-selected
reserve set tested here.

The best Stage 3 point used 4.0039% initial wire overhead and 7% reserve overhead, for 11.0107%
actual total overhead. In 4,096 deterministic trials per channel it completed:

- 99.731% on a 2% memoryless BEC;
- 91.040% on the short-burst Gilbert–Elliott trace; and
- 64.868% on the long-burst Gilbert–Elliott trace.

The memoryless result did not compensate for the burst-loss failure. The Stage 3 research gate
therefore issued a **NO-GO**: Stage 3.2 was not implemented, reservoir fields were not added to the
manifest or wire protocol, and current Carousel repair behavior stands. The methodology and
confidence intervals are in [`plans/stage3-sim-report.md`](../plans/stage3-sim-report.md); the final
ruling is [`plans/stage3-review-claude.md`](../plans/stage3-review-claude.md).
