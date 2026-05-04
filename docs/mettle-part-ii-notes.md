# METTLE Part II Technical Notes

Source:
- Qianru Yu, Tianji Yang, Jingfan Meng, Jun Xu, "METTLE: Efficient Streaming Erasure Code with Peeling Decodability", arXiv:2602.10020.
- Part II source in the arXiv bundle is embedded as `METTLE_extended.pdf`; PDF metadata names its TeX source as `METTLE_extended.tex`, but that TeX file is not included in the public arXiv source archive.

This document is a technical reconstruction of Part II. It is not a verbatim transcription. It records the definitions, equations, parameters, metrics, and implementation-relevant semantics that must be preserved by the code.

## Preface Caveat

The arXiv version says Part I has the latest benchmark results and that Part II is supplementary material with a high-level preliminary implementation description. When Part I and Part II differ, Part I should be treated as the newer experimental source.

Concrete consequence:
- Part II often explains the scheme with `l = 3` for readability.
- Part I evaluation uses `l = 4`, `w = 600`, and non-TLE probabilities `(1/2, 1/4, 1/8)`.
- Part I explicitly says evaluation overheads include tail loss.

## 1. Introduction

The paper is about packet-level erasure coding. A source symbol and a codeword symbol are both whole packets.

Terminology:
- A lossy packet channel is modeled as an erasure channel.
- A source symbol is one uncoded packet.
- A codeword symbol is one coded packet.
- Coding efficiency means low overhead.
- Coding complexity means low encode/decode work per packet.
- Streaming means low average decode latency, measured in future coded packets needed before a source packet is recovered.

METTLE is presented as a native streaming code because its effective block size can be the whole object or stream, while decode latency is governed by the coupling window rather than by object size.

The three design goals are:
- High coding efficiency.
- Low coding complexity.
- Streaming/on-the-fly decodability.

## 1.1 Motivation

Block codes couple coding efficiency to decoding latency:

$$
\text{larger block size} \Rightarrow \text{better pooling} \Rightarrow \text{lower overhead}
$$

but:

$$
\text{larger block size} \Rightarrow \text{larger decode delay}
$$

For a block code with source block size `k`, the receiver often needs to wait for a block-scale number of packets. LT/Raptor are "rateless" in `n`, but still have an effective source block size `k` for decoding progress.

METTLE's intended separation is:

$$
\text{effective block size} \approx \text{entire object/source prefix}
$$

while:

$$
\text{decode latency} \approx O(w)
$$

where `w` is the time-coupling window.

## 1.2 METTLE Overview

METTLE stands for Multi-Edge Type with Touch-less Leading Edge.

It inherits low complexity from peeling-family codes:

$$
\text{encode/decode work per packet} = O(l)
$$

where `l` is a small constant, typically 3 to 5.

The paper highlights three systems properties:
- Hashing-based graph generation: no stored full Tanner graph is needed.
- Continuous rate: the overhead ratio `c` can be a real-valued configuration rather than fixed by an `(n, k)` block design.
- Rate adaptation: `c` can be changed as the channel changes with modest protocol cost.

The high-level operational rule is:

$$
t \text{ source arrivals} \Rightarrow (1+c)t \text{ coded departures}
$$

ignoring integer rounding.

## 1.2.1 Baseline METTLE

Baseline METTLE adapts Walzer-style spatial coupling into time coupling.

For the source packet at time position:

$$
x \in \{0, 1, 2, \ldots\}
$$

the ball position is exactly `x`, not a hash-selected random location.

Baseline edge support:

$$
h_i(x) \sim \mathrm{Uniform}\left([(1+c)x,\ (1+c)(x+w))\right)
$$

for:

$$
i = 1, \ldots, l
$$

Part II's running example often uses:

$$
l = 3
$$

The baseline peels primarily left-to-right and needs high overhead. Part II reports baseline overhead around `c = 0.21` for 1% loss at success probability 0.999.

## 1.2.2 MET and TLE

METTLE improves the baseline with two changes:
- MET: non-TLE edges have different landing-position distributions.
- TLE: the first edge is deterministic and collision-free.

Part II reports that, under 1% loss, these reduce overhead from roughly:

$$
c \approx 0.21
$$

to:

$$
c \approx 0.055
$$

for the full scheme.

## 1.2.3 Resilience and Systematic Variant

For time-varying erasure rate:

$$
r(t) \in [r_{\min}, r_{\max}]
$$

with long-term average:

$$
r_{\mathrm{avg}}
$$

the paper's claim is that METTLE can usually budget near the average rather than near the worst case:

$$
c \approx f(r_{\mathrm{avg}})
$$

instead of:

$$
c \approx f(r_{\max})
$$

When the actual loss exceeds the budget, peeling may stall. If feedback exists, the receiver can request a small retransmission around the stall, and the sender can adapt `c`.

Part II also states that TLE enables a systematic variant without changing coding efficiency. That systematic variant is not the version evaluated in Part I's main non-systematic results.

## 2. METTLE Coding Scheme

The paper introduces the scheme through:
- IBLT.
- Walzer's spatially coupled IBLT.
- METTLE's time-coupled packet code.

The explanatory language is balls and bins:
- Ball: source packet/source symbol.
- Bin: coded packet/codeword symbol.
- Edge: one XOR relationship between a source packet and a coded bin.

## 2.1 IBLT

An IBLT for a set `S` uses approximately:

$$
(1+c)n
$$

bins, where:

$$
n \approx |S|
$$

and `c > 0` is an overhead-like ratio.

Each ball is thrown into `l` bins using independent uniform hashes:

$$
h_i(x) \sim \mathrm{Uniform}\left([0,\ (1+c)n)\right), \quad i = 1,\ldots,l
$$

Each bin stores metadata sufficient for peeling. In an erasure-code version, the important payload operation is XOR over touched source packets.

The peeling decoder repeatedly finds degree-one bins, recovers that ball, and removes that ball from its other bins. With the optimal IBLT-style parameter:

$$
l = 3
$$

successful peeling needs roughly:

$$
c \ge 0.222
$$

asymptotically.

Set reconciliation identity:

$$
|A \triangle B| \triangleq (A \setminus B) \cup (B \setminus A)
$$

IBLT supports blind encoding of a set difference:

$$
\mathrm{IBLT}(A) - \mathrm{IBLT}(B) = \mathrm{IBLT}(A \triangle B)
$$

The blind-encoding requirement is the reason TLE is not compatible with Walzer's original set-reconciliation application.

## 2.2 Walzer's Scheme

Walzer's scheme keeps the balls-and-bins peeling model but adds spatial coupling.

Number of bins:

$$
(1+c)(n+w)
$$

where:

$$
w > 0
$$

is the coupling window.

The asymptotic model assumes:

$$
n \to \infty,\quad w=o(n)
$$

so the limiting overhead is still `c`.

Each ball `x` is first mapped to a random spatial position:

$$
p_x \in [0,n)
$$

Then each edge lands uniformly in the local window:

$$
h_i(x) \sim \mathrm{Uniform}\left([(1+c)p_x,\ (1+c)(p_x+w))\right)
$$

for:

$$
i = 1,\ldots,l
$$

The bin range is:

$$
[0,\ (1+c)(n+w))
$$

The sparse boundary regions are:

$$
\text{west coast}: [0,\ (1+c)w)
$$

and:

$$
\text{east coast}: [(1+c)n,\ (1+c)(n+w))
$$

These low-density regions seed peeling from both ends.

Part II gives the intuition that the expected occupancy near the first bin is close to zero and grows inland. For `l = 3`, the mean occupancy reaches roughly:

$$
\frac{3}{1+c}
$$

around bin position:

$$
(1+c)w
$$

The extra tail region is:

$$
(1+c)w
$$

bins.

## 2.3 METTLE

METTLE makes three changes to Walzer's scheme:
- Time coupling instead of spatial coupling.
- TLE.
- MET.

It also has a systematic variant enabled by TLE.

## 2.3.1 Time Coupling

For a source stream or large finite prefix, the source packet at sequence number `x` is placed at ball position:

$$
p_x = x
$$

This is possible because the sender knows packet order, unlike blind set reconciliation.

The baseline time-coupled window is:

$$
[(1+c)x,\ (1+c)(x+w))
$$

For Part II's simplified `l = 3` baseline:

$$
h_1(x), h_2(x), h_3(x) \sim \mathrm{Uniform}\left([(1+c)x,\ (1+c)(x+w))\right)
$$

independently.

Streaming departure rule:

$$
t \text{ uncoded arrivals} \Rightarrow (1+c)t \text{ coded departures}
$$

TLE later makes the `t`-th source packet appear in a coded departure at the same left boundary, giving zero artificial coding latency under no loss.

Each coded packet must carry its bin id:

$$
z
$$

so the decoder can reconstruct which source positions may touch it.

The implicit Tanner graph has:
- left vertices: source packets/balls.
- right vertices: coded bins.
- edges: touched bin ids generated from source position and shared seed.

Complexity:

$$
\text{encoding per source packet} = O(l)
$$

$$
\text{peeling work per decoded source packet} = O(l)
$$

with `l` small.

## 2.3.2 MET Edges

For a ball at source position `x`, define the right boundary of its time-coupling window:

$$
R(x) = (1+c)(x+w)
$$

For edge `i`, define:

$$
\eta_i \triangleq R(x) - h_i(x)
$$

so:

$$
h_i(x) = (1+c)(x+w) - \eta_i
$$

Baseline METTLE uses i.i.d. uniform landing positions. MET changes the non-TLE edge distributions so edges have different types.

The paper's MET idea is exponential decay in distance from the right boundary. In the Part II three-edge explanation, before TLE is fixed:

$$
\eta_2 \sim \mathrm{Binomial}((1+c)w,\ 1/2)
$$

$$
\eta_3 \sim \mathrm{Binomial}((1+c)w,\ 1/4)
$$

Part I's evaluated full METTLE uses one TLE plus three non-TLE edges:

$$
l = 4
$$

with:

$$
(\zeta_2,\zeta_3,\zeta_4) = (1/2,\ 1/4,\ 1/8)
$$

equivalently:

$$
\eta_i \sim \mathrm{Binomial}((1+c)w,\ 2^{-(i-1)}), \quad i=2,3,4
$$

The intuition is that exponential decay makes the west side of the window sparser and easier to peel left-to-right.

Part II reports that MET alone improves 1% loss overhead only modestly:

$$
c: 0.21 \to 0.18
$$

for success probability 0.999, motivating TLE.

## 2.3.3 Touch-Less Leading Edge

TLE fixes the first edge to the left boundary of the source's window.

In eta notation:

$$
\eta_1 = (1+c)w
$$

Therefore:

$$
h_1(y) = (1+c)y
$$

for:

$$
y = 0,1,\ldots,n-1
$$

TLE has two properties.

First, it is injective in the continuous/ideal model:

$$
y_1 \ne y_2 \Rightarrow h_1(y_1) \ne h_1(y_2)
$$

because adjacent TLE positions differ by:

$$
1+c > 1
$$

In an integer implementation this requires deterministic rounding that preserves distinct TLE bin ids.

Second, after source packet `y` arrives, bin:

$$
(1+c)y
$$

can be finalized immediately because no future source packet can touch it. This is the zero-latency-under-lossless-channel property.

Adding TLE to baseline without MET improves reported overhead at 1% loss from:

$$
c = 0.21
$$

to:

$$
c = 0.07
$$

The paper warns not to make all edges deterministic. For example:

$$
\eta_2 = \frac{(1+c)w}{2}, \quad \eta_3 = \frac{(1+c)w}{4}
$$

was tested and degraded coding efficiency because deterministic multiple edges introduce many short cycles. One deterministic edge per ball does not create the same girth problem.

## 2.3.4 Systematic Variant

The main scheme is non-systematic. The systematic variant is possible because of TLE.

Let the original source payloads be:

$$
p_1, p_2, \ldots, p_n
$$

placed at source positions:

$$
0,1,\ldots,n-1
$$

The TLE bins are:

$$
0,\ 1+c,\ 2(1+c),\ \ldots,\ (n-1)(1+c)
$$

The systematic target is to make:

$$
p_j
$$

appear in the `j`-th TLE bin:

$$
(j-1)(1+c)
$$

The encoder can instead solve for fake/internal ball contents:

$$
q_1, q_2, \ldots, q_n
$$

so that the TLE-bin outputs equal the desired source payloads.

Let:

$$
G \in \{0,1\}^{n \times n}
$$

be the generator submatrix restricted to the `n` TLE bins. The paper's key property is:

$$
G \text{ is upper triangular with diagonal } 1
$$

Therefore:

$$
\det(G)=1
$$

over GF(2), and `G` is full rank.

So the systematic transform is:

$$
Gq = p
$$

and:

$$
q = G^{-1}p
$$

over GF(2).

The remaining repair bins are then computed from `q`. Part II counts non-TLE repair bins as:

$$
(1+c)w + cn
$$

before tail compression.

Implementation note: Part I's reported evaluation uses non-systematic METTLE. The systematic variant should not be mixed into reproduction unless explicitly testing that variant.

## 3. Background and Related Work

## 3.1 Binary Erasure Channel and GE Variant

BEC erases each packet independently:

$$
E_j \sim \mathrm{Bernoulli}(\varepsilon)
$$

where:

$$
\Pr[E_j = 1] = \varepsilon
$$

and `E_j = 1` means packet `j` is erased.

The paper writes this as:

$$
\mathrm{BEC}(\varepsilon)
$$

GE is used for time-varying/bursty erasures and is detailed in Section 4.1.2.

## 3.2 LT and Raptor

LT code:
- Rateless in output length.
- Peeling-based.
- Each coded symbol is the XOR of a random subset of source symbols.
- Degree follows the robust Soliton distribution.
- Average degree is:

$$
O(\log n)
$$

METTLE's degree is a small constant:

$$
l \in \{3,4,5\}
$$

LT high efficiency needs large source block size, e.g. tens of thousands of packets, which causes block-scale decode latency.

Raptor code:
- Adds a precode to LT.
- Uses LDPC + HDPC style intermediate symbols.
- Avoids LT's long tail.
- Uses inactivation decoding and Gaussian elimination for harder residual systems.

Raptor's improved efficiency comes with higher decoding complexity. METTLE remains purely peeling in the paper's main design.

## 3.2.3 Three-Sigma Effect

For a block with `n` packets and erasure probability `epsilon`, the number of erasures is:

$$
X \sim \mathrm{Binomial}(n,\varepsilon)
$$

Mean:

$$
\mu = n\varepsilon
$$

Standard deviation:

$$
\sigma = \sqrt{n\varepsilon(1-\varepsilon)}
$$

For 99.9% block success under a Gaussian tail approximation, the block should tolerate roughly:

$$
\mu + 3.09\sigma
$$

erasures.

This is the "3 sigma effect": small blocks must provision above the mean by a large relative margin.

## 3.3 Reed-Solomon and Streaming Codes

RS codes are MDS and efficient in overhead for a fixed block size, but packet-level RS encode/decode is computationally expensive.

Streaming RS systems often use diagonal interleaving. The goal is to spread burst losses across multiple RS codewords. This helps latency-constrained recovery, but the code still must choose small effective blocks/deadlines and often provision for near-worst-case loss.

METTLE's contrast:

$$
\text{budget near average loss} \quad \text{rather than} \quad \text{near worst-case loss}
$$

under the paper's burst-resilience claim.

## 3.4 LDPC and Spatially Coupled LDPC

LDPC under BEC degenerates to peeling, making packet-level LDPC an important complexity comparison.

METTLE's Tanner graph allows graph-quality tools such as girth analysis.

The paper says density evolution for METTLE is hard to handle analytically because METTLE is multi-edge type. Walzer's scheme has scalar recursion because all edges are same-type uniform. METTLE gives a vector recursion.

The paper notes the available coupled-vector-recursion theory expects symmetry and monotonicity assumptions that METTLE's equations do not satisfy.

The authors therefore numerically solved density-evolution equations to:
- guide the MET distribution search.
- validate simulations for large `n`.

The equations are not printed in Part II.

## 4. Evaluation

Part II evaluates METTLE under:
- BEC.
- GE bursty channels.

Metrics:
- Coding efficiency.
- Latency.
- FSU.
- Failure probability.
- LDPC erasure tolerance comparison.
- Girth distribution.

## 4.1 Experimental Setup

Monte Carlo setup:

$$
\text{runs per data point} = 1000
$$

Source packets per run:

$$
n = 10^5
$$

Nominal codeword packets:

$$
(1+c)n
$$

Part II then separately discusses tail cost and compression.

A run is successful only if all `n` source packets are decoded. However, Part I later clarifies that isolated error-floor symbols under heavy loss are not counted as decoding failures for METTLE's coding-efficiency table.

## 4.1.1 Metrics

Fraction of symbols undecoded:

$$
\mathrm{FSU} =
\frac{\#\{\text{unrecovered source symbols}\}}{n}
$$

Failure probability:

$$
P_{\mathrm{fail}} =
\Pr[\text{not all source symbols are recovered}]
$$

Overhead ratio:

$$
c =
\frac{\#\{\text{extra codeword symbols beyond } n\}}{n}
$$

Tail cost after compression:

$$
\frac{(1+c)w}{2n}
$$

Part II words this as excluding tail from the main `c` parameter, then adding compressed tail for fair comparisons. Part I says reported overheads account for tail loss.

Latency for a source symbol recovered when coded bin `z` arrives should be normalized by the send-rate factor:

$$
\ell(x) =
\frac{z - (1+c)x}{1+c}
$$

This excludes propagation delay.

## 4.1.2 Erasure Channels

BEC:

$$
\Pr[\text{drop packet}] = \varepsilon
$$

independently for every packet.

GE channel parameters:

$$
(\epsilon,\delta,\alpha,\beta)
$$

States:
- state 0: good.
- state 1: bad.

Good-state drop probability:

$$
\epsilon
$$

Bad-state drop probability:

$$
\delta
$$

Good-to-bad transition:

$$
\alpha
$$

Bad-to-good transition:

$$
\beta
$$

The paper assumes:

$$
\delta > \epsilon
$$

When:

$$
\delta = 1
$$

the bad state creates bursts of consecutive erased packets.

Stationary average drop probability:

$$
p_{\mathrm{avg}} =
\frac{\alpha\delta + \beta\epsilon}{\alpha+\beta}
$$

Mean bad-state burst length:

$$
\frac{1}{\beta}
$$

GE configurations from Part II:

| Configuration | `alpha` | `beta` | `epsilon` | `delta` | `p_avg` |
| --- | ---: | ---: | ---: | ---: | ---: |
| VoIP | `5e-4` | `0.2` | `0.01` | `1` | `1.25%` |
| WiMAX video streaming | `0.04` | `0.05` | `0.01` | `0.02` | `1.44%` |
| Videoconferencing-light | `0.05` | `0.75` | `0.01` | `0.1` | `1.56%` |
| Videoconferencing-heavy | `0.05` | `0.75` | `0.05` | `0.5` | `7.81%` |
| Long-burst | `0.001` | `0.01` | `0.01` | `0.1` | `1.82%` |

Implementation note: if code uses `(p_good_to_bad, p_bad_to_good, epsilon_good, epsilon_bad)`, the mapping is:

$$
p_{\mathrm{good}\to\mathrm{bad}} = \alpha
$$

$$
p_{\mathrm{bad}\to\mathrm{good}} = \beta
$$

$$
\epsilon_{\mathrm{good}} = \epsilon
$$

$$
\epsilon_{\mathrm{bad}} = \delta
$$

## 4.1.3 Coding Schemes

### Full METTLE

Full METTLE includes TLE and MET.

For source position `x`:

$$
h_1(x) = (1+c)x
$$

For non-TLE edges:

$$
\eta_i \sim \mathrm{Binomial}((1+c)w,\zeta_i), \quad i=2,\ldots,l
$$

and:

$$
h_i(x) = (1+c)(x+w) - \eta_i
$$

Part II evaluation parameters:

$$
l = 4
$$

$$
(\zeta_2,\zeta_3,\zeta_4) = (1/2,\ 1/4,\ 1/8)
$$

$$
w = 600
$$

The paper says `w = 600` was chosen after checking values in:

$$
w \in [400,\ 1000]
$$

as a coding-efficiency/latency tradeoff.

### Baseline METTLE

No MET and no TLE:

$$
h_i(x) \sim \mathrm{Uniform}\left([(1+c)x,\ (1+c)(x+w))\right)
$$

for all:

$$
i=1,\ldots,l
$$

Part II uses:

$$
l = 4,\quad w = 600
$$

for this baseline's empirical best setting.

### Baseline + TLE

TLE exists:

$$
h_1(x) = (1+c)x
$$

but non-TLE edges are uniform:

$$
h_i(x) \sim \mathrm{Uniform}\left([(1+c)x,\ (1+c)(x+w))\right), \quad i=2,\ldots,l
$$

Part II uses:

$$
l = 3,\quad w = 600
$$

for this variant's empirical best setting.

### Packet-Level LDPC

A regular LDPC ensemble is defined by:

$$
(d_v, d_c)
$$

The overhead ratio is:

$$
c = \frac{d_v}{d_c - d_v}
$$

The parity-check matrix:

$$
H \in \{0,1\}^{m \times n}
$$

has:
- exactly `d_v` ones per variable column.
- exactly `d_c` ones per check row.

The paper generates `H` by random socket matching and rejects duplicate matrix entries so:

$$
H_{ij} \in \{0,1\}
$$

## 4.2 Numerical Results

## 4.2.1 Coding Efficiency Under BEC

For:

$$
\varepsilon = 0.01
$$

Part II reports:

$$
\text{METTLE reaches } P_{\mathrm{fail}} < 10^{-3} \text{ below } c = 0.055
$$

Baseline only:

$$
c \approx 0.21
$$

Baseline + TLE:

$$
c \approx 0.07
$$

For heavier loss:

$$
\varepsilon = 0.10
$$

METTLE's curve shifts right but remains usable; the baseline-only curve is omitted in the figure because it performs poorly.

### Tail Compression

Uncompressed east-coast tail:

$$
(1+c)w
$$

For:

$$
n=10^5,\quad w=600
$$

the uncompressed tail is about:

$$
\frac{(1+c)w}{n} \approx 0.6\%
$$

for small `c`.

Because METTLE only peels left-to-right, a sparse east coast does not help decoding. The paper compresses the last `w` source windows so the east coast has inland-like density.

For the last `w` balls:

$$
x \in [n-w,\ n-1]
$$

the coupling range is shrunk by a linearly increasing factor:

$$
\gamma(x) \in [1,2]
$$

with:

$$
\gamma(n-w)=1
$$

and:

$$
\gamma(n-1)=2
$$

An implementation-compatible form is:

$$
\gamma(x) =
1 + \frac{x-(n-w)}{w-1}
$$

for `w > 1`. The compressed window width is:

$$
W_{\mathrm{tail}}(x) =
\frac{(1+c)w}{\gamma(x)}
$$

rounded deterministically for integer bins.

This reduces tail cost to roughly:

$$
\frac{(1+c)w}{2}
$$

and tail overhead ratio to:

$$
\frac{(1+c)w}{2n}
$$

Part II says subsequent evaluations use tail compression.

## 4.2.2 Coding Efficiency Under Bursty Channels

The GE results are compared to BEC curves with similar average loss.

Light-loss GE cases:
- VoIP: `p_avg = 1.25%`.
- WiMAX video streaming: `p_avg = 1.44%`.
- Videoconferencing-light: `p_avg = 1.56%`.

These generally require overhead between BEC(0.01) and BEC(0.02), with VoIP showing a longer tail because its burst length is longer.

Heavy cases:
- Videoconferencing-heavy: `p_avg = 7.81%`, close to BEC(0.08).
- Long-burst: `p_avg = 1.82%`, bad-state duration about 100 packets.

The paper reports that Long-burst needs only about:

$$
0.03 \text{ to } 0.05
$$

more overhead than BEC at its average loss, and far less than budgeting for its worst bad-state loss:

$$
c \approx 0.253
$$

for BEC(0.1).

## 4.2.3 LDPC Erasure Tolerance

For LDPC comparison, rate is fixed by the ensemble, so the paper reports empirical threshold: the largest erasure probability satisfying:

$$
P_{\mathrm{fail}} < 10^{-3}
$$

Table 2 reconstructed:

| Ensemble | Code rate | Overhead `c` | Empirical threshold |
| --- | ---: | ---: | ---: |
| METTLE | `0.95` | `5.263%` | `0.01` |
| METTLE | `0.9375` | `6.667%` | `0.018` |
| METTLE | `0.934` | `7.143%` | `0.02` |
| METTLE | `0.9` | `11.111%` | `0.04` |
| LDPC `(4,80)` | `0.95` | `5.263%` | `0.031` |
| LDPC `(5,80)` | `0.9375` | `6.667%` | `0.036` |
| LDPC `(6,90)` | `0.934` | `7.143%` | `0.036` |
| LDPC `(4,40)` | `0.9` | `11.111%` | `0.068` |

The comparison accounts for METTLE compressed tail loss:

$$
\frac{w/2}{n} = 0.3\%
$$

when:

$$
w=600,\quad n=10^5
$$

The paper's conclusion is that METTLE has lower erasure tolerance than comparable LDPC, but much lower latency.

## 4.2.4 Latency

Part II reports BEC average latency for METTLE:

| Loss rate `epsilon` | Average latency, packets |
| ---: | ---: |
| `0.01` | `57` |
| `0.02` | `84` |
| `0.03` | `117` |
| `0.10` | `199` |

These are source-symbol-time packet latencies normalized by `1+c`.

## 4.2.5 Girth Analysis

The graph-quality check samples:

$$
10^4
$$

random edges from a graph with:

$$
10^5
$$

nodes and:

$$
c=0.05
$$

For each sampled edge, remove it and run BFS from one endpoint to find the shortest cycle containing that edge.

The paper compares:

$$
w=600
$$

and:

$$
w=1200
$$

Reported interpretation:
- For `w = 600`, most sampled edges are in cycles of length 6-8.
- The 25th percentile is length 6.
- Less than 5% of edges are in length-4 cycles.
- `w = 1200` improves the length-4-cycle share to about 3%.

This is used to argue that one deterministic TLE edge does not create a harmful girth problem.

## 5. Conclusion

Part II's conclusion restates that METTLE combines:
- time coupling,
- MET edge types,
- deterministic TLE,
- peeling-only decoding,
- low latency,
- high coding efficiency relative to other low-latency packet-level options.

The important implementation semantics are:
- The paper-native main scheme is non-systematic unless explicitly using the systematic variant.
- Edge generation is source-position based, not block-hash-position based.
- TLE must be unique and immediately releasable.
- Non-TLE MET edges use binomial right-boundary offsets.
- The finite stream tail should be compressed, and reported overhead must state whether tail is included.
- BEC is independent per coded packet/bin.
- GE state transitions and per-state erasure probabilities must match `(epsilon, delta, alpha, beta)`.

## Implementation Alignment Checklist

Use this checklist when comparing code to Part II and Part I together.

### Core Parameters

Paper-current evaluation path:

$$
l=4,\quad w=600,\quad (\zeta_2,\zeta_3,\zeta_4)=(1/2,1/4,1/8)
$$

Source prefix for coding-efficiency evaluation:

$$
n = 10^5
$$

Target coding-efficiency failure probability:

$$
P_{\mathrm{fail}} < 10^{-3}
$$

### Non-Systematic Encoder

For every source packet `x`, compute:

$$
h_1(x) = \lfloor (1+c)x \rfloor
$$

or another deterministic integer rule that preserves TLE uniqueness.

For `i=2,3,4`:

$$
\eta_i \sim \mathrm{Binomial}((1+c)w,\zeta_i)
$$

and:

$$
h_i(x) = (1+c)(x+w) - \eta_i
$$

with integer rounding defined consistently.

The coded bin payload is:

$$
B_z = \bigoplus_{x : z \in E(x)} P_x
$$

### Decoder

The decoder receives `(bin_id, payload)` pairs and reconstructs the graph from:
- source sequence range,
- seed,
- `c`,
- `w`,
- `l`,
- MET probabilities,
- finite termination/tail compression rule.

Peeling rule:

$$
\deg(z)=1 \Rightarrow \text{recover unique unknown source touching } z
$$

Then XOR that decoded source out of every received neighboring bin.

### Tail Compression

If the source prefix is finite with length `n`, apply tail compression only to:

$$
x \in [n-w,\ n-1]
$$

The compressed range should reduce tail loss from:

$$
(1+c)w
$$

to approximately:

$$
\frac{(1+c)w}{2}
$$

### Code Efficiency Reproduction

BEC construction must be:

$$
\Pr[\text{erase coded packet } z] = \varepsilon
$$

independently for every coded packet.

For isolated error floor under BEC:

$$
\Pr[\text{source } x \text{ has all } l \text{ incident bins erased}] = \varepsilon^l
$$

Expected isolated unrecoverable source symbols in a prefix of size `n`:

$$
n\varepsilon^l
$$

For Part I's `l=4`, `n=10^5`:

$$
\mathrm{BEC}(0.1):\quad 10^5 \cdot 0.1^4 = 10
$$

These isolated symbols are not counted as METTLE decoding failures in the Part I coding-efficiency interpretation.

### RaptorQ Comparison

For coding-efficiency comparison, RaptorQ uses a small `k` chosen to match METTLE latency:

$$
k \approx 2\tau
$$

where:

$$
\tau = \text{METTLE average decode latency}
$$

Part I's newer table is the authoritative RaptorQ comparison source, not Part II's preliminary examples.

