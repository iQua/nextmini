# RaptorQ / METTLE Implementation Audit

Date: 2026-07-12  
Revision: `e7aae3202ee5bf539f02fe2f7d3c846d968c96ce`

## Bottom line

- The RaptorQ happy path is wired correctly: source ESIs are `0..K`, repair ESIs are mapped to the crate's zero-based repair index, and sender/receiver rebuild the same OTI. The current implementation is not safe for all manifests it accepts, however; several RFC/crate/wire limits are unchecked and can turn a valid-looking session into a delayed panic when repair begins.
- The METTLE kernel is substantially aligned with the paper: non-systematic coded bins, `l=4`, `w=600`, the `(1/2, 1/4, 1/8)` MET profile, TLE, left-to-right peeling, and linear tail compression are present. The lossless-session adapter and the paper-reproduction harness are not fully paper-equivalent.
- Therefore the answer is: the codec cores mostly work, but the overall RaptorQ/METTLE implementation is not yet correct enough to claim RFC-safe RaptorQ or paper-faithful METTLE.

## Sources used

- Qianru Yu, Tianji Yang, Jingfan Meng, Jun Xu, [“METTLE: Efficient Streaming Erasure Code with Peeling Decodability”](https://arxiv.org/abs/2602.10020), arXiv:2602.10020v1, 2026-02-10.
- Authors' [extended METTLE PDF](https://sites.cc.gatech.edu/home/jx/reprints/METTLE_isit_extended.pdf), including Part I and supplementary Part II.
- [RFC 6330: RaptorQ Forward Error Correction Scheme for Object Delivery](https://datatracker.ietf.org/doc/html/rfc6330).
- [`raptorq` 2.0.1 API/source](https://docs.rs/raptorq/2.0.1/raptorq/), the version resolved by this workspace during the audit.

## Findings

### P1 — Accepted RaptorQ geometries can panic only after loss triggers repair

Evidence:

- `dataplane/src/node/session/fec.rs:78-86` computes `K*T` without checked multiplication and casts `symbol_size` with `as u16`.
- `dataplane/src/node/session/fec_policy.rs:29-75` checks nonzero block/K values but does not enforce RaptorQ's `K <= 56,403`, a positive `u16` symbol size, or the smaller local packet-envelope limit.
- `dataplane/src/node/session/plan.rs:143-161` permits any nonzero `symbols_per_block` and derives `T = ceil(block_size/K)`.
- `dataplane/src/node/packet.rs:359-380` also casts an oversized IPv4 total length to `u16`, while `dataplane/src/node/network/framing.rs:10-21` rejects framed packets above 65,535 bytes at the receiver.
- RFC 6330 requires `K <= 56,403`; `raptorq` 2.0.1 enforces this with assertions. Its OTI constructor accepts `symbol_size: u16`.

A concrete accepted configuration is `block_size = 2,097,152`, `K = 32`: `T = 65,536`, which becomes zero at `as u16`. The systematic/no-loss path may appear to work because it does not instantiate the RaptorQ encoder. The first requested repair then reaches `SourceBlockEncoder` with an invalid OTI and panics. `K > 56,403` has the same delayed-failure shape.

Impact: configuration-dependent process crash; the no-loss fast path hides the defect during smoke tests.

Fix: introduce one checked, scheme-aware geometry constructor used by manifest validation, sender, and receiver. For RaptorQ it should at least enforce `1 <= K <= 56_403`, checked `K*T`, `1 <= T <= u16::MAX`, and the actual local FEC-symbol payload ceiling (packet-envelope overhead makes it smaller than 65,535). Make `oti()` return `Result` and remove lossy `as` casts.

### P1 — A peer-controlled RaptorQ ESI can panic the receiver

Evidence:

- `messages/src/lossless_session/validation.rs:197-213` validates only block id and advertised tree id for a block symbol; it places no bound on `symbol_id`.
- `dataplane/src/node/session/receiver/fec.rs:381-394` forwards arbitrary repair `symbol_id` values into the adapter once at least `K` distinct symbols are buffered.
- `dataplane/src/node/session/fec.rs:241-249` calls `PayloadId::new(0, esi)`.
- `raptorq` 2.0.1 asserts that an ESI is a 24-bit unsigned integer (`esi < 16,777,216`).

Impact: a malformed or buggy peer can crash the receiver by supplying enough distinct, correctly sized symbols with a 24-bit-overflowing ESI.

Fix: validate scheme-specific symbol-id ranges before storing symbols, cap locally generated repair ESIs, and expose fallible adapter methods instead of relying on dependency assertions.

### P1 for paper reproduction / P2 for transport — METTLE is reset per logical block, not run as one paper stream

Evidence:

- The paper explicitly says METTLE is not a normal block code; its evaluation uses a `10^5`-source “mega-codeword,” and latency is governed by `w`, not a small `k` boundary.
- `dataplane/src/node/session/sender/fec.rs:39-70` keeps one encoder state per logical block.
- `dataplane/src/node/session/sender/fec.rs:769-795` constructs a new terminated METTLE stream, resets source ids, and derives a new graph seed for every `block_id`.
- `dataplane/src/node/session/sender/fec.rs:1329-1350` explicitly tests and accepts a 16-block METTLE object.
- The source comment at `dataplane/src/node/session/sender/fec.rs:404-410` calls this “one paper-native finite object stream,” which is false for `total_blocks > 1`.

Every reset pays the compressed termination tail again and creates a new peeling boundary. This is especially severe for small `K`: with the current algorithm, `K=32,c=0` emits 600 bins for a 32-source block; the repository default geometry is `block_size=8500,K=32` (`dataplane/src/node/config.rs:772-786`). Even the repository's “paper-scale” `K=2400,c=0` stream has 2,699 bins, about 12.46% termination overhead, whereas the paper evaluates METTLE at `K=100,000`.

Impact: decoded bytes remain correct, but coding efficiency, latency distribution, tail cost, and cross-boundary resilience are not the paper's METTLE. Multi-block benchmark results cannot be presented as direct paper reproduction.

Fix: carry one global METTLE source/bin sequence over the object (or over explicitly documented large prefixes), and map decoded source ids back to storage blocks. If independent finite blocks are required operationally, name and report the backend as a finite-block METTLE adaptation and publish its actual `K` and total tail overhead.

### P1 for experimental claims — Table-IV harness gives METTLE more packets than the paper's stated overhead

Evidence:

- The extended paper preface says all evaluation overhead ratios explicitly include tail loss; Table IV reports 5.5% for BEC(0.01).
- `mettle/tests/paper_coding_efficiency.rs:145-190` records those table percentages as `mettle_overhead_ratio`.
- `mettle/tests/paper_coding_efficiency.rs:301-305` feeds that percentage directly into the kernel's interior expansion parameter.
- `mettle/tests/paper_coding_efficiency.rs:682-704` then transmits through `terminal_departure_end_exclusive`, adding the compressed tail on top.
- The reporter at `mettle/tests/paper_coding_efficiency.rs:1160-1181` prints the nominal table value, not the actual transmitted ratio.

Measured during this audit at `K=100,000`, BEC(0.01), 100 graph trials:

- configured `c=5.50%` -> 105,815 transmitted bins -> **5.815% actual overhead**, 0/100 stalls;
- configured `c=5.18%` -> 105,495 transmitted bins -> **5.495% actual overhead**, 0/100 stalls.

The 100-trial smoke is not enough to establish the paper's `<10^-3` target, but it proves the packet-budget mismatch. The current comparison gives METTLE 315 extra packets per 100,000 sources in the 5.5% row.

Fix: define whether the user-facing ratio is interior expansion or total finite-stream overhead. For Table-IV reproduction, solve the interior ratio so that `terminal_symbol_count / K - 1` equals the paper row, assert that actual ratio in the test, and compare codecs by actual transmitted/received packet counts.

### P2 — The finite production decoder does not retain the paper's hashing/streaming memory property

Evidence:

- The paper presents the Tanner graph as reconstructed from hashing on the fly and lists low storage/implementation cost as a systems benefit.
- `mettle/src/stream.rs:147-170` deliberately routes every terminated decoder through `new_terminated_with_precomputed_graph`.
- `mettle/src/decoder.rs:64-83,769-796` materializes both source-to-bin and bin-to-source adjacency for the whole finite stream, plus dense per-bin receive/seen state.
- `dataplane/src/node/session/receiver/fec.rs:42-61` uses that terminated decoder for every active block.

Impact: algorithmic outputs are equivalent (the repository tests precomputed vs rolling), but memory is `O(K + B)` rather than coupling-window bounded, construction runs synchronously on the receive task's first bin, and several incomplete blocks multiply the allocation. Production behavior therefore does not substantiate the paper's on-the-fly graph/storage claim.

Fix: use the rolling/hash-reconstructed decoder in the session path or build a bounded ring/window index. Measure graph-construction time and peak memory separately if the dense optimization remains available.

### P2 — The current end-to-end FEC validation suite is red

Observed:

- `cargo nextest run -p mettle`: 75 passed, 14 skipped.
- `cargo nextest run -p nextmini --lib`: 179 passed, 1 skipped.
- Selected integration binaries could not all compile because `sink_file` is missing from fixtures in `dataplane/tests/fec_receiver.rs`, `dataplane/tests/fec_round_regressions.rs`, and `dataplane/tests/multiblock_transfer.rs`.
- Of the four integration binaries that compiled, 17 tests passed and `dataplane/tests/fec_mettle_session.rs:31-213` failed: the sender emitted tail bin 2400 before `SourceDone`, while the test contract expects only the `K` initial bins before feedback.
- `cargo clippy -p mettle --all-targets -- -D warnings` fails at `mettle/src/block.rs:227-239` (`while_let_loop`).
- `cargo fmt --all -- --check` passes.

The failing METTLE test exposes an unresolved contract rather than proving the new sender wrong by itself: the sender now transmits the whole terminated tail before feedback, while the test expects early completion after the lossless `K`-bin prefix. Either behavior can be designed deliberately, but code and test currently disagree.

Fix: repair the fixtures first; then decide and document whether `SourceDone` closes the immediate `(1+c)K` departure prefix or the full terminated codeword, and add lossy, multi-block, oversized-geometry, and invalid-ESI tests around that contract.

### P3 — The public METTLE repair-deficit helper hard-codes zero overhead

Evidence:

- `dataplane/src/node/session/fec.rs:275-291` exposes `repair_deficit` for both schemes.
- Its METTLE branch at `dataplane/src/node/session/fec.rs:315-343` always constructs metadata with `OverheadRatio::ZERO`; `BlockParams` contains no METTLE overhead field.

The live receiver currently bypasses this helper and uses “1 means retransmit a pass,” so this is dormant, but the public helper returns the wrong graph estimate for every nonzero configured METTLE rate.

Fix: either remove the unused METTLE branch/helper or make overhead part of the validated parameter bundle.

## What is correct

### METTLE kernel

- `mettle/src/params.rs:61-68`: paper evaluation profile `l=4`, `w=600`, probabilities `(1/2,1/4,1/8)`.
- `mettle/src/params.rs:81-99`: rational, injective TLE placement with deterministic integer rounding.
- `mettle/src/params.rs:305-353`: independently seeded binomial non-TLE offsets inside the coupling window.
- `mettle/src/params.rs:170-232`: the last `w` source windows shrink linearly from factor 1 to factor 2, matching the paper's tail-compression description.
- `mettle/src/encoder.rs:71-95`: source bytes are XORed directly into coded bins and finalized bins depart in id order; no hidden systematic transform is used.
- `mettle/src/decoder.rs:444-648`: genuine degree-one peeling, including out-of-order future decoding followed by ordered source release.
- Encoder/decoder graph generation is deterministic and duplicate edge selections are handled consistently. A diagnostic over 10 seeds x 100,000 sources at the BEC(0.01) profile found no within-source duplicate bins (the MET distributions are widely separated at `w=600`).

### RaptorQ happy path

- `dataplane/src/node/session/fec.rs:133-173`: ESI `K+r` maps to `repair_packets(r,1)`, which matches `raptorq` 2.0.1's zero-based repair API.
- `dataplane/src/node/session/fec.rs:237-271`: sender and receiver use the same single-source-block OTI and preserve source/repair ESIs.
- `dataplane/src/node/session/receiver/fec.rs:360-433`: all-systematic blocks bypass decoding safely; mixed source/repair sets go through RaptorQ and are truncated to the logical block length.
- Unit tests cover all-source, mixed source/repair, and large-symbol round trips for supported sizes.

## Recommended order of work

1. Add one checked FEC geometry/symbol-id boundary and remove RaptorQ panic paths.
2. Decide whether production METTLE is a single object stream or an explicitly named finite-block adaptation; make comments, manifest fields, and tests agree.
3. Correct the paper harness to use actual total overhead including tail, then run enough trials to support the stated failure-rate target.
4. Restore the integration suite and add negative boundary tests.
5. Revisit the dense finite decoder only after correctness/contracts are fixed; it is a performance architecture choice, not the first blocker.
