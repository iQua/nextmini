# Perfect FEC Runtime — Master Plan (v2)

v2 incorporates the Codex plan review (`plans/perfect-fec-runtime-review-codex.md`, 2026-07-16) in full:
all 7 Critical and 7 High findings accepted. Stage 4 (multi-pass reseed) is extracted to
`plans/rateless-mettle-vnext.md` and no longer gates this branch.

Branch: `perfect-fec-runtime` (worktree `/Users/winifred/nextmini-perfect-fec`, forked from
`codex/tree-scoped-transports-main` @ e7aae32). **Never touch `main` or any other branch. Never push.**
Commit per sub-stage. Commit messages: imperative, no Co-Authored-By / Generated-by footers.

Roles: Claude = plan owner + per-stage reviewer. Codex = implementer.
Ambiguity rule: STOP, append the question to `plans/perfect-fec-runtime-questions.md`, commit, continue
with the next unblocked item.

## 0. What "perfect" means (theory → construction contract)

| Layer | Theory statement | Construction invariant |
|---|---|---|
| L1 pooling | Decoding depends only on the pooled per-block symbol set, never on tree identity | Decoder state keyed by (block, symbol) only; `tree_id` routing/stats only. Already true — regression test pins it (permute tree labels for the same pooled set → same completion). |
| L2 no ownership | Any symbol may ride any tree; no per-tree quotas | Flat round-robin + WouldBlock→next-tree sweep. Already true — existing tests pin it. |
| L3 work conservation | While some active peer still needs DoF: the sender is either emitting a fresh id of a globally-incomplete block, blocked on backpressure/pacing, or servicing controls/timers — never in any other wait state; emission for a block stops once its completion joins to global | NEW (Stage 1). Defined over the explicit sender state machine below; enforced by event-boundary counters (`queued_after_final_ack_processed == 0`, wait-state enumeration), not by unobservable "in-flight tail" claims. |

Honest limits (stated in docs and tests): backend gap (RaptorQ needs K..K+h; h histogram measured, pinned
only on deterministic fixtures; METTLE approaches ideal only after Stages 2–3); feedback-latency tail
(in-flight symbols when the last ack is generated are unavoidable bandwidth waste, zero τ effect —
measured receiver-side as `symbols_received_after_local_block_complete`, never inferred sender-side);
the theory is conditional on realized delivery processes.

## P. Protocol assumptions and state machines (normative; precedes all stages)

P1 Loss model. Control frames may be lost, duplicated, reordered, and arbitrarily delayed (fair-loss:
infinite retransmissions eventually deliver). Under permanent control-path failure the session ends by
timeout abort. The sender NEVER reports success without holding cumulative completion from every frozen
active peer; it never depends on a receiver confirming `SessionComplete`.

P2 Session identity. Normative precondition: `session_id` is a fresh cryptographically random `u64`
per transfer (allocator change + reuse test). All completion state is scoped to it. If the allocator
cannot guarantee this, add an explicit `transfer_incarnation` nonce bound into Manifest, Ready,
BlockAck, AckProbe, SessionComplete (decision recorded in the questions file before Stage 1 starts).

P3 Quorum. The active quorum is frozen at Ready (existing semantics). No per-peer removal mid-session.
Empty active quorum after the grace period ⇒ immediate trivial success (existing rounds behavior, kept
and tested). Liveness violations abort the whole session (existing semantics).

P4 BlockAck canonical form and join. Payload: extensible enum, v1 variant
`Blocks { completed_watermark: u64, extra_completed: canonical ranges }`, reserved discriminant for the
METTLE stream-progress variant (Stage 2). Validation: `0 ≤ watermark ≤ total_blocks`; ranges canonical,
disjoint, non-touching, within `[watermark, total_blocks)`; ranges touching the watermark are folded
into it on receipt (not rejected). Sender peer state is a join-semilattice: accepted acks UNION with
stored completion, then canonicalize; completion never regresses. Duplicates/reorders are completion
no-ops but valid alive-signals. Range overflow: if a snapshot exceeds `MAX_NEED_RANGES`-style wire
capacity, the receiver sends the watermark plus the lowest-id ranges that fit (deterministic
truncation); monotone watermark advance guarantees eventual convergence. Property test: every
permutation + duplication of an increasing snapshot sequence yields the same final state.

P5 Receiver states. `Active → LocallyComplete (passive) → Finished`.
- Active: decode eagerly; advertise cumulative BlockAck on debounce (≈5–10 ms), heartbeat
  (≈250–500 ms), and on `AckProbe`.
- LocallyComplete: object fully decoded AND committed to sinks (P8); keep answering heartbeats and
  `AckProbe` with the final cumulative ack until `SessionComplete` arrives or the passive window
  expires. The passive replay window (in-process, plus the runtime replay cache in
  `session/api.rs`/`runtime.rs` after exit) MUST be ≥ the sender's abort budget + margin; both
  configurable, inequality documented and asserted in config validation.
- Finished: on `SessionComplete` or window expiry.

P6 Sender states. `Sending → Probing → Finished`.
- Sending: the work-conserving loop (Stage 1.3). Transitions per block on ack joins.
- Probing: no globally-incomplete block remains un-emittable but some peer's cumulative completion is
  missing ⇒ periodically send `AckProbe` to exactly the peers whose acks are missing (also used when
  the ESI cap nears). Payload emission may continue concurrently for blocks that are still incomplete.
- Finished: cumulative completion held from every active peer ⇒ broadcast `SessionComplete` (best
  effort, fire-and-forget with a small repeat count), report `SessionOutcome::Completed`.

P7 Liveness clocks (per peer, replacing round-barrier liveness):
- `last_ack_seen` — any valid ack/heartbeat/probe-answer; expiry = silence timeout ⇒ session abort.
- `last_ack_progress` — only when the joined completion set strictly grows; expiry = stall timeout
  (separate, longer, configurable) ⇒ session abort.
Clocks start at the Ready-quorum freeze. `quorum_liveness` in `sender/state.rs` is global and
solicitation-oriented; it is replaced (not reused) for carousel. Tests: long healthy transfer, silent
peer, live-but-stalled peer, heterogeneous progress rates.

P8 Completion semantics. "Complete" = decoded AND accepted by every configured sink. Sink writes become
fallible; a sink error aborts the receiver session with a distinct outcome and is never acked.
Vocabulary used by all metrics/tests: emitted (frame handed to a tree queue) / queued (accepted by the
queue) / delivered (received by peer) / decoded / complete (P8).

P9 Wire versioning. Manifest/control/frame layout changes bump the lossless protocol version (currently
7 → 8). A normative version/layout table lives in `messages/src/lossless_session/mod.rs` docs;
`docs/perfect-runtime-invariants.md` (Stage 5) links to it. Receivers reject unknown modes/versions at
manifest validation.

## Stage 0 — Foundations (audit fixes + green baseline)

Source: `plans/raptorq-mettle-audit-2026-07-12.md`.

0.1 Checked FEC geometry, layered by dependency (review edit #2): a dependency-free wire-geometry
type in `messages` (pure arithmetic: K/T bounds, checked `K*T`, symbol-payload ceiling derived from the
frame/envelope constants in ONE place — currently 65,535 − 20 (IPv4) − 36 (TCP+lossless option) − 20
(lossless header) − 16 (BlockSymbol metadata) = 65,443 — exported, never duplicated); codec-specific
caps (RaptorQ `1 ≤ K ≤ 56_403`, ESI < 2^24; METTLE terminated-stream bin bound) layered in dataplane
`fec_policy`/`session/fec.rs` after syntactic validation. `oti()` → `Result`; no lossy `as` casts.
The audit's `block_size=2_097_152, K=32` config is rejected at manifest time.

0.2 Symbol-id bounds: scheme-aware `symbol_id` validation before store/decode; local repair-ESI
generator stops before wrap with a protocol error; crate asserts unreachable from peer input
(fuzz/property tests over body lengths, ids, range counts).

0.3 Fallible sink path: `write_block`/METTLE source-run writes return errors; receiver aborts with a
distinct outcome on sink failure (P8 groundwork; benefits rounds mode too). Injected sink-error test.

0.4 Test baseline: fix `sink_file` fixtures (`fec_receiver.rs`, `fec_round_regressions.rs`,
`multiblock_transfer.rs`), clippy `while_let_loop` in `mettle/src/block.rs`. Resolve the
`fec_mettle_session.rs` SourceDone contract: rounds mode initial phase = full terminated codeword
(current sender behavior); update the stale test and document.

0.5 Boundary tests (review "Stage 0 boundaries" list, verbatim adopted): K ∈ {1, 56_403} accepted,
{0, 56_404} rejected; symbol size min/max and max+1; checked `K*T`, padding, last partial block, empty
object, usize conversion failures; ESI 2^24−1 accepted, 2^24 rejected; METTLE last terminated bin
accepted, `terminal_end_exclusive` rejected.

Gate 0: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo nextest run` green (controller tests need Postgres via `./start-database.sh`; if unavailable,
record the skip in the questions file).

## Stage 1 — Carousel protocol slice (RaptorQ-first)

1.0 Protocol plumbing: `FecFeedbackMode { Rounds, Carousel }` in config + manifest (default Rounds);
protocol version bump per P9; P2 session-id allocator change + reuse test. Surface inventory (review
H7): `messages/{types,control_frames,validation,header,mod}.rs` + test support;
`dataplane/node/config.rs`, `session/fec_policy.rs`; `session/sender/{mod,state,fec}.rs`;
`session/receiver/{mod,fec}.rs`; `session/{api,runtime}.rs`.

1.1 BlockAck + AckProbe + SessionComplete control frames per P4/P5/P6, with encode/decode/validation
unit tests mirroring the Need-frame tests, including >wire-max disjoint ranges and truncation.

1.2 Receiver: ack state machine per P5 (debounce, heartbeat, probe answers, passive window, runtime
replay cache extension keyed by session).

1.3 Sender carousel loop per P6 with the send-path refactor (review C2, prerequisite):
- `send_symbol` becomes a single sweep returning `Queued | AllWouldBlock | AllClosed`; it never owns a
  wait loop. The outer loop is a control-aware `select!` over {control inbox, per-peer timers,
  pacing} — on `AllWouldBlock` it services controls/timers, then retries only if the block is still
  globally incomplete. Completion is re-checked after pacing and immediately before frame submission.
- Emission order: Phase A source ESIs `0..K` block-sequential (RaptorQ initial phase is exactly K
  source symbols — there is no initial-overhead knob); Phase B one fresh repair ESI per
  globally-incomplete block, round-robin across blocks, monotone `next_fountain_symbol`.
- Ack join marks a block globally complete ⇒ no further emission for it (already-queued frames in tree
  channels are not drained; the receiver-side tail metric accounts for them).

1.4 Liveness per P7 (new per-peer clocks; whole-session abort semantics preserved; empty-quorum
trivial-success test).

1.5 Metrics/test-observer surface (review H4): a shared per-session `SessionMetrics` (Arc) exposed via
a deterministic test hook, replacing the no-op logging stubs. Counters defined by event boundaries:
`queued_after_final_ack_processed` (MUST be 0), `carousel_backpressure_sweeps`, enumerated
`sender_wait_states` durations, per-block monotone ESI check (start/end/count, every increment
checked), receiver `symbols_received_after_local_block_complete`, receiver duplicate count (measured,
deduped safely — NOT a sender-proof invariant; the enforceable invariant is the scheduler never emits
the same (block, ESI) twice), `symbols_at_decode − K` histogram.

1.6 Conformance suite (review Stage-1 list, adopted): a new deterministic harness with loss, delay,
reorder, duplication, backpressure, per-peer observation (fec_multitree.rs is style precedent only).
Required tests: ack-join permutation property; ack-loss liveness (first ack, N heartbeats, final ack);
completion handoff (drop SessionComplete, recover via probe/replay); incarnation/stale-frame safety;
backpressure responsiveness (ack delivered while all trees blocked terminates work without a tree
opening); pacing race; freshness under tree fallback + control reorder; quorum safety incl. empty
quorum; timer fairness under paused Tokio time; range scaling; eager decode + L1 tree-permutation;
metric semantics. Rounds-vs-carousel comparison on matched seeds is recorded as BENCHMARK EVIDENCE
with stated tolerances (review H5) — not a pass/fail gate.

Gate 1: Gate-0 checks + the full 1.6 suite green + rounds-mode regression suite untouched and green.

## Stage 2 — Single-stream METTLE (paper-native), memory-first

Mode matrix (review H2, decided): `Rounds + METTLE` remains the legacy finite-block adaptation,
regression-frozen, explicitly named "finite-block METTLE adaptation" in docs. Paper-native
object-stream METTLE exists only under Carousel. No implicit behavior change behind existing fields.

2.0 Memory/layout spike (moved first, review H1): benchmark the dense precomputed graph AND the
existing internal rolling terminated decoder (`MettleDecoder::new_terminated`; only the public
`stream::Decoder::new_terminated` forces dense — expose the rolling path) at representative source
counts (up to ~2^21), production symbol sizes, loss/reorder patterns, worst stalls. Measure
construction latency, steady/peak RSS, peak buffered-payload bytes, event-loop blocking. Set an
explicit budget; choose object-stream vs negotiated mega-prefix streams. Decoder construction happens
before Ready or off the receive loop; allocation failure = clean manifest rejection.

2.1 Object symbol plan: a dedicated `ObjectSymbolPlan` (NOT overloaded `BlockPlan`): global source
segmentation `ceil(total_bytes / T)` with padding only on the final source; explicit mapping
source id ↔ object offset ↔ sink write; if mega-prefixes: deterministic stream ids, per-stream source
counts, seed derivation (`block_seed(session_id, stream_id)`), all manifest-negotiated (every receiver
must build the same graph).

2.2 Wire/feedback: METTLE progress variant of BlockAck (reserved discriminant from P4):
`{ stream_id, decoded_source_watermark, stalled evidence }` — per-stream when prefixes are used.
Manifest fields for stream geometry; validation.

2.3 Sender/receiver single-stream implementation: one terminated stream per object/prefix; departure in
bin-id order; sink writes via `ObjectSymbolPlan`; kill per-block stream resets on the carousel path.

2.4 Reorder-safe targeted repair (review C5): NO frontier-gap inference. Design: numbered repair
epochs — the sender emits a departure checkpoint control (epoch n covers bins < B_n) only after all
epoch-n payload frames have been queued to tree channels; receivers age gaps against the checkpoint
plus a configured reorder budget before reporting `missing bin ranges (epoch-tagged)`; the sender
dedupes requests per epoch and re-emits the union. Full-stream replay remains the fallback after a
precisely defined no-progress count (epochs without watermark advance). Tests: extreme cross-tree
reorder with zero loss ⇒ zero retransmissions classified required; known losses near/far from the
watermark ⇒ targeted recovery; duplicate-traffic is MEASURED (no "strictly less" claim until measured).

2.5 Delete dead `repair_deficit` METTLE branch (audit P3) — deletion preferred since carousel no longer
uses it.

2.6 Harness accounting fix (audit P1-experimental, moved before Stage 3): report actual transmitted
overhead (`terminal_symbol_count/K − 1` incl. compressed tail); solve interior c for a target total;
assert in test; enough trials to support stated failure probabilities.

Gate 2: green suite; RSS/construction thresholds and max simultaneous streams explicit and met;
mode-matrix tests prove rounds untouched; object round-trip property tests (padding, partial final
source, prefix boundaries) pass; codec sweep shows the K=256-style tail pathology gone.

## Stage 3 — Reservoir repair (simulation-first, research-gated)

Documented from the start as an extension BEYOND the METTLE paper (the paper endorses feedback/rate
adaptation, not this construction).

3.0 Deterministic puncturing + storage prototype: exact reserve-set selection (seeded PRF over bin
positions that are not any source's TLE bin — precise definition accounting for TLE positions also
receiving non-TLE edges), stable test vectors, exact reserve cardinality after tail effects;
sender-side reserve payload strategy measured against a hard budget (retention ≈ c_reserve×object
bytes vs recompute vs spill — recomputing a bin is not the O(l) streaming path; measure it).

3.1 Simulation sweep on ACTUAL finite counts (needs 2.6): (c_wire, c_reserve) × BEC p ∈ {0.1–2%} AND
GE/bursty traces; report completion probability with confidence intervals, actual total overhead,
repair latency, duplicate traffic, sender memory.

3.2 Integration only if 3.1 passes its gate: manifest fields (`c_total`, `c_wire`, reserve cardinality,
PRF version, seed derivation); receiver reconstructs the reserve set and classifies intentional holes
as non-loss in 2.4 feedback (never "ordinary erasures"); repair emits unsent reserve bins overlapping
stalled source regions first; freshness is a sender-emission property — per-peer loss of an emitted
reserve bin is handled by 2.4 retransmission and counted separately; reserve-exhausted ⇒ 2.4 fallback.

Gate 3: 3.1 report recorded under `results/`; storage within budget; session tests green; each reserve
id emitted at most once before exhaustion under reordered stall reports from multiple peers.

## Stage 4 — extracted

Multi-pass reseed (rateless METTLE) moved to `plans/rateless-mettle-vnext.md` (separate,
protocol-versioned research effort; wire id width, pass bounds, seed-hash vectors, recovered-source
backing, eviction, equation-freshness measurement). It no longer gates this branch (review C6).

## Stage 5 — Docs + conformance polish

`docs/perfect-runtime-invariants.md`: L1/L2/L3 → enforcing tests map, protocol state machines (from §P,
kept normative there), version/layout table link, measured-vs-guaranteed claims table. Update
`docs/mettle-paper-notes.md` deviations (finite-block adaptation vs object-stream; reservoir as
extension). Rounds-vs-carousel benchmark results recorded with methodology.

## Standing constraints

- Worktree `/Users/winifred/nextmini-perfect-fec`, branch `perfect-fec-runtime` only; no push; never
  touch `main` or `codex/tree-scoped-transports-main`.
- Every commit: `cargo fmt --all`; clippy `-D warnings` clean and `cargo nextest run` for touched
  crates; full workspace at stage gates.
- Rounds mode keeps passing its existing regression suite at every stage.
- Tracing via `tracing::*` with actionable ids (session, block/stream, tree, peer) per AGENTS.md.

## Dependency order (review-recommended, adopted)

1. Stage 0 (incl. wire geometry + fallible sinks) → 2. §P normative protocol + P2 allocator →
3. send-loop refactor + metrics surface → 4. RaptorQ carousel + full 1.6 adversarial suite →
5. METTLE 2.0 memory/layout spike + negotiated geometry → 6. single-stream METTLE + 2.4 repair +
2.6 accounting → 7. reservoir 3.0/3.1, integrate on pass → 8. docs/conformance → 9. vNext reseed
(separate plan).
