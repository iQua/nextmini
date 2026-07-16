# Perfect FEC Runtime — Master Plan

Branch: `perfect-fec-runtime` (worktree `/Users/winifred/nextmini-perfect-fec`, forked from
`codex/tree-scoped-transports-main` @ e7aae32). **Never touch `main` or any other branch. Never push.**
Commit per sub-stage. Commit messages: imperative, no Co-Authored-By / Generated-by footers.

Roles: Claude = plan owner + per-stage reviewer. Codex = implementer (and plan reviewer before Stage 0).
If a decision is ambiguous during implementation, STOP, append the question to
`plans/perfect-fec-runtime-questions.md`, commit, and move to the next unblocked item.

## 0. What "perfect" means (theory → construction contract)

The theory (paper `problem.tex` Dec_b semantics + the rank/DoF analysis) has three layers. Each layer
maps to a construction invariant that must become CI-checkable:

| Layer | Theory statement | Construction invariant |
|---|---|---|
| L1 pooling | Decoding depends only on the pooled per-block symbol set, never on tree identity (Prop 1) | Receiver decoder state keyed by (block, symbol) only; `tree_id` used for routing/stats exclusively. Already true — keep a regression test. |
| L2 no ownership | Any symbol may ride any tree; no per-tree quotas (dominance over striping, Prop 2) | Flat round-robin schedule + WouldBlock→next-tree + all-blocked→yield loop in `send_symbol`. Already true — keep `fec_tree_schedule_is_one_slot_per_tree_round_robin` + backpressure tests. |
| L3 work conservation | Sender never idles while some receiver still needs DoF and some tree can accept a frame; every emission is a fresh (never-before-sent) symbol id of a globally-incomplete block (Prop 3 hypothesis) | NEW: carousel mode (Stage 1). Invariants: (a) monotone fresh ids per block, (b) zero sender wait states other than backpressure/pacing while work exists, (c) emission for a block stops once every active peer acked it. |

Honest limits (state these in docs/tests, do not claim past them):
- Backend gap: RaptorQ needs K..K+2 symbols w.h.p. (measure `h`); METTLE approaches the ideal only
  after Stages 2–4. τ-side optimality is "first instant the pooled history is backend-decodable" —
  exactly Dec_b — not "exactly K packets".
- Feedback-latency tail: symbols in flight when the last ack is generated are unavoidable waste
  (≈ rate×RTT bandwidth, zero effect on completion time). Counted by a metric, never hidden.
- The theory is conditional on the realized delivery processes; nothing here claims extra capacity.

## Stage 0 — Foundations (audit fixes + green baseline)

Source: `plans/raptorq-mettle-audit-2026-07-12.md` (copied into this branch).

0.1 Checked FEC geometry (audit P1). One fallible scheme-aware constructor used by manifest
validation, sender, receiver: RaptorQ `1 ≤ K ≤ 56_403`, checked `K*T`, `1 ≤ T ≤` actual packet-envelope
payload ceiling (not bare u16::MAX), `oti()` → `Result`, no lossy `as` casts.
Files: `dataplane/src/node/session/fec.rs`, `fec_policy.rs`, `plan.rs`, `messages/src/lossless_session/validation.rs`.
Tests: rejected-geometry cases; the audit's `block_size=2_097_152, K=32` config must be rejected at
manifest time, not panic at first repair.

0.2 Symbol-id bounds (audit P1). Scheme-aware `symbol_id` validation before storing/decoding:
RaptorQ ESI < 2^24 and locally generated repair ESIs capped; METTLE bin ids bounded by the terminated
stream limit. A malformed peer must not be able to panic the receiver (turn crate asserts into errors).

0.3 Test baseline. Fix `sink_file` fixtures (`dataplane/tests/fec_receiver.rs`,
`fec_round_regressions.rs`, `multiblock_transfer.rs`), clippy `while_let_loop` in `mettle/src/block.rs`.
Resolve the `fec_mettle_session.rs` SourceDone contract: in rounds mode, document + test that the
initial phase is the full terminated codeword (current sender behavior); update the stale test contract.
Gate 0: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo nextest run` all green (controller tests may need Postgres via `./start-database.sh`; if
unavailable, record the skip in the questions file).

## Stage 1 — BlockAck + work-conserving carousel (RaptorQ-first)

Protocol (new session feedback mode, manifest-negotiated: `fec_feedback_mode = rounds | carousel`;
default stays `rounds`; receivers reject unknown modes at manifest validation).

1.1 `BlockAck` control frame (messages crate). Session-level, never assigned to a payload tree.
Cumulative + self-healing: `{ completed_watermark: u64, extra_completed: ranges }` (all blocks below
watermark complete; ranges reuse the `MissingBlockRange`-style encoding). Emission policy: batched on
completion (≈5–10 ms debounce) AND periodic heartbeat re-advertisement (≈250–500 ms) while the session
is incomplete, so any single ack loss self-heals. Include round-trip-free validation + encode/decode
unit tests mirroring the existing Need frame tests.

1.2 Receiver changes. On block completion (where `shared.complete_blocks` is updated), enqueue ack
state; a small timer flushes the cumulative digest. Keep the rounds-mode Need path untouched.

1.3 Carousel sender (`dataplane/src/node/session/sender/fec.rs`, new mode next to rounds):
- State: per-peer cumulative ack (watermark + set), per-block `globally_complete` flag.
- Loop: (a) drain controls; (b) Phase A: emit initial symbols block-sequentially (RaptorQ: source
  ESIs `0..K`; keep the configured initial overhead as a config default that carousel may set to 0);
  (c) Phase B: round-robin one fresh repair symbol per globally-incomplete block (`next_fountain_symbol`
  monotone; never reuse an id; hard-stop with protocol error at the ESI cap instead of wrapping);
  (d) finish when every block is acked by every active quorum member, then run the existing session
  teardown/Complete path.
- Tree selection: reuse `send_symbol` unchanged (L2 invariant).
- No SourceDone / Need / repair_window in this mode.
- On ack marking a block globally complete: stop emitting it immediately (already-queued frames in
  tree channels are accounted as tail waste, not drained).

1.4 Liveness. Rounds mode used the round barrier as liveness; carousel replaces it with: per-peer
last-ack-progress timestamp (heartbeats count), fed into the existing `quorum_liveness` policy with the
same timeout/abort semantics.

1.5 Metrics (the L3 proof hooks), exposed via the existing stats structs:
`carousel_backpressure_yields`, `sender_wait_states` (must be 0 outside backpressure/pacing/control-drain
while work exists), `symbols_after_global_complete` (tail waste), per-block `emitted_fresh_ids`;
receiver: `duplicate_symbols` (must be 0 in carousel for RaptorQ), `symbols_at_decode - K` histogram (h).

1.6 Conformance test (the theory test). In-process multi-tree lossy session (reuse
`fec_multitree.rs` harness style): record each receiver's per-block delivery count timeline; assert
(a) receiver completes at the first symbol arrival that makes the pooled set decodable (Dec_b eagerness),
(b) sender work-conservation counters hold, (c) measured `h` ≤ 2 for RaptorQ in the test seeds,
(d) with one artificially slow tree, carousel barrier completion ≤ rounds barrier completion on the
same loss trace (dominance smoke test).
Gate 1: Gate 0 checks + new tests green; rounds-mode regression suite untouched and green.

## Stage 2 — METTLE single object stream (paper-native)

Kills the per-block stream resets (audit P1-repro): one terminated METTLE stream over the object's
source symbols (paper model: k ~ 10^5+; tail `(1+c)w/2` amortizes to ~0.01% at 2M sources instead of
135% at K=256).

2.1 Sender: one `mettle_stream` per object (or per configurable mega-prefix if decoder memory
requires; measure first, see 2.4). Source id ↔ object offset mapping via `BlockPlan` geometry;
`block_id` in frames becomes the stream id (0) for METTLE mode — keep the wire format, change semantics
behind the scheme check. Departure = bin-id order.

2.2 Ack semantics for METTLE carousel: METTLE releases decoded sources in order (ordered release in
`mettle/src/decoder.rs`), so the natural ack is `{ decoded_source_watermark: u64, stalled: ranges }`.
Reuse the BlockAck frame with a scheme-tagged payload variant. Sender completion: every peer watermark
== total sources.

2.3 Interim repair (until Stage 3): receiver reports missing-bin ranges below its receive frontier
(erasure gaps are directly visible as bin-id gaps); sender re-emits the union across peers, stall-region
first. This replaces the current whole-stream replay (`schedule_mettle_repair_pass` reset-to-0 path) —
strictly less duplicate traffic; keep replay only as a last-resort fallback after k rounds of no progress.

2.4 Decoder memory measurement: dense precomputed graph at n≈2^21 sources (audit P2 concern) —
measure peak RSS + construction time in a bench; if unacceptable, gate object-stream size to mega-prefix
streams and record the tradeoff; rolling/hash-reconstructed decoder is a stretch goal, not required here.

2.5 Fix audit P3 (`repair_deficit` hard-codes zero overhead): make overhead part of the validated
parameter bundle or delete the dead METTLE branch.
Gate 2: green suite; mettle paper harness (fixed accounting, see 2.6) shows total overhead ≈ interior c
+ `(1+c)w/2n` tail on the object-stream path; the K=256-style pathology is gone from the codec sweep.

2.6 Harness accounting fix (audit P1-experimental): report actual transmitted overhead
(`terminal_symbol_count/K − 1`), solve interior c to hit a target total, assert it in the test.

## Stage 3 — Reservoir repair (fresh multicast repair for METTLE)

Rationale: paper Part II §1.2.3 endorses feedback + rate adaptation; encode CPU is O(l) per source
independent of c, so extra bins cost memory, not throughput.

3.1 Params: `c_total = c_wire + c_reserve`. Build the graph at `c_total`. Reserve set = seeded PRF
over non-TLE bins only (TLE bins are peeling anchors), |reserve| ≈ c_reserve·n. Initial departure =
non-reserve bins in id order (receiver sees reserve bins as ordinary erasures). Repair = emit reserve
bins, stall-overlapping first, then in id order — every reserve bin is fresh for every receiver.
Fallback when reserve exhausted: Stage 2.3 targeted re-send.

3.2 Simulation sweep (extend the mettle paper harness): (c_wire, c_reserve) × BEC p ∈ {0.1–2%} →
one-shot stall probability, repair rounds to completion, duplicate count (must be 0 until reserve
exhausted). Pick defaults hitting rounds-mode wire overhead at equal or better completion.
Gate 3: sweep results recorded under `results/` + defaults wired into config + session tests green.

## Stage 4 — Multi-pass reseed = rateless METTLE (research-grade, optional)

4.1 Namespace: top 8 bits of the u128 bin id = pass index; pass p uses seed_p = H(base_seed, p)
(stable hash, documented). Wire format unchanged (bin id already travels in the METTLE payload).

4.2 Decoder: N edge-generators sharing one recovered-source set; a degree-1 bin from any pass peels;
recovered sources XOR out of neighbors in every pass's received bins. Complexity stays O(l) per
recovery per pass.

4.3 Sender: when reserve is exhausted, open pass p+1 instead of replaying — unlimited fresh
equations; carousel then treats METTLE exactly like RaptorQ (A3 holds unconditionally).
Gate 4: harness curves (decode success vs cumulative overhead across passes) recorded; memory bounded;
this stage may land as experimental config default-off.

## Stage 5 — Docs + conformance polish

Update `docs/mettle-paper-notes.md` deviations section (block-adaptation → object-stream; reservoir;
multi-pass as extensions beyond the paper), metrics documentation, and a short
`docs/perfect-runtime-invariants.md` mapping L1/L2/L3 to the tests that enforce them.

## Standing constraints

- Worktree `/Users/winifred/nextmini-perfect-fec` only; branch `perfect-fec-runtime` only; no push;
  never touch `main` or `codex/tree-scoped-transports-main`.
- Every commit: `cargo fmt --all`, clippy `-D warnings` clean for touched crates, `cargo nextest run`
  for touched crates minimum; full workspace at stage gates.
- Rounds mode must keep passing its existing regression suite at every stage (A/B ability is a
  deliverable, not a casualty).
- Tracing via `tracing::*`, actionable ids (session, block, tree, peer) per AGENTS.md.
