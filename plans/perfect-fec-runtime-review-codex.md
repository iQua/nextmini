# Perfect FEC Runtime Plan Review (Codex)

Date: 2026-07-16  
Reviewed branch/worktree: `perfect-fec-runtime` at `/Users/winifred/nextmini-perfect-fec`  
Reviewed inputs: `plans/perfect-fec-runtime.md`, `plans/raptorq-mettle-audit-2026-07-12.md`, `docs/mettle-paper-notes.md`, `docs/mettle-part-ii-notes.md`, the lossless-session implementation under `dataplane/src/node/session/` and `messages/src/lossless_session/`, and the METTLE kernel under `mettle/src/`.

## Executive verdict

The plan has the right high-level decomposition—first make the existing RaptorQ path safe, then add continuous cumulative feedback, then remove METTLE's per-block resets—but it is **not implementation-ready after Stage 0**.

Stage 0 is a sound starting point with a few ownership and boundary details to add. Stage 1 needs a complete feedback/termination state machine and a control-responsive backpressure loop before BlockAck can be called self-healing. Stage 2 is feasible, especially because a rolling terminated decoder already exists internally, but its memory decision must move before the wire and stream-layout implementation, and its missing-bin repair rule is not sound under multi-tree reordering. Stage 3 is a plausible experiment after those contracts exist, but its puncturing, storage, and feedback semantics are underspecified. Stage 4 contradicts the current wire format and cannot provide the claimed unbounded freshness or bounded memory as written; it should be split into a separate wire-versioned research plan.

The most important safety issue is completion: the current protocol has no sender-to-receiver `Complete` control, completed-receiver replay is triggered only by `SourceDone`, and carousel explicitly removes `SourceDone`. A lost final BlockAck can therefore leave the sender emitting until timeout after the receiver has stopped advertising completion. The most important implementation issue is that the existing all-trees-backpressured loop cannot observe controls, so reusing `send_symbol` unchanged contradicts both immediate ack response and liveness.

## Verdict by stage

| Stage | Verdict | Required condition before implementation/gate |
| --- | --- | --- |
| Construction contract (L1–L3) | **Revise** | Keep L1/L2. Define L3 over explicit sender states and separate “queued after ack processing” from network tail already in flight. The current `send_symbol` loop does not satisfy the proposed control responsiveness. |
| Stage 0 — Foundations | **Proceed with edits** | Define where shared geometry lives, derive the actual envelope ceiling from constants, make symbol/sink completion fallible, and add exact boundary/property tests. |
| Stage 1 — BlockAck/carousel | **No-go as written** | Specify monotone merge, range limits, per-peer silence versus progress timers, final-ack recovery, session incarnation, zero-quorum behavior, and a real teardown protocol. Refactor the all-blocked send path so controls and timers remain serviceable. |
| Stage 2 — Single METTLE stream | **Feasible, but reorder first** | Make decoder memory/stream segmentation Stage 2.0; negotiate the result in the manifest. Define global source mapping, the rounds/carousel mode matrix, per-stream progress, and a reorder-safe repair epoch/frontier. |
| Stage 3 — Reservoir | **Research-gated** | Simulate and budget storage before production wiring. Define deterministic puncturing, intentional-hole handling, reserve-payload retention/recomputation, and manifest fields. |
| Stage 4 — Multi-pass reseed | **Reject/rewrite** | Requires a new wire representation and a new decoder architecture. Top-eight-bit `u128` names only 256 passes, while the wire currently carries `u32`; memory/work grow with pass count. Move this to a separate optional experiment. |
| Stage 5 — Docs/conformance | **Proceed incrementally** | Write the normative protocol/state-machine document before Stage 1, not only after Stage 4. Keep final user docs and measured claims for Stage 5. |

## Severity-ranked issues

### Critical — C1: Final BlockAck loss is not self-healing through receiver teardown

The proposed heartbeat says it runs “while the session is incomplete,” but the receiver cannot know that the sender has accepted its final cumulative ack. There is no `Complete` control in `LosslessSessionControl` (`messages/src/lossless_session/types.rs:256-266`), and the sender currently simply returns `SessionOutcome::Completed` after all `NeedReport::Complete` reports (`dataplane/src/node/session/sender/fec.rs:818-839`). Therefore the plan's “existing session teardown/Complete path” does not exist on the wire.

The existing rounds-mode safety net also cannot be reused unchanged:

- A complete receiver enters passive-complete and stops after `session_finish_timeout_for` (`dataplane/src/node/session/receiver/mod.rs:289-317`). In production that grace is capped at about 5.3 seconds (`dataplane/src/node/session/timing.rs:13-24,39-42`), while the default sender peer-report timeout is 15 seconds (`dataplane/src/node/config.rs:778-803`).
- After receiver exit, the runtime caches only a `NeedReport::Complete` replay (`dataplane/src/node/session/api.rs:143-157`). It sends that replay only in response to a same-round `SourceDone` (`dataplane/src/node/session/runtime.rs:603-677`). Carousel explicitly has no `SourceDone`.
- A periodic BlockAck that stops at local object completion does not heal loss of the final ack. A periodic BlockAck that continues only until the current passive deadline can still stop well before the sender's abort deadline.

Required plan edit:

1. Add explicit carousel closure states and controls. A minimal safe design is `AckProbe` plus `SessionComplete { incarnation }`:
   - A receiver advertises its cumulative BlockAck on debounce, heartbeat, and `AckProbe`, including after local object completion.
   - The sender periodically probes any peer whose ack is missing, including when no payload work remains or the ESI cap is near.
   - Completed-receiver replay caches the final BlockAck and answers `AckProbe` for at least the sender's full timeout budget.
   - After all active peers are complete, the sender sends `SessionComplete`; this lets receivers leave passive state promptly. Sender safety depends on the already-received BlockAcks, not on receiving a final confirmation.
2. State the fair-loss assumption for eventual completion and the timeout result under permanent control-path failure. No finite lossy handshake can give both sides perfect simultaneous knowledge, so the safe side to favor is: the sender never reports success without every active peer's cumulative completion.
3. Add loss tests for the first completion ack, every ack for several heartbeat periods, the first `SessionComplete`, and receiver handoff to runtime replay.

### Critical — C2: `send_symbol` cannot observe acks or liveness while all trees are backpressured

Stage 1 says to reuse `send_symbol` unchanged and stop a block immediately when its final peer ack arrives. Those requirements conflict.

`send_symbol` performs an internal infinite retry loop. When every usable tree returns `WouldBlock`, it calls `yield_now()` and retries without returning to the outer loop (`dataplane/src/node/session/sender/fec.rs:607-692`). The control receiver is available only to the outer `run` loop (`dataplane/src/node/session/sender/fec.rs:416-478`). Consequently:

- A BlockAck can wait indefinitely while all trees remain blocked.
- The sender can queue at least one stale symbol after an ack even if a tree later opens.
- Per-peer timeout/probe timers cannot run while stuck inside this helper.
- A full control inbox can backpressure the runtime actor while the sender spins.

Pacing has a smaller version of the same race: the sender awaits pacing before calling `send_symbol` and does not re-check global completion between the wait and queue attempt (`dataplane/src/node/session/sender/fec.rs:528-589`).

Required plan edit:

- Preserve the existing flat tree-selection algorithm, but change the helper contract. One sweep should return `Queued`, `AllWouldBlock`, or `AllClosed`; it must not own the wait loop.
- On `AllWouldBlock`, return to a control-aware outer `select!`/state transition, service queued BlockAcks and liveness deadlines, then retry only if the block is still globally incomplete.
- Re-check block/session completion after pacing and immediately before frame submission.
- Count an all-blocked transition as `carousel_backpressure_yields`; test that an ack delivered while every tree is blocked terminates the relevant work without waiting for a tree to reopen.

This is a Stage 1 prerequisite, not an optional optimization.

### Critical — C3: Cumulative does not imply reorder-safe unless sender updates are monotone

The BlockAck payload is cumulative, but the plan does not say how the sender combines reordered frames. Replacing a peer's stored `(watermark, ranges)` with the most recently received frame can regress state when an older heartbeat arrives later. That can resume already-complete blocks and makes `globally_complete` non-monotone.

Required plan edit:

- Define peer ack state as a join-semilattice: an accepted ack unions completed blocks with the existing state, then canonicalizes by advancing the contiguous watermark and merging ranges. It never removes completion.
- Validate `0 <= watermark <= total_blocks`; every extra range must be canonical, lie in `[watermark, total_blocks)`, and not overlap/touch another range. Decide whether ranges touching the watermark are rejected or folded into it.
- Define duplicate and reordered acks as no-ops for completion but valid peer-alive observations.
- Add a property test that applies every permutation and duplication of an increasing sequence of ack snapshots and obtains the same final state, with no block ever transitioning from complete to incomplete.

The range-capacity behavior also needs a rule. `MissingBlockRange` is useful, but an object can have more disjoint completed islands than one frame can encode. Specify pagination/snapshot sequence, or specify a deterministic truncation policy that still guarantees the watermark eventually advances. Test more than the wire maximum of disjoint ranges.

### Critical — C4: “Heartbeats count as progress” makes liveness ambiguous and can be non-terminating

The plan asks for a per-peer “last-ack-progress timestamp” while also saying heartbeats count. An unchanged heartbeat proves that a process/control path is alive; it does not prove decoding progress. If every heartbeat resets a progress timeout, a live but permanently stalled receiver can keep the sender emitting until ESI exhaustion without any bounded no-progress decision. If heartbeats do not extend anything, a valid long transfer can time out despite regular feedback.

The “existing `quorum_liveness` policy” is not directly reusable. It is global rather than per-peer, its timeout is fixed from `started_at`, and `note_feedback_progress` moves only the next solicitation time, not the timeout (`dataplane/src/node/session/sender/state.rs:49-118`).

Required plan edit:

- Track per peer:
  - `last_ack_seen`: updated by any valid duplicate/heartbeat, used for silence/failure timeout.
  - `last_ack_progress`: updated only when the joined completion set strictly grows, used for a separately named stall/no-progress policy.
- State whether a no-progress receiver is aborted, the whole session is aborted, or emission continues until the scheme ID limit. The existing active quorum is frozen and has no peer-removal semantics, so “same abort semantics” currently means whole-session abort.
- Define when these clocks start (after Ready freeze, first payload, or first expected heartbeat), and test a long healthy transfer, a silent peer, a live stalled peer, and peers progressing at different rates.

### Critical — C5: METTLE missing-bin inference is not sound under multi-tree reordering

Stage 2.3 says erasure gaps below the receive frontier are “directly visible.” They are not. The sender departs in bin-id order, but each bin is assigned to one of several trees and `WouldBlock` can change the selected tree. Receivers can observe a higher bin before a delayed lower bin. A maximum received ID therefore proves neither that all lower bins were sent on the same path nor that an absent lower ID was erased.

This is especially important because carousel removes the round boundary. Stage 2.3 nevertheless says to fall back after “k rounds of no progress,” although carousel has no defined repair rounds.

Required plan edit:

- Replace `receive_frontier` with an explicitly safe loss-evidence mechanism. Viable designs include a sender departure checkpoint plus a per-tree drain/reorder fence, or aged gaps associated with a numbered repair epoch and a conservative reorder budget. A control checkpoint that can overtake data on another tree is not sufficient by itself.
- Include the checkpoint/epoch in the METTLE progress feedback. Define how the sender deduplicates requests and when an epoch ends.
- Define “stall region”: source-ID range, bin-ID range, or incident-bin set. Ordered release gives a contiguous decoded-source watermark; it does not by itself expose all equations in the stalled peeling component.
- Test extreme cross-tree reordering with zero loss and assert no retransmission is classified as required; then drop known bins and assert eventual targeted recovery.
- Remove the claim “strictly less duplicate traffic” until measured. False gap detection can create duplicates, and full replay may still be required.

### Critical — C6: Stage 4's namespace contradicts the wire and is not unbounded

METTLE's kernel uses `u128` bin IDs (`mettle/src/stream.rs:13-30`), but lossless `BlockSymbol` carries `symbol_id: u32` (`messages/src/lossless_session/types.rs:249-254`; `messages/src/lossless_session/block_frames.rs:26-74`). The current sender explicitly narrows each kernel bin ID with `u32::try_from` before transmission (`dataplane/src/node/session/sender/fec.rs:169-177`). The bin ID does not “already travel in the METTLE payload.”

Further, eight pass bits represent only 256 passes, not unlimited passes. The decoder work stated in Stage 4.2 is `O(P*l)` per recovered source after `P` active passes, and received equations/graph state also grow with `P` unless an eviction proof is supplied. The current decoder retains only a coupling-window prefix of released source payloads (`mettle/src/decoder.rs:699-720`), whereas a later pass restarted from bin zero may need old recovered payloads to reduce new equations.

Required plan edit:

- Remove Stage 4 from the sequential runtime plan and create a separate experimental, protocol-versioned design.
- Choose an actual wire representation, for example `{ pass_id, raw_bin_id }`, and update the symbol envelope ceiling if the frame grows. Stage 0's geometry ceiling otherwise becomes stale.
- Bound and validate pass count; specify exhaustion instead of “unlimited.”
- Define recovered-source backing (memory, sink reads, or another store), graph/equation eviction, and a measured memory limit.
- Do not equate a fresh namespaced ID with a fresh degree of freedom. Test equation identity/rank or decode gain across seeds before claiming A3 unconditionally.

### Critical — C7: BlockAck needs a transfer incarnation or an explicit non-reuse contract

The wire header identifies a transfer only by `session_id` (`messages/src/lossless_session/header.rs:3-20`). A carousel BlockAck has no round number. If a session ID is reused while an old final BlockAck remains in flight, the old cumulative ack can falsely complete blocks in the new transfer, particularly when geometry matches. Clearing local runtime replay on new session start does not remove frames already in the network.

Required plan edit:

- Prefer adding a sender-generated `transfer_incarnation`/manifest nonce and bind Manifest, Ready, BlockAck, completion controls, and payload frames to it.
- If the project instead guarantees globally unique session IDs over the maximum network/replay lifetime, state that as a normative precondition and add an allocator/reuse test. The current opaque `u64` type and runtime do not enforce it.

## High-severity feasibility and staging issues

### H1: Decoder memory measurement must precede single-stream wire design

The public terminated decoder always selects the dense precomputed graph (`mettle/src/stream.rs:147-169`). That graph contains source-to-bin and bin-to-source adjacency (`mettle/src/decoder.rs:64-83,769-796`), and the decoder also allocates dense seen/received-bin arrays (`mettle/src/decoder.rs:120-214,396-418`). At `n ~= 2^21`, these several O(n) structures and per-bin `Vec` headers/allocations plausibly consume hundreds of MiB before accounting for received symbol payloads. During a long peeling stall, buffered payload memory can itself approach O(n * symbol_size).

Stage 2.4 currently measures after 2.1–2.3, then conditionally changes from an object stream to mega-prefix streams. That choice changes stream IDs, termination tails, ack shape, seed derivation, and wire validation, so it cannot be a late local optimization.

There is also a useful existing alternative: `MettleDecoder::new_terminated` already uses rolling state (`mettle/src/decoder.rs:350-377`), and tests compare it against the precomputed decoder. Only the public `stream::Decoder::new_terminated` forces dense mode. Exposing and benchmarking the rolling terminated path is therefore not a from-scratch stretch goal.

Required plan edit:

- Move 2.4 to **Stage 2.0**, before sender/ack wiring.
- Benchmark dense and rolling decoders at representative source counts, production symbol sizes, loss/reordering patterns, and worst observed stalls. Measure construction latency, steady-state RSS, peak RSS, and event-loop blocking.
- Set an explicit budget, not “if unacceptable.”
- If mega-prefixes are selected, put the prefix source count/stream count derivation in the manifest. Every receiver must construct the same graph; it cannot choose a local prefix size after Ready.
- Construct a potentially expensive decoder before sending Ready or off the async receive loop, and make allocation/size failure a clean manifest rejection.

### H2: The object source mapping and rounds/carousel compatibility are undefined

`BlockPlan::symbol_geometry` is per logical block (`dataplane/src/node/session/plan.rs:110-173`). The current METTLE sender stores a `BlockSpan`, creates one stream per block, and lazily materializes that block's padded source-symbol vector (`dataplane/src/node/session/sender/fec.rs:73-193,769-795`). Changing wire `block_id` to stream ID zero does not create a global object mapping.

The plan must choose one canonical mapping, including padding:

- Is `total_sources = total_blocks * K`, preserving padding at every block boundary?
- Or is the object segmented globally into `ceil(total_bytes / T)` symbols, with padding only on the final source?
- How are a partial final logical block and a partial final source written?
- If mega-prefixes are used, is the source watermark local to each stream, and how are multiple stream progress records carried?

A dedicated `ObjectSymbolPlan` (or equivalent) is clearer than changing `BlockPlan` semantics implicitly.

There is also a mode-matrix conflict. Stage 2 says it kills per-block resets, while the standing constraint says rounds mode remains untouched. Existing rounds-mode METTLE feedback is block-based `NeedReport::Fec`; object-stream carousel feedback is source-watermark based. State explicitly whether:

1. `rounds + METTLE` remains the legacy finite-block adaptation and only `carousel + METTLE` is paper-native, or
2. rounds mode is migrated too, in which case its Need/wire semantics and tests must change.

Do not leave scheme behavior to change implicitly behind the same manifest fields.

### H3: Reservoir repair needs a puncturing/storage design before production config

Stage 3 is a punctured `c_total` graph: reserved bins are deliberately not sent in the initial departure. Several consequences are missing:

- Stage 2's gap detector will see intentional reserve holes as missing unless it can reconstruct and classify the reserve set. Calling them “ordinary erasures” conflicts with delaying them until a stall.
- The current rolling encoder finalizes and discards bins as it advances. To send a reserved bin later, the sender must retain its full payload (roughly `c_reserve * object_bytes`, plus overhead), spill it, or recompute it. Recomputing one bin from possible touchers is not the same O(l)-per-source streaming path.
- `c_total`, `c_wire`, the exact finite reserve cardinality after tail effects, PRF/algorithm version, and seed derivation must be manifest-visible. The current manifest has one coded-rate rational only.
- “Non-TLE bin” needs a precise definition because a bin position used as a source's TLE can also receive non-TLE edges from other sources.
- A reserve bin is fresh on its first global emission, but a receiver can still lose it. Per-peer requests after that point may require retransmission; freshness is a sender-emission property, not a delivery guarantee.

Required plan edit:

- Run the Stage 3.2 codec simulation and a sender storage prototype before wiring defaults.
- Specify exact deterministic reserve selection and initial transmitted count, and prove sender/receiver agreement with test vectors.
- Prefer feedback that reports stalled source regions; the sender can select unsent reserve bins overlapping those regions. Keep intentional reserve holes distinct from accidental missing initial bins.
- Add a hard reserve-memory budget and choose retention, recomputation, or spill behavior.
- Include bursty/GE traces as well as BEC, because the paper notes specifically motivate feedback under time-varying/bursty loss.

The paper supports feedback and rate adaptation (`docs/mettle-part-ii-notes.md:144-172`); it does not specify this reservoir or multi-pass construction. Those stages must be described as extensions from the start, not validated by the citation alone.

### H4: The proposed proof metrics are neither exposed nor all observable as named

`FecSenderStats` and `FecReceiverStats` are private implementation structs, and their detailed logging hooks are no-ops (`dataplane/src/node/session/sender/fec.rs:195-273,1097-1104`; `dataplane/src/node/session/receiver/fec.rs:118-227,728-735`). `ReceiverProgress` exports only timing markers (`dataplane/src/node/session/runtime.rs:111-138`). There are no “existing stats structs” through which the new metrics can be consumed by integration tests.

Metric definitions also need correction:

- The sender cannot directly count packets that were already queued before global completion but arrived afterward. `symbols_after_global_complete` should be split into `symbols_queued_after_final_ack_processed` (must be zero) and receiver-observed `symbols_received_after_local_block_complete` (network/queue tail). Neither alone equals all in-flight tail after the final ack was generated.
- Receiver `duplicate_symbols == 0` is not a protocol invariant if the network can duplicate frames. The enforceable invariant is that the carousel scheduler never emits the same `(block_id, ESI)` twice. Receiver duplicates should be measured, deduplicated safely, and not treated as sender proof without a controlled no-duplication network.
- `emitted_fresh_ids` should have an exact definition that can be checked without retaining an enormous set—monotone start/end/count is sufficient for RaptorQ if every increment is checked.
- `sender_wait_states` needs an enumerated state model and duration/counter semantics; “control drain” is work, not a wait state.

Required plan edit: add a shared per-session metrics/test-observer surface (or deterministic test hook) in Stage 1.5, list the files/API that expose it, and define each counter by an event boundary.

### H5: The dominance assertion is not a correctness gate as written

“Carousel completion <= rounds completion on the same loss trace” is not universally guaranteed by the described schedulers. The modes can send different symbol IDs at different times, use different feedback timing, and distribute repairs differently across blocks. A loss trace keyed by packet ordinal therefore applies to different equations; a trace keyed by time/tree also changes when scheduling changes. Carousel may also round-robin repair across many incomplete blocks while rounds mode targets a reported deficit.

Required plan edit:

- Keep this as a benchmark/smoke result, not a stage correctness invariant, unless the trace coupling and assumptions are formalized.
- Compare matched initial overhead, pacing, tree availability, and channel process; record both completion and total transmissions over many deterministic seeds with a stated tolerance.
- Keep the true conformance assertions separate: eager decode, monotone fresh IDs, no avoidable sender wait, and stop after cumulative quorum completion.

Likewise `h <= 2` is suitable for fixed, pinned RaptorQ test seeds but is not a universal codec invariant. Record the histogram broadly; hard-code only known deterministic fixtures.

### H6: Completion currently acknowledges decode even if sink I/O failed

`write_block` logs file errors but returns `()`; `complete_block` then inserts the block into `complete_blocks` regardless (`dataplane/src/node/session/receiver/mod.rs:591-631`; `dataplane/src/node/session/receiver/fec.rs:578-590`). METTLE source-run writes have the same shape (`receiver/mod.rs:633-695`). A cumulative BlockAck becomes authoritative and lets the sender discard the object, so the plan must say whether “complete” means decoded in memory or successfully committed to all configured sinks.

Required plan edit: either make sink writes fallible and ack only after the chosen durability condition succeeds, or explicitly scope the protocol invariant to decode completion and expose sink failure separately. Add an injected sink-error test.

### H7: The actual protocol/code surface is wider than the listed files

Stage 1 and later require coordinated changes beyond `sender/fec.rs` and the messages control frame:

- `messages/src/lossless_session/types.rs`, `control_frames.rs`, `validation.rs`, `header.rs`/version docs, and test support;
- `dataplane/src/node/config.rs` and `session/fec_policy.rs` for `fec_feedback_mode` and later METTLE fields;
- `session/sender/mod.rs` and `sender/state.rs` for BlockAck dispatch and per-peer timers;
- `session/receiver/mod.rs` for debounce/heartbeat timer events and passive completion;
- `session/api.rs` and `session/runtime.rs` for completed BlockAck replay/probes;
- both block-symbol encoders (`messages/.../block_frames.rs` and `dataplane/.../sender/block_symbol_frame.rs`) if the symbol ID changes;
- packet-envelope constants if a wider ID changes the maximum symbol payload.

The protocol version should be bumped when the manifest/control/block frame layouts change. Current source comments already disagree about versions while the constant is version 7 (`messages/src/lossless_session/mod.rs:1-5`; `types.rs:3-7`); make the normative version/layout table part of the plan.

Stage 1 also refers to a configured RaptorQ initial overhead, but no such knob exists. RaptorQ's current initial count is exactly K (`dataplane/src/node/session/fec.rs:294-312`). Delete that phrase or add and define a real sender configuration separately.

## Missing invariants and acceptance tests

The following should be explicit stage-gate requirements rather than implied coverage.

### Stage 0 boundaries

1. RaptorQ accepts `K=1` and `K=56_403`, rejects `K=0` and `56_404`; exercise the smallest and largest valid symbol sizes.
2. Derive the current maximum FEC payload from the IPv4/TCP/lossless frame constants (currently 65,535 minus 20-byte IPv4, 36-byte TCP-with-lossless-option, 20-byte lossless header, and 16-byte BlockSymbol metadata = 65,443 bytes). Test exactly max and max+1; do not duplicate a magic number across crates.
3. Test checked `K*T`, object/block padding, last partial block, empty object, and host `usize` conversion failures.
4. RaptorQ accepts ESI `2^24-1`, rejects `2^24`; the local generator stops before wrap and returns a protocol error.
5. METTLE accepts the last terminated bin ID and rejects `terminal_end_exclusive` and unrepresentable IDs before the decoder sees them.
6. Fuzz/property-test malformed symbol/control frames around body lengths, IDs, and range counts; no dependency assertion may be reachable from peer input.
7. Test sink allocation/write failure under the chosen completion definition.

### Stage 1 protocol and carousel

1. **Ack join invariant:** duplicates and all reorderings produce the same monotone peer state.
2. **Ack-loss liveness:** drop the first ack, multiple consecutive heartbeats, and the final local-completion ack; sender eventually completes under fair delivery.
3. **Completion handoff:** drop `SessionComplete`, let the live receiver exit, and recover the cached final BlockAck through the defined probe/replay path.
4. **Incarnation safety:** delayed Ready/BlockAck/data from an older transfer cannot affect a new transfer reusing local routing state.
5. **Backpressure responsiveness:** with every tree blocked, deliver an ack and a liveness deadline; the sender services them before any later queue opportunity.
6. **Pacing race:** deliver the final ack during a pacing wait; no post-check symbol is queued for that block.
7. **Freshness:** for each block, every sender-emitted RaptorQ ESI is strictly increasing and below `2^24`, regardless of tree fallback, control reordering, or backpressure.
8. **Quorum safety:** a block becomes global-complete iff every frozen active peer has acked it; non-quorum/missing-identity acks are ignored. Define and test an empty active quorum (abort, send-only completion, or explicit no-receiver success) rather than relying on vacuous truth.
9. **Timer fairness:** continuous data/control readiness cannot starve debounce, heartbeat, silence timeout, or probe deadlines. Use paused Tokio time rather than wall-clock sleeps.
10. **Range scaling:** zero blocks, watermark at total, maximum ranges, too many disjoint ranges, pagination/truncation, out-of-range and noncanonical forms.
11. **Eager decode:** after each unique pooled arrival, independently attempt decode and assert the receiver marks completion at the first successful set. Permute tree labels for the same pooled set to enforce L1.
12. **Metric semantics:** assert `queued_after_final_ack_processed == 0`; separately measure late receiver arrivals. Do not infer network tail from an unobservable sender counter.

`dataplane/tests/fec_multitree.rs` is currently a small striping/packet-capture test, not a lossy multi-receiver timeline harness. Reusing its setup style is fine, but Stage 1.6 needs a new deterministic channel/harness capable of loss, delay, reorder, duplication, backpressure, and per-peer observation.

### Stage 2 single-stream METTLE

1. A source-ID/object-offset property test over multi-block objects, non-divisible block/symbol sizes, mega-prefix boundaries, and the final partial source; reconstructed bytes must equal the exact object with no block-padding leakage.
2. Exactly one graph seed/stream for the one-stream mode; for prefix mode, deterministic stream IDs, source counts, seed derivation, and ack watermarks per stream.
3. Rounds/carousel mode-matrix tests proving legacy rounds behavior is unchanged if that is the chosen contract.
4. Zero-loss but heavily reordered multi-tree delivery must not create false “confirmed missing” bins under the new frontier/epoch rule.
5. Known losses near and far from the decoded watermark; targeted repair must make progress, deduplicate requests, and fall back after a precisely defined no-progress condition.
6. Decoder duplicate/invalid/pending outcomes must be distinguishable for metrics. The current public `push_bin` returns an empty decoded vector for all three cases.
7. Dense-versus-rolling equivalence on the session path, plus construction/RSS/peak buffered-payload gates at the selected maximum stream size and maximum supported simultaneous sessions.
8. Wire bounds for global source count (`u64` internally), terminal bin count (`u128` internally), and the chosen transmitted ID width.
9. Harness accounting asserts actual finite transmitted count, including compressed tail, for each target overhead. Statistical claims need enough trials/confidence to support the stated failure probability.

### Stage 3 reservoir

1. Stable selection test vectors; exact reserve cardinality; no selected TLE positions; checked rational arithmetic for `c_total`.
2. Sender and receiver classify every initial hole identically as intentional reserve versus accidental loss.
3. Each reserve ID is emitted at most once before exhaustion across multiple peers and reordered stall reports; retransmission after loss is counted separately.
4. A receiver that stalls later does not cause already-emitted reserve symbols to be mislabeled fresh.
5. Peak sender reserve-payload storage/recompute cost is within an explicit budget.
6. Sweep BEC and GE/bursty traces, report confidence intervals, actual total finite overhead, completion probability, repair latency, duplicate traffic, and memory—not only repair-round count.

### Stage 4 research prototype

Before any runtime integration: wire round-trip at pass/ID bounds, stable seed-hash vectors, cross-pass peeling correctness, already-released-source reduction, pass exhaustion, equation-identity/decode-gain measurements, and an enforced memory cap as pass count grows.

## Concrete edits to the master plan

The following changes would make the plan executable in dependency order.

### 1. Add a “Protocol assumptions and state machines” section before Stage 0

Specify:

- fair-loss versus permanent-failure behavior;
- frozen active quorum and empty-quorum semantics;
- transfer incarnation/session-ID reuse rule;
- BlockAck canonical form and monotone join;
- receiver states `Active -> LocallyComplete/Passive -> Finished`;
- sender states `Sending -> AwaitingAck/Probing -> QuorumComplete -> Finished`;
- `AckProbe`, `SessionComplete`, completed replay, and their timers;
- separate silence and no-progress clocks;
- exact meanings of “emitted,” “queued,” “delivered,” “decoded,” and “complete.”

Make this normative and versioned. Stage 5 can later produce user-facing documentation from it.

### 2. Amend Stage 0

- Put checked geometry in a dependency-appropriate shared module. `messages` currently has no dependency on `mettle`, `raptorq`, or dataplane packet layout, so “one constructor used by manifest validation, sender, receiver” needs an explicit design: either a dependency-free wire-geometry type in `messages`, or dataplane validation layered after syntactic message validation. Do not copy codec formulas into three crates.
- Derive the symbol payload ceiling from frame/envelope constants and account for future wire-width changes.
- Make local encoding/decoding constructors and sink completion fallible.
- Add all exact boundary/property cases listed above.

### 3. Replace Stage 1.1–1.4 with a complete carousel protocol slice

- Define `FecFeedbackMode` in config and manifest and bump the protocol version.
- Define BlockAck as an extensible enum from the start, e.g. `Blocks { watermark, extra_completed }` with a reserved METTLE progress discriminant. Validate it against the installed manifest.
- Define sender ack merge as union/canonicalization, never replacement.
- Add probe/closure controls and runtime replay before claiming ack-loss healing.
- Replace the internal all-blocked retry loop with a one-sweep result and a control/timer-aware outer state machine; re-check after pacing.
- Add per-peer `last_seen` and `last_progress` clocks and name their policies.
- Add a real session metrics/test-observer API with observable counter definitions.
- Delete the nonexistent “configured initial overhead” text unless a real knob is added.

Gate Stage 1 on deterministic loss/reorder/backpressure/teardown tests, not only the happy-path conformance smoke.

### 4. Reorder and clarify Stage 2

New order:

1. **2.0 memory/layout spike:** benchmark dense and existing rolling decoder; choose object versus negotiated mega-prefix and a hard budget.
2. **2.1 object symbol plan:** define global source size/count, padding, stream IDs, prefix boundaries, and sink mapping.
3. **2.2 wire/mode matrix:** define whether rounds stays legacy; add per-stream scheme-tagged progress fields.
4. **2.3 sender/receiver single-stream implementation.**
5. **2.4 reorder-safe repair epochs/frontiers** and full-replay fallback with a defined no-progress counter.
6. **2.5 delete or fix dead `repair_deficit`.** Deletion is preferable if the carousel path no longer uses it.
7. **2.6 accounting harness**, moved early enough that Stage 3 uses actual finite overhead.

Do not use `BlockPlan` wording alone for the source mapping, and do not use “rounds” in a carousel fallback without defining repair epochs.

### 5. Make Stage 3 simulation-first

Split Stage 3 into:

- 3.0 deterministic puncturing/reserve prototype and storage measurement;
- 3.1 simulations (BEC plus bursty traces) using actual finite counts;
- 3.2 only then freeze manifest fields/defaults and integrate sessions.

State how reserve holes are excluded from Stage 2 accidental-loss feedback and how unsent versus previously emitted reserve bins are tracked globally across peers.

### 6. Move Stage 4 to a separate vNext research plan

The new plan must choose wire ID width, pass bound, raw-ID validation, seed hash, source-payload backing, pass garbage collection, and equation freshness criteria before code integration. Keep it default-off, but do not gate the otherwise-complete runtime/docs on it.

### 7. Adjust stage gates and documentation timing

- Add protocol model/property tests to Gate 1.
- Make RSS/construction thresholds and maximum active streams explicit in Gate 2.
- Require recorded sender memory as well as decoder curves in Gate 3.
- Treat rounds-versus-carousel dominance as benchmark evidence, not a universal pass/fail invariant.
- Document reservoir and multi-pass as beyond-paper extensions at the stage where they are introduced.

## Recommended dependency order

1. Stage 0 safety/baseline, including exact wire geometry and fallible completion semantics.
2. Normative carousel protocol, incarnation, ack join, timers, and teardown/replay.
3. Control-responsive send-loop refactor plus metrics/test harness.
4. RaptorQ carousel and its full adversarial conformance suite.
5. METTLE decoder/layout benchmark and negotiated stream geometry.
6. Single-stream METTLE plus reorder-safe targeted repair and corrected accounting.
7. Reservoir simulation/storage prototype; integrate only if its measured gate passes.
8. Documentation/conformance polish.
9. Multi-pass reseed as a separate wire-versioned research effort.

## Bottom line

Stage 0 can start after small clarifications. Stage 1 should not start from the current text: without monotone merge, control-aware backpressure, distinct liveness clocks, incarnation binding, and a final-ack/teardown path, the carousel can regress, hang, or falsely complete. Stage 2's core single-stream goal is implementable, and the existing rolling decoder improves its prospects, but memory/layout must be decided and negotiated before the wire is changed. Stage 3 needs simulation and storage evidence first. Stage 4 is incompatible with the current `u32` symbol wire ID and should be redesigned independently rather than treated as the final sub-stage of this branch.
