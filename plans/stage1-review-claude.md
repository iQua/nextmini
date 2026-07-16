# Stage 1 review (Claude)

Date: 2026-07-16
Scope: commits `e4d2971..e82cc18` (7 sub-stage commits + report) against plan v2 §P and Stage 1
(`plans/perfect-fec-runtime.md`), plus the two Stage 0 findings folded into 1.3.
Verdict: **APPROVED with required follow-up fixes before Stage 2.** One MAJOR (pre-existing runtime
deadlock made realistic by carousel), a set of MINORs, and two WEAK conformance items. No finding
invalidates the Stage 1 protocol design or its committed test evidence.

## Verification performed

- Independent Gate 1 re-run on `e82cc18`: `cargo fmt --check` clean; workspace clippy `-D warnings`
  clean; `cargo nextest run` → 737 passed, 0 failed, 17 pre-existing skips. Worktree clean.
- Four independent code reviews (wire/messages layer; sender carousel + state; receiver + runtime;
  conformance-suite coverage audit), each against the normative §P text, all with file:line evidence.
- Deadlock finding (M1) verified by hand in the actor source before recording.
- `fec_round_regressions.rs` untouched by any Stage 1 commit (verified via git), still green.
- Benchmark evidence file satisfies H5: matched seeds, stated tolerances, explicitly not a CI gate.

## Requirements verified met (summary)

- **1.0/P9**: `FecFeedbackMode{Rounds(default), Carousel}` with serde back-compat; version 7→8 with a
  normative layout table in `messages/src/lossless_session/mod.rs:3`; unknown modes/versions rejected
  at decode; Carousel+METTLE rejected on both dataplane ends. P2 allocator: OS-seeded CSPRNG u64,
  rejects zero, retains issued ids (`controller/src/utils.rs:21`).
- **1.1/P4**: extensible BlockAck with reserved METTLE discriminant; watermark/range validation with
  fold-on-receipt (not reject); deterministic lowest-id truncation (`for_wire`); decoder totality over
  peer bytes (checked adds, count-before-multiply, exhaustive malformed-input sweeps).
- **1.2/P5/P8**: Active→LocallyComplete→Finished; debounce 8ms/heartbeat 300ms in spec range; complete
  advertised only after sink commit; SinkError never acked; passive-window ≥ stall_timeout + margin
  asserted in `fec_policy.rs:157` with checked adds; runtime replay cache answers probes post-exit
  with target-peer filtering; final ack cannot be indefinitely debounced (`get_or_insert` never
  extends the deadline).
- **1.3/C2**: `send_symbol` is one synchronous sweep returning `Queued|AllWouldBlock|AllClosed`
  (`sender/fec.rs:1160`), no internal wait loop; outer control-aware select over {inbox, per-peer
  timers, pacing}; completion re-checked after pacing and immediately before submission; Phase A
  block-sequential `0..K`, Phase B one fresh repair ESI per globally-incomplete block, round-robin,
  cursor advances only on Queued; monotone `next_after`; acked-complete blocks never rescheduled.
- **1.4/P7**: per-peer `last_ack_seen`/`last_ack_progress` with distinct configurable timeouts
  (3s/15s defaults, ordering validated); either expiry aborts the whole session; clocks start at the
  Ready-quorum freeze with no intervening await; empty frozen quorum ⇒ immediate trivial success;
  rounds `QuorumLiveness` untouched, not reused.
- **P6/P1**: Probing targets exactly the ack-missing peers while emission continues; `Completed` only
  via `carousel_quorum_complete` (cumulative completion from every frozen peer); SessionComplete is
  best-effort ×3 with no dependence on receiver confirmation; no other Completed path exists.
- **Stage 0 findings**: both applied — exhaustion aborts only when another emission is required, with
  completion re-checked from joined state (pinned by
  `last_valid_repair_esi_only_exhausts_on_the_next_required_emission`); `patch_tree_id` fully checked.
- **1.5**: shared Arc `SessionMetrics` + caller-owned test hooks replace the no-op stubs; every
  planned counter present and recorded at the correct P8 event boundary.
- **1.6**: 10/12 required conformance items SOLID (ack-join permutation; ack-loss liveness;
  completion handoff; backpressure responsiveness with all trees blocked; pacing race; tree-fallback
  freshness + stale-ack reorder; quorum incl. empty; range scaling through the production `for_wire`;
  eager decode + tree-label permutation; metric semantics). Suite deterministic, <0.5s, no RNG, no
  real network, paused time where required.

## Required fixes (Codex, before Stage 2)

1. **MAJOR — lossless runtime actor deadlock at receiver-completion handoff.**
   `runtime.rs:357/372/423` is a single actor that both answers the `ReceiverCompleted{ack}`
   handshake and delivers frames inline via a blocking `inbox.send(frame).await`
   (`deliver_live_receiver`, runtime.rs:800). A completed receiver stops draining its data inbox and
   blocks in `register_completed_replay` awaiting the ack (`receiver/mod.rs:753`). If a `Deliver` for
   that session sits ahead of `ReceiverCompleted` in the actor queue while the data inbox is full of
   carousel tail symbols, the actor blocks forever and the entire runtime (all sessions) wedges.
   Mechanism pre-exists Stage 1, but carousel tail traffic makes it realistic and the Stage 1 replay
   handoff depends on this path. Fix direction: non-blocking `try_send` in `deliver_live_receiver`
   (drop-on-full is protocol-legal for data; controls can fall through to the replay path), and/or
   have the receiver keep draining inboxes until the ack arrives. Add a conformance test that fills
   the data inbox, completes the receiver, and asserts the runtime stays live.
2. **MINOR — completed transfer reported Aborted on control-channel close** (`sender/fec.rs:562-566`
   and the wait-branch returns at :934, :963, :985): `TryRecvError::Disconnected` is mapped to Abort
   before `carousel_quorum_complete` is consulted; if the final completing ack was consumed inside a
   wait branch and the runtime then drops the inbox, a fully-acked transfer aborts. Check
   quorum-completion before mapping disconnect to Abort (rounds `drain_controls` treats disconnect as
   empty — mirror that, then let the loop conclude).
3. **MINOR — nested pace/backpressure wait starves timers under ack flood** (`sender/fec.rs:925-951`):
   the `biased` select polls `ctrl_rx.recv()` first each iteration, so a continuous stream of valid
   no-progress acks defers per-peer liveness expiry and due probes indefinitely while parked in the
   nested wait. Bound consecutive control polls per wait (constant exists: MAX_CONTROLS_PER_BOUNDARY)
   or re-check timers inside the nested loop.
4. **MINOR — probe dropped by both live task and cache during the handoff window**
   (`receiver/mod.rs:296-301` + runtime.rs:423): frames delivered between receiver loop-exit and
   `ReceiverCompleted` processing are reported Delivered into an inbox nobody reads; P5's "no window"
   requirement is only saved by sender re-probing. Fold into the fix for (1): once a receiver has
   queued `ReceiverCompleted`, probes must be answerable from the replay entry.
5. **MINOR — replay cache growth is unbounded** (`runtime.rs:662-671`): eviction is lazy and only
   fires for `Carousel` entries on a frame for that exact session; `Plain`/`Fec` entries never
   expire. Add insert-time or periodic sweeping eviction.
6. **MINOR — pacing double-charge on backpressure retry** (`sender/fec.rs:611-626` vs :669-678):
   a symbol retried after `AllWouldBlock` re-charges the token bucket (and regenerates the RaptorQ
   payload) each sweep, over-throttling after trees reopen. Charge pacing once per emitted symbol and
   cache the generated payload across retries of the same (block, ESI).
7. **MINOR — invalid carousel config now fails plain/rounds preflight** (`runtime.rs:537, :603`):
   `validate_carousel_timing` runs for every session. Scope it to sessions that actually negotiate
   Carousel (or validate at config load with a clear error) so rounds-only nodes keep starting.
8. **MINOR — duplicate flow delivery starts two live transfers** (`controller/src/new_node.rs:208`,
   `controller/src/db_sync.rs:145`): random session ids removed the accidental idempotence of
   deterministic ids (`SessionAlreadyActive`). Deduplicate by flow key before allocating a session id.

## Test strengthening (same pass)

- T1 Incarnation/stale-frame conformance is WEAK: only a single stale Manifest on a fresh slot with a
  timeout-absence assertion. Add: reuse of a session slot across two transfers, stale payload symbols
  and stale acks injected into the live successor, positive assertion the successor completes clean.
- T2 Timer-fairness test is WEAK: the 64-frame burst can drain before the timer check, and only the
  debounce timer is exercised. Keep the inbox continuously refilled during the assertion window and
  cover the heartbeat timer.
- T3 Passive-window expiry termination (SessionComplete dropped forever; receiver Finishes on window
  expiry per P5) has no test — only post-exit cache expiry is covered.
- T4 `record_queued_after_final_ack` has no positive-increment test, so the `== 0` assertions cannot
  distinguish "invariant holds" from "counter never fires". Drive it nonzero once via a direct unit
  test of the metrics hook.
- T5 The allocator "reuse test" (`controller/src/utils.rs:788`) never exercises retention (two random
  u64s are distinct anyway). Test the issued-set directly (pre-seed the set, assert re-draw).
- W1 messages API: `encode_control` accepts a validate()-accepted-but-unfolded BlockAck and emits
  non-canonical bytes (fold happens on receipt). Canonicalize in encode (or debug_assert canonical),
  and add the missing `validate_control(BlockAck)`-vs-Rounds/Plain-manifest rejection test.

## Nits (opportunistic)

- Latent busy-livelock: if `block_ack()` returns None while `carousel_ack` is armed, the expired
  deadline is never cleared (`receiver/mod.rs:540-548` + :334-339) — add a guard/clear.
- `Instant::now() + Duration::from_millis(cfg)` panics on absurd-but-valid configs
  (`receiver/mod.rs:510`); use saturating add like the rest of the timer math.
- Receiver silently ignores a Carousel+METTLE manifest (`receiver/mod.rs:822-828`) instead of a
  distinct rejection — revisit when Stage 2 lifts the restriction.
- `#[path]`-recompiled `state.rs` in the conformance crate; MAX_CONTROLS_PER_BOUNDARY staleness at
  boundary checks (bounded, self-correcting).

## Cross-check note

The coverage audit initially reported the P2 reuse test as missing; the receiver/runtime review
located it in `controller/src/utils.rs:788` (it lives in the controller crate). The Stage 1 report's
claim stands, but the test is weak per T5.
