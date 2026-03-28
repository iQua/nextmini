# Simple Lossless Protocol Rewrite Plan

## Goal

Rewrite the lossless session protocol so that:

- slow trees do not hold fast trees idle
- receivers report residual need immediately after a sender burst boundary
- sender may start repair after the first useful feedback
- sender still has deterministic final completion semantics
- the implementation becomes simpler for the right reason:
  - better-defined rounds
  - explicit replay/staleness rules
  - phased cleanup of dead or duplicated wire surface

This revision intentionally changes the original plan.

The new priority order is:

1. define the round and quorum state machine correctly
2. implement `SourceDone` and `Need` around that state machine
3. update replay and stale-control handling
4. only then simplify the wire format in low-risk phases

## Why The Original Rewrite Plan Was Too Aggressive

The prior version tried to combine:

- a new round model
- early repair start
- removal of multiple control kinds
- removal of multiple manifest and payload fields
- removal of tree-related wire fields
- header simplification

in one rewrite.

That is the wrong order.

The highest-risk parts are not codec mechanics. They are:

- under-specified round closure
- stale and duplicate `Need`
- lost or duplicated `SourceDone`
- feedback quorum versus `ready_grace`
- replay for partially-complete receivers
- hidden dependencies on tree identity in the current dataplane

This plan therefore splits the rewrite into:

- **Phase 1: behavioral rewrite**
- **Phase 2: selective wire cleanup**

## Current System Constraint Summary

The current protocol is built around a strict global barrier:

`Manifest -> Ready -> payload sweep -> Eot -> wait for all receiver reports -> next burst`

That barrier avoids stale-control problems, but it also leaves fast trees idle.

The dataplane also has two distinct transport contracts:

- `TreeVisibleNonBlocking`
- `SharedQueueNonBlocking`

So any claim that “faster trees naturally send more” is only correct on tree-visible ingress paths. It is not universally true on shared-queue paths.

Phase 1 also does **not** redesign control routing.

`SourceDone` and `Need` continue to use the existing control-path routing contract.
This rewrite improves utilization mainly by removing the full-report repair barrier, not by assuming control messages have zero latency.

## Phase 1 Protocol Surface

Phase 1 focuses on correctness first.

### Keep

1. `Manifest`
2. `Ready`
3. `BlockData`
4. `BlockSymbol`
5. `SourceDone`
6. `Need`

### Remove In Phase 1

1. `Eot`
2. `PlainStatus`
3. `FecStatus`
4. `BlockAck`
5. standalone `BlockStatus`

### Explicitly Defer To Phase 2

1. inner header simplification
2. removing inner `session_id`
3. removing manifest `tree_ids`
4. removing `Ready.node_id`
5. removing `BlockSymbol.tree_id`
6. removing `payload_len`

These may still be good ideas, but they are not part of the first correctness pass.

## Phase 1 Semantics

## SourceDone

`SourceDone { round_id }` replaces `Eot`.

It means:

- the sender has finished injecting burst `round_id`
- it does **not** mean all symbols from that burst have drained from the network

It exists to:

- open one feedback window
- allow receivers to compute one residual snapshot
- provide a replay/synchronization point when control is lost or reordered

## Need

`Need { round_id, payload }` is the only receiver-to-sender report.

`Need(round_id)` is defined as:

- a **full snapshot of the receiver’s current residual object need**
- not a delta against burst `round_id`
- not “what was missing from the sender’s most recent burst”

This definition is essential.

It ensures a late `Need(r)` from a slow receiver is still meaningful as long as round `r` remains open.

### Need payload by mode

- plain mode: missing block ranges
- FEC mode: per-block deficit symbols

### Empty Need

An empty `Need(round_id)` means:

- this receiver currently needs nothing more to complete the object

There is no separate `Complete` report type.

## Round Model

### Round numbering

- round `0`: initial source burst
- round `1`: first repair burst
- round `2`: second repair burst
- ...

### Burst identity versus feedback-open round

`round_id` names a completed sender burst.

`SourceDone(r)` means:

- burst `r` has finished injecting
- feedback for burst `r` is now open

The sender may begin emitting burst `r + 1` after the first useful `Need(r)` arrives, even while feedback for `r` remains open.

But the protocol still allows only one feedback-open round at a time:

- feedback for `r + 1` does **not** open until `SourceDone(r + 1)` is emitted
- `SourceDone(r + 1)` must not be emitted until round `r` is closed

This is the key rule that preserves simple replay/staleness semantics while still allowing early repair start.

### Core invariant

There is exactly **one feedback-open round at a time**.

The sender never has multiple open feedback rounds concurrently.

### Same-round `Need` immutability

In Phase 1:

- a receiver emits at most one `Need(round_id)` per observed `SourceDone(round_id)`
- duplicate `SourceDone(round_id)` may trigger replay of that **same exact** `Need(round_id)`
- same-round changed `Need` payloads are **not allowed**

If the project later wants same-round mutable updates, add an explicit `report_seq`.
That is not part of Phase 1.

### Round closure

A round does **not** close just because repair work drains.

A round closes only when:

1. every peer in the active session quorum has reported for that round
2. all work induced by that round’s merged `Need(round_id)` snapshots has been fully emitted
3. there is no pending same-round solicitation or replay work left to send

### Round advancement

The sender may advance from round `r` to round `r + 1` only if:

1. round `r` is closed
2. the sender emitted a **non-empty burst `r + 1`** in response to round `r`

This prevents opening meaningless empty rounds.

### Completion

The sender may declare session completion only when:

1. the current round is closed
2. every quorum peer reported empty `Need(round_id)` for that closed round
3. the sender has no remaining work

If round `r` closes with all-empty `Need(r)`, the session completes at `r`.
It does **not** open a synthetic empty round `r + 1`.

## Missing-Peer Handling

This rewrite does **not** use a settle timer for repair start.

But it also does **not** allow missing-peer reports to be ignored.

If merged work drains while some quorum peers have not yet reported for the open round:

- the sender keeps the same round open
- the sender retransmits `SourceDone(round_id)` on a solicitation interval
- the sender does **not** advance the round

That rule handles:

- lost `SourceDone`
- delayed control delivery
- replay triggers for duplicate `SourceDone`
- missing peer reports without opening spurious empty rounds

This plan intentionally chooses same-round solicitation/replay instead of a settle timer or completion-only grace window.

## Ready And Quorum Semantics

The old rewrite left this ambiguous. Phase 1 makes it explicit.

### Active session quorum

The sender has two peer sets:

1. configured receivers
2. active session quorum

The active session quorum is:

- the set of peers that successfully send `Ready` before the data gate opens

The data gate opens when:

- all configured receivers are ready
- or `ready_grace` expires

### Freeze point

The quorum freezes when the sender emits the first payload burst for round `0`.

Late `Ready` after that point does not join the quorum for that session.

### Final completion

Only quorum peers count toward final completion.

This avoids a protocol deadlock where `ready_grace` opens the data gate but non-ready peers still count toward the final completion quorum.

## Tree Identity Scope

The plan now uses a narrower and more accurate invariant:

**Tree identity is not part of the decoding or reconstruction contract, but it remains part of the outer transport and scheduling contract.**

That means:

- decoder correctness should not depend on tree identity
- transport routing and backpressure may still depend on tree identity
- manifest `tree_ids` are not removed in Phase 1
- throughput claims about “fast trees naturally send more” are scoped to tree-visible ingress paths

## Migration And Versioning

This is a hard wire break.

Phase 1 therefore includes an explicit migration policy:

- bump `LOSSLESS_SESSION_VERSION`
- recommended rollout model: **flag-day cluster upgrade**
- mixed-version interoperability is **not** supported in Phase 1
- decoders reject unsupported versions immediately

If mixed-version rollout becomes necessary later, that is a separate dual-stack effort.

## Sender Algorithm

### Shared sender rules

For both plain and FEC:

1. open round `0`
2. send initial burst
3. emit `SourceDone(0)`
4. keep round `0` feedback-open
5. begin emitting burst `1` as soon as the first useful `Need(0)` arrives
6. continue merging additional `Need(0)` snapshots from other quorum peers while round `0` remains open
7. if burst `1` drains but not all quorum peers reported for round `0`, retransmit `SourceDone(0)` and keep waiting
8. once round `0` closes:
   - complete immediately if all reports were empty
   - otherwise, if burst `1` has already drained, emit `SourceDone(1)` immediately
   - otherwise wait until burst `1` drains, then emit `SourceDone(1)`
9. repeat the same pattern for round `r` and burst `r + 1`

### Plain mode

For a given open round:

- `Need(round_id)` contributes a full-snapshot missing block range set
- sender merges all reported missing ranges into one retransmit block set
- retransmits are idempotent

### FEC mode

For a given open round:

- `Need(round_id)` contributes a full-snapshot per-block deficit map
- sender merges peer reports by taking the max deficit per block
- sender may begin repair as soon as first useful report arrives
- sender uses work-conserving tree scheduling where transport contract allows it

### Throughput claim scope

This plan only claims fast-tree utilization improvements on:

- `TreeVisibleNonBlocking` ingress paths

On shared-queue paths:

- protocol latency may improve
- but tree-proportional throughput is not guaranteed

## Receiver Algorithm

### Shared receiver rules

For both plain and FEC:

1. install manifest
2. send `Ready`
3. on `SourceDone(round_id)`, compute one full residual snapshot
4. send `Need(round_id, snapshot)` immediately
5. cache that `Need(round_id)` for replay
6. if duplicate `SourceDone(round_id)` arrives, replay the exact cached `Need(round_id)`

Late-arriving source data after `SourceDone(round_id)` is allowed.

It does not mutate the already-issued `Need(round_id)`.

It only affects what the receiver reports in the next round.

### No settle timer

There is no settle timer for repair start.

Rationale:

- fast trees should not be held idle waiting for a timer
- speculative repair is acceptable
- correctness is preserved by full-snapshot `Need`, same-round replay, and explicit round closure rules
- oversend is an accepted latency tradeoff in Phase 1, especially in FEC mode

## Runtime Replay

Replay must become round-aware.

The current runtime only replays completed receiver status after duplicate payload or `Eot`.

Phase 1 replay must support:

- latest cached non-empty `Need(round_id)`
- latest cached empty `Need(round_id)`
- duplicate payload
- duplicate `SourceDone(round_id)`

Replay must never synthesize a changed same-round `Need`.

Replay must never emit feedback for a closed round.

Replay must preserve the single-feedback-open-round rule even if later bursts have already been partially emitted.

## Phase 2 Cleanup Rules

Phase 2 cleanup happens only after Phase 1 semantics are proven in tests.

### Safe candidates for Phase 2

1. remove `payload_len`
2. migrate `Ready` to empty payload if transport `peer_id` is proven on all handshake paths
3. consider removing inner `BlockSymbol.tree_id` if outer transport metadata is sufficient everywhere

### Explicitly deferred until a separate tree-contract audit

1. remove manifest `tree_ids`
2. remove inner `session_id`
3. simplify the inner frame header aggressively

Those are not part of the core rewrite.

## Invariants

These invariants must be encoded in tests before substantial implementation work:

1. `Need(round_id)` is a full residual snapshot, not a delta.
2. Only one feedback-open round exists at a time.
3. A receiver emits at most one distinct `Need(round_id)` per round.
4. Duplicate `SourceDone(round_id)` causes replay of the same cached `Need(round_id)`.
5. Same-round changed `Need` payloads are rejected in Phase 1.
6. Sender may start repair after the first useful `Need(round_id)`.
7. Sender may not close a round until every quorum peer reported.
8. Sender may not advance the round just because merged work drained.
9. Sender retransmits `SourceDone(round_id)` while waiting for missing peer reports.
10. Sender completes only after all quorum peers report empty `Need` for the closed round.
11. Quorum freezes when the first payload burst is emitted.
12. Only quorum peers count toward final completion.
13. Tree identity is transport/scheduling metadata, not decoder correctness metadata.
14. Fast-tree utilization claims are limited to tree-visible ingress paths.
15. The sender may emit burst `r + 1` before round `r` closes, but may not emit `SourceDone(r + 1)` until round `r` is closed.
16. If round `r` closes with all-empty `Need(r)`, the session completes without opening `r + 1`.

## TDD Rule

Every implementation task below follows Red/Green:

- add or update failing tests first
- implement the smallest change that makes them pass
- refactor only after green

## Dependency Graph

```text
T1 -> T2
T1 -> T3
T1 -> T4
T1 -> T5
T1 -> T6
T2 -> T4
T2 -> T5
T2 -> T7
T2 -> T8
T2 -> T9
T2 -> T10
T3 -> T4
T3 -> T5
T3 -> T11
T4 -> T8
T4 -> T9
T4 -> T10
T4 -> T11
T5 -> T8
T5 -> T9
T5 -> T10
T5 -> T11
T6 -> T12
T7 -> T8
T7 -> T9
T7 -> T10
T8 -> T9
T8 -> T10
T8 -> T11
T9 -> T11
T10 -> T11
T7 -> T11
T11 -> T12
T11 -> T13
T12 -> T13
T13 -> T14
```

## Detailed Tasks

### T1. Freeze the revised protocol spec and invariants

- depends_on: []
- Write a short protocol spec section in `plans/simple-lossless.md` and mirror the critical rules in code comments near the message definitions.
- Explicitly define:
  - `Need(round_id)` as full residual snapshot
  - one feedback-open round at a time
  - same-round `Need` immutability
  - `SourceDone(round_id)` replay/solicitation semantics
  - round closure rules
  - round advancement rules
  - quorum freeze semantics
  - tree identity scope
- Add red tests that encode those invariants before implementation begins.

Acceptance criteria:

- The new semantics are written down in one place.
- The riskiest rules exist as failing tests before code changes.

### T2. Introduce explicit shared round and quorum state types

- depends_on: [T1]
- Add shared sender-side types for:
  - feedback-open round id
  - current burst id
  - per-peer report status
  - quorum membership
  - whether a non-empty next burst has been emitted in response to the open round
  - whether all work induced by the open round has drained
  - whether the next `SourceDone` is pending on round closure
- Add small helper types instead of scattering ad hoc counters and booleans through plain/FEC sender code.

Acceptance criteria:

- Plain and FEC sender rewrites can share one round/quorum vocabulary.
- Burst state versus feedback-open state is explicit in code before behavior rewrites start.

### T3. Add explicit migration/versioning policy

- depends_on: [T1]
- Bump `LOSSLESS_SESSION_VERSION`.
- Document Phase 1 as a flag-day protocol change.
- Ensure unsupported protocol versions fail fast.
- Add tests for version rejection.

Acceptance criteria:

- The plan no longer hand-waves migration.
- Version mismatch behavior is explicit and tested.

### T4. Replace `Eot` with `SourceDone { round_id }`

- depends_on: [T1, T2, T3]
- Add `SourceDone { round_id: u32 }`.
- Remove `Eot` from live protocol paths.
- Update encode/decode, validation, tests, sender/receiver matching, and runtime replay triggers.
- Add a sender-side solicitation interval for retransmitting `SourceDone(round_id)` while a round remains open and some quorum peers have not reported.

Acceptance criteria:

- `SourceDone` is the only burst-boundary control message.
- Lost or duplicated boundary control has a defined replay path.

### T5. Replace `PlainStatus` and `FecStatus` with `Need { round_id, ... }`

- depends_on: [T1, T2, T3]
- Add `Need { round_id, payload }`.
- Remove `PlainStatus` and `FecStatus` from live protocol paths.
- Encode empty payload as “complete.”
- Reject same-round changed payloads in Phase 1.

Acceptance criteria:

- `Need` is the only receiver-to-sender report.
- Same-round duplicate handling is deterministic.

### T6. Remove dead compatibility variants

- depends_on: [T1]
- Delete `BlockAck`.
- Delete standalone `BlockStatus`.
- Remove associated validation, decode, encode, and tests.

Acceptance criteria:

- Dead control kinds no longer exist in production code.

### T7. Define and implement active session quorum semantics

- depends_on: [T2]
- Make the active session quorum explicit.
- Freeze quorum when the first payload burst for round `0` is emitted.
- Define late `Ready` as non-participating for that session.
- Add tests for `ready_grace` expiration and final completion with non-ready configured receivers.

Acceptance criteria:

- Final completion quorum is unambiguous.
- `ready_grace` no longer creates hidden completion ambiguity.

### T8. Rewrite receiver logic around cached per-round `Need`

- depends_on: [T2, T4, T5, T7]
- Plain receiver:
  - compute one missing-range snapshot on `SourceDone(round_id)`
  - cache it
  - replay it on duplicate `SourceDone(round_id)`
- FEC receiver:
  - compute one deficit snapshot on `SourceDone(round_id)`
  - cache it
  - replay it on duplicate `SourceDone(round_id)`
- Keep accepting late symbols after `SourceDone(round_id)`, but do not mutate the cached same-round snapshot.

Acceptance criteria:

- Receiver emits exactly one distinct `Need` per round.
- Duplicate `SourceDone` produces exact replay, not recomputation drift.

### T9. Rewrite the plain sender around the shared round state machine

- depends_on: [T2, T4, T5, T7, T8]
- Remove the current all-receiver barrier implementation.
- Start retransmission after the first useful `Need(round_id)` from a quorum peer.
- Merge additional same-round peer snapshots into one block retransmit set.
- Allow burst `r + 1` emission to begin while feedback for round `r` remains open.
- If work drains and some quorum peers still have not reported:
  - keep the round open
  - retransmit `SourceDone(round_id)`
  - do not advance the round
- If the round closes:
  - complete if all reports are empty
  - otherwise emit `SourceDone(r + 1)` only after round `r` is closed and burst `r + 1` has drained

Acceptance criteria:

- Plain sender begins repair early without losing slow-peer correctness.
- Plain sender no longer opens empty follow-up rounds.
- Plain sender never has two feedback-open rounds at once.

### T10. Rewrite the FEC sender around the shared round state machine

- depends_on: [T2, T4, T5, T7, T8]
- Remove the current all-receiver report barrier implementation.
- Start repair symbol transmission after the first useful `Need(round_id)` from a quorum peer.
- Merge same-round peer snapshots by taking max per-block deficit.
- Allow burst `r + 1` emission to begin while feedback for round `r` remains open.
- Keep the round open until every quorum peer reported and merged work drained.
- Retransmit `SourceDone(round_id)` while waiting for missing peer reports.
- Scope fast-tree utilization expectations explicitly to tree-visible ingress behavior.

Acceptance criteria:

- FEC sender starts repair early.
- FEC sender still gives slow peers a correct reporting window.
- The implementation does not over-claim behavior on shared-queue ingress.
- FEC sender never opens feedback for burst `r + 1` before round `r` closes.

### T11. Update runtime replay and stale-control handling

- depends_on: [T2, T3, T4, T5, T7, T8, T9, T10]
- Rewrite completed and partially-complete receiver replay to be round-aware.
- Replay the latest cached `Need(round_id)` when duplicate payload or duplicate `SourceDone(round_id)` indicates the sender may still be on that round.
- Reject stale control for closed rounds.
- Add tests for:
  - duplicate `SourceDone`
  - late `Need(round_id)` while round still open
  - stale `Need(round_id)` after round closure
  - feedback for round `r + 1` not opening until round `r` is closed

Acceptance criteria:

- Replay is round-aware, not just completion-aware.
- Stale control handling is explicit and deterministic.

### T12. Phase 2 low-risk wire cleanup

- depends_on: [T6, T11]
- Evaluate and implement only low-risk wire cleanups:
  - remove `payload_len`
  - possibly migrate `Ready` to empty payload if transport `peer_id` is proven on all handshake paths
- Add tests proving equivalent behavior.

Acceptance criteria:

- Only low-risk cleanup lands in this phase.
- Cleanup does not change round/quorum semantics.

### T13. Phase 2 tree-contract audit and selective field cleanup

- depends_on: [T11, T12]
- Audit all current uses of tree identity across:
  - manifest validation
  - sender preflight
  - processor ingress contract
  - routing behavior
  - control-path routing for `SourceDone` and `Need`
  - receiver validation
- Only after that audit:
  - consider removing inner `BlockSymbol.tree_id`
  - decide whether manifest `tree_ids` can ever be removed
- Header simplification and inner `session_id` removal remain deferred unless the audit proves them safe and in-scope.

Acceptance criteria:

- Tree-related cleanup is driven by a real dependency audit.
- The plan no longer assumes tree identity is purely local when current code disproves that.

### T14. Validation, integration coverage, and cleanup

- depends_on: [T13]
- Add full integration coverage for:
  - slow peer reporting after fast-peer repair start
  - identical vs changed duplicate `Need(round_id)`
  - lost, duplicated, and out-of-order `SourceDone`
  - work drained while missing peer reports
  - `ready_grace` with non-ready configured receivers
  - tree-visible vs shared-queue ingress behavior
  - runtime replay of latest non-empty and final empty `Need`
- Split `messages/src/lossless_session.rs` only after behavior is stable.
- Remove obsolete cutover comments.

Acceptance criteria:

- The new protocol is behaviorally covered before further cleanup.
- Cleanup is the last step, not the first.

## Validation Matrix

### Messages crate

- `cargo test -p nextmini_messages`

Must cover:

- `SourceDone(round_id)` roundtrip
- plain `Need(round_id, ranges)` roundtrip
- FEC `Need(round_id, deficits)` roundtrip
- empty `Need(round_id)` roundtrip
- unsupported protocol version rejection
- removed dead control kinds rejected

### Dataplane crate

- `cargo test -p dataplane`

Must cover:

1. slow peer `Need(r)` arrives after fast-peer repair already started
2. duplicate `Need(r)` with identical payload is safe
3. changed same-round `Need(r)` is rejected in Phase 1
4. duplicate `SourceDone(r)` triggers exact replay of cached `Need(r)`
5. lost `SourceDone(r)` is repaired by solicitation retransmit
6. work drained while some quorum peers are still missing does not advance the round
7. non-ready peers excluded from final quorum after gate open
8. sender starts repair after first useful `Need`
9. sender completes only after all quorum peers reported empty `Need`
10. tree-visible vs shared-queue ingress expectations are both covered
11. sender may emit burst `r + 1` early, but `SourceDone(r + 1)` does not open feedback until round `r` closes
12. closing an all-empty round completes the session without opening an empty follow-up round

### Full suite

- `cargo nextest run`

## Risks

- Early repair may oversend relative to the current all-receiver barrier.
- The burst-id versus feedback-open-round distinction must be implemented explicitly or the new protocol will race.
- Quorum freeze changes session semantics and must be clearly communicated.
- Same-round `Need` immutability is a deliberate simplification; changing it later requires `report_seq`.
- Tree cleanup remains risky until the transport/scheduling contract is audited.

## Non-Goals

- No settle timer for repair start
- No attempt to infer full network drain before feedback
- No mixed-version interoperability in Phase 1
- No aggressive header simplification in Phase 1
- No claim that shared-queue ingress can achieve tree-proportional throughput

## Recommended Execution Order

1. Freeze semantics and tests.
2. Introduce shared round/quorum state.
3. Add migration/versioning.
4. Add `SourceDone`.
5. Add `Need`.
6. Remove dead control kinds.
7. Implement quorum rules.
8. Rewrite receiver behavior.
9. Rewrite plain sender.
10. Rewrite FEC sender.
11. Rewrite runtime replay.
12. Do only low-risk wire cleanup.
13. Audit tree-contract dependencies before any further field removal.
14. Finish with integration coverage and cleanup.
