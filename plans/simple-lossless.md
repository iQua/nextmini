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

### Canonical `Need` encoding

Phase 1 defines same-round equality on canonical wire form.

- plain `Need` ranges must be sorted, coalesced, and non-overlapping
- FEC `Need` deficits must be sorted by block id, contain at most one entry per block, and omit zero deficits

Receivers must canonicalize before encoding.

Same-round immutability is then checked byte-for-byte on the canonical encoding.

Decoders must reject noncanonical `Need` bodies.

That prevents semantic equality from depending on encoder quirks.

### FEC deficit semantics

In Phase 1, FEC `Need` is a heuristic snapshot of residual demand.

It means:

- “at least this much more repair may still be useful right now”
- not “this is the minimal repair set required to decode”
- not “this is a perfect decode certificate”

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

Protocol action:

- changed same-round `Need` from a quorum peer is a session-fatal protocol violation
- changed same-round `Need` from a non-quorum peer is dropped and counted

If the project later wants same-round mutable updates, add an explicit `report_seq`.
That is not part of Phase 1.

### Sender work accounting for the open round

For the feedback-open round `r`, the sender keeps mode-local accounting instead of a separate shared frontier framework.

- in plain mode: track the merged missing block set/ranges implied by same-round `Need(r)` snapshots and which retransmits for burst `r + 1` have already been admitted locally
- in FEC mode: track the merged max-per-block deficit map implied by same-round `Need(r)` snapshots and how many repair symbols for burst `r + 1` have already been admitted locally

Rules:

- required same-round work may only grow while round `r` remains open
- locally admitted work may only grow as retransmits or repair symbols are accepted by local processor ingress
- `work locally exhausted` means the sender has admitted enough work to cover the current merged round requirement and has no additional local frames buffered or waiting to be admitted for that requirement
- if a late useful `Need(r)` arrives after burst `r + 1` appeared locally exhausted, the sender extends the current round work in place, resumes burst `r + 1` emission, and keeps `SourceDone(r + 1)` pending

This is intentionally a sender-local, mode-local accounting concept.

Phase 1 does **not** attempt to infer downstream scheduler drain or full network drain before advancing rounds.

### Round closure

A round does **not** close just because repair work becomes locally exhausted.

A round closes only when:

1. every peer in the active session quorum has reported for that round
2. the sender's merged round work is locally exhausted
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

### Round-scoped control acceptance

For sender-side control handling:

- `Need(round_id)` for the current feedback-open round is accepted
- `Need(round_id)` for a closed round is stale and dropped
- `Need(round_id)` for a future round is invalid and dropped
- changed same-round `Need(round_id)` from a quorum peer aborts the session
- changed same-round `Need(round_id)` from a non-quorum peer is dropped

Those drops must be logged and counted.

## Missing-Peer Handling

This rewrite does **not** use a settle timer for repair start.

But it also does **not** allow missing-peer reports to be ignored.

If merged work becomes locally exhausted while some quorum peers have not yet reported for the open round:

- the sender keeps the same round open
- the sender retransmits `SourceDone(round_id)` on a solicitation interval
- the sender does **not** advance the round

That rule handles:

- lost `SourceDone`
- delayed control delivery
- replay triggers for duplicate `SourceDone`
- missing peer reports without opening spurious empty rounds

This plan intentionally chooses same-round solicitation/replay instead of a settle timer or completion-only grace window.

### Frozen quorum liveness policy

Phase 1 does **not** allow a frozen quorum peer to stall a session forever.

Phase 1 also does **not** support mid-session quorum shrink or peer eviction.

The failure policy is:

- sender retransmits `SourceDone(round_id)` at a fixed `source_done_solicitation_interval`
- sender tracks a per-peer solicitation count for the open round
- sender tracks a hard `peer_report_timeout` from the first `SourceDone(round_id)` for that round
- if a quorum peer remains silent past the timeout, the sender aborts the session with `PeerReportTimeout { peer_id, round_id }`

Timeout calibration rule:

- `peer_report_timeout` is a control-path timeout, not a payload-throughput timeout
- it must exceed the configured control-path RTT budget
- it must also exceed several `source_done_solicitation_interval` periods so healthy but slow control delivery does not false-abort

This keeps Phase 1 simple:

- no infinite wait
- no silent peer eviction
- one explicit abort reason for operator handling

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

The quorum freezes when the sender emits:

- the first payload burst for round `0`, or
- `SourceDone(0)` for a zero-byte object session

Late `Ready` after that point does not join the quorum for that session.

### Phase 1 identity normalization

While `Ready.node_id` remains on the wire:

- `Ready.node_id` must equal the transport-derived `peer_id`
- quorum membership is stored keyed only by transport `peer_id`
- `Ready` with an identity mismatch is rejected for quorum purposes
- `Ready` or `Need` with no transport `peer_id` is invalid and dropped

Identity mismatches and missing transport identity must be logged and counted.

### Final completion

Only quorum peers count toward final completion.

This avoids a protocol deadlock where `ready_grace` opens the data gate but non-ready peers still count toward the final completion quorum.

### Non-quorum control handling

After freeze:

- `Need` from non-quorum peers is ignored
- late `Ready` from non-quorum configured peers is ignored for the current session
- those drops must be logged and counted

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

Phase 1 rollout work also includes:

- updating harnesses and fixtures that generate or parse old control kinds
- adding a header-peek or decode-error path so version mismatch logs can include local version, observed remote version, and session id before the frame is dropped
- documenting that in-flight old-version frames after cutover will be dropped

## Observability Requirements

Phase 1 is not acceptable without logs or metrics for:

- open feedback round id
- current emitted burst id
- first useful `Need` arrival
- late or stale `Need` dropped
- same-round changed `Need` rejected
- solicitation count per peer
- quorum membership and freeze point
- non-quorum control dropped
- `Ready.node_id` versus transport `peer_id` mismatch
- missing transport `peer_id` on inbound control
- changed same-round `Need` protocol violations
- completion reason versus abort reason
- version mismatch drops
- control-path latency for `SourceDone` and `Need`
- receiver transition into `object_complete`, `passive_complete`, and `session_finished`
- passive-complete local GC reason

## Abort Reason Exposure

Phase 1 keeps the current coarse public outcome surface.

- `PeerReportTimeout` is an internal abort reason
- it must be exposed through logs and metrics
- expanding public session outcome enums is deferred

## Sender Algorithm

### Shared sender rules

For both plain and FEC:

1. open round `0`
2. send initial burst
3. emit `SourceDone(0)`
4. keep round `0` feedback-open
5. if the active quorum is empty at `SourceDone(0)`, complete immediately
6. begin emitting burst `1` as soon as the first useful `Need(0)` arrives
7. continue merging additional `Need(0)` snapshots from other quorum peers while round `0` remains open
8. if burst `1` appears locally exhausted and a later useful `Need(0)` extends the merged round work, resume burst `1` emission in place and keep `SourceDone(1)` pending
9. if burst `1` is locally exhausted but not all quorum peers reported for round `0`, retransmit `SourceDone(0)` and keep waiting
10. if a quorum peer stays silent past `peer_report_timeout`, abort the session
11. once round `0` closes:
   - complete immediately if all reports were empty
   - otherwise, if burst `1` is already locally exhausted, emit `SourceDone(1)` immediately
   - otherwise wait until burst `1` is locally exhausted, then emit `SourceDone(1)`
12. repeat the same pattern for round `r` and burst `r + 1`

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
- late useful same-round reports may materially increase repair oversend and can force extra rounds relative to the current barriered design
- sender uses work-conserving tree scheduling where transport contract allows it

### Throughput claim scope

This plan only claims fast-tree utilization improvements on:

- `TreeVisibleNonBlocking` ingress paths

On shared-queue paths:

- protocol latency may improve
- but tree-proportional throughput is not guaranteed

Regardless of ingress mode:

- repair latency is still gated by control-path delivery of `SourceDone` and `Need`
- timeout behavior must therefore be calibrated to control latency, not payload throughput alone

## Receiver Algorithm

### Shared receiver rules

For both plain and FEC:

1. install manifest
2. send `Ready`
3. on `SourceDone(round_id)`, compute one full residual snapshot
4. send `Need(round_id, snapshot)` immediately
5. cache the canonical encoded `Need(round_id)` for replay
6. if duplicate `SourceDone(round_id)` arrives, replay the exact cached `Need(round_id)`

Late-arriving source data after `SourceDone(round_id)` is allowed.

It does not mutate the already-issued `Need(round_id)`.

It only affects what the receiver reports in the next round.

Receiver-side stale-boundary rule:

- once a receiver has already handled `SourceDone(k)`, any later-arriving `SourceDone(j)` with `j < k` is stale and must be dropped
- the receiver must not answer a stale older `SourceDone` with its latest cached empty `Need`

## Receiver Lifetime After Local Completion

Local completion is monotonic once the receiver has fully reconstructed the object.

Phase 1 splits receiver lifecycle into:

1. `object_complete`
2. `passive_complete`
3. `session_finished`

`object_complete` means the object is fully reconstructed locally.

It does **not** mean the task may exit.

Instead it enters a passive-complete state:

- the receiver remains live for the session
- on every later `SourceDone(round_id)`, it emits a canonical empty `Need(round_id)`
- it updates its replay cache to that later round id
- it remains authoritative for future-round empty replies until local session termination

Phase 1 does **not** let runtime synthesize empty `Need` for future round ids after the live receiver is gone.

Phase 1 also does **not** add a new sender terminal control.

So Phase 1 needs an explicit local finish rule.

The receiver enters `session_finished` only when one of these happens:

- the local session aborts
- the local runtime tears the session down explicitly
- a passive-complete local-GC timeout expires after the most recent answered `SourceDone(round_id)` and no later payload or `SourceDone` arrives

Local GC rule:

- `session_finish_timeout` must exceed `peer_report_timeout`
- it must also exceed the control-path RTT budget
- object reconstruction alone is never sufficient for task exit

This makes receiver termination explicit without adding a new terminal wire control in Phase 1.

### No settle timer

There is no settle timer for repair start.

Rationale:

- fast trees should not be held idle waiting for a timer
- speculative repair is acceptable
- correctness is preserved by full-snapshot `Need`, same-round replay, and explicit round closure rules
- oversend is an accepted latency tradeoff in Phase 1, especially in FEC mode
- in FEC mode, that oversend can be material because deficits are heuristic snapshots and same-round immutable

## Replay Ownership And Handoff

Replay state must have exactly one owner at any instant.

Ownership model:

- while the receiver task is live, it owns the cached canonical `Need(round_id)` for the open round
- before the receiver task exits or is torn down, it transfers that cached replay state to runtime handoff storage
- after handoff, runtime owns replay for that receiver
- completed receiver replay is a specialized form of runtime-owned replay, not a separate semantic model

No-gap handoff rule:

- duplicates arriving during teardown must observe either live receiver-owned replay state or runtime-owned replay state
- there must never be an interval where neither layer can answer replay for the latest cached round
- while a live receiver task exists, inbound duplicates must be delivered to that live task; runtime-owned replay may answer only after ownership flips and the live session entry is no longer authoritative

## Runtime Replay

Replay must become round-aware.

The current runtime only replays completed receiver status after duplicate payload or `Eot`.

Phase 1 replay must support:

- latest cached non-empty `Need(round_id)`
- latest cached empty `Need(round_id)`
- duplicate `SourceDone(round_id)`

Replay must never synthesize a changed same-round `Need`.

Replay must never emit feedback for a closed round.

Replay must preserve the single-feedback-open-round rule even if later bursts have already been partially emitted.

Duplicate `SourceDone(round_id)` is the only authoritative replay or solicitation trigger in Phase 1.

Phase 1 does **not** use duplicate payload as an authoritative trigger for synthesized round-aware replay.

## Phase 2 Cleanup Rules

Phase 2 cleanup happens only after Phase 1 semantics are proven in tests.

### Phase 2a mini-project: `payload_len` removal

- separate task
- only after the full behavioral matrix is green

### Phase 2b mini-project: empty `Ready`

- separate task
- only after transport-derived `peer_id` is proven on every handshake path
- only after the full behavioral matrix is green

### Explicitly not low-risk in Phase 2

- `BlockSymbol.tree_id` removal
- manifest `tree_ids` removal
- inner `session_id` removal
- aggressive inner header simplification

`BlockSymbol.tree_id` removal is a transport/routing mini-project, not a trivial cleanup.

Manifest `tree_ids` removal is explicitly out of scope until a transport/routing contract redesign proves it safe.

`BlockSymbol.tree_id` is also preserved by default in this plan unless a transport/routing redesign proves outer-tree identity is available everywhere validation and scheduling need it.

## Invariants

These invariants must be encoded in tests before substantial implementation work:

1. `Need(round_id)` is a full residual snapshot, not a delta.
2. Only one feedback-open round exists at a time.
3. A receiver emits at most one distinct `Need(round_id)` per round.
4. Duplicate `SourceDone(round_id)` causes replay of the same cached `Need(round_id)`.
5. Same-round changed `Need` payloads are rejected in Phase 1.
6. Sender may start repair after the first useful `Need(round_id)`.
7. Sender may not close a round until every quorum peer reported.
8. Sender may not advance the round just because merged work becomes locally exhausted.
9. Sender retransmits `SourceDone(round_id)` while waiting for missing peer reports.
10. Sender completes only after all quorum peers report empty `Need` for the closed round.
11. Quorum freezes when the first payload burst is emitted.
12. Only quorum peers count toward final completion.
13. Tree identity is transport/scheduling metadata, not decoder correctness metadata.
14. Fast-tree utilization claims are limited to tree-visible ingress paths.
15. The sender may emit burst `r + 1` before round `r` closes, but may not emit `SourceDone(r + 1)` until round `r` is closed.
16. If round `r` closes with all-empty `Need(r)`, the session completes without opening `r + 1`.
17. A silent frozen quorum peer cannot stall the session forever; the session aborts on `PeerReportTimeout`.
18. Sender-local required work for round `r` is monotonic while round `r` is open.
19. Sender-local admitted work for round `r` is monotonic and `SourceDone(r + 1)` is forbidden until round `r` is locally exhausted.
20. Late useful `Need(r)` extends the current round work in place; it does not open a second feedback round.
21. Same-round `Need` equality is byte-for-byte on canonical encoding.
22. Non-quorum control is dropped and observed.
23. Zero-byte object sessions freeze quorum on `SourceDone(0)` and complete through the same all-empty round rules.
24. `Ready.node_id` must match transport `peer_id` in Phase 1 while `Ready.node_id` remains on the wire.
25. Once a receiver reaches monotonic local completion, it must still answer every later `SourceDone(k)` with empty `Need(k)` until local session termination.
26. Duplicate `SourceDone(round_id)` is the only authoritative replay trigger in Phase 1.
27. `Need(round_id)` for a future round is invalid and dropped deterministically.
28. If the active quorum is empty at `SourceDone(0)`, the sender completes immediately without waiting for `Need`.
29. Changed same-round `Need` from a quorum peer is a session-fatal protocol violation.
30. Noncanonical `Need` bodies are rejected at decode time.
31. `Ready` or `Need` with no transport `peer_id` is invalid and dropped.
32. `object_complete` does not imply receiver exit; only `session_finished` may exit.
33. Passive-complete receivers may exit only after local abort, explicit teardown, or `session_finish_timeout`.
34. Receiver-side stale `SourceDone(j < k)` is dropped.

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
T2 -> T11
T3 -> T4
T3 -> T5
T3 -> T12
T3 -> T13
T4 -> T8
T4 -> T9
T4 -> T10
T4 -> T12
T4 -> T13
T5 -> T8
T5 -> T9
T5 -> T10
T5 -> T12
T5 -> T13
T6 -> T13
T7 -> T8
T7 -> T9
T7 -> T10
T7 -> T11
T7 -> T12
T7 -> T13
T8 -> T9
T8 -> T10
T8 -> T11
T8 -> T12
T8 -> T13
T11 -> T9
T11 -> T10
T9 -> T13
T9 -> T14
T10 -> T13
T10 -> T14
T11 -> T12
T11 -> T13
T11 -> T14
T12 -> T13
T12 -> T14
T13 -> T14
T14 -> T15
T15 -> T16
T16 -> T17
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

### T2. Introduce explicit shared quorum and liveness state types

- depends_on: [T1]
- Add shared sender-side types for:
  - quorum membership
  - quorum freeze
  - missing-peer solicitation timing
  - peer-report timeout tracking
- Keep round-local work accounting inside the plain and FEC sender implementations instead of introducing a second shared frontier model.
- Add small helper types where the state is truly shared instead of scattering ad hoc counters and booleans through sender code.

Acceptance criteria:

- Plain and FEC sender rewrites share one quorum and liveness vocabulary.
- The implementation does not depend on a second unused sender-state abstraction.
- The plan does not depend on unobservable network-drain signals.

### T3. Add explicit migration/versioning policy

- depends_on: [T1]
- Bump `LOSSLESS_SESSION_VERSION`.
- Document Phase 1 as a flag-day protocol change.
- Ensure unsupported protocol versions fail fast.
- Add a header-peek or lightweight decode-error path so unsupported versions can be logged before the frame is dropped.
- Update harnesses, fixtures, and logs for the hard wire break.
- Add tests for version rejection.

Acceptance criteria:

- The plan no longer hand-waves migration.
- Version mismatch behavior is explicit and tested.
- Version mismatch is obvious in production logs.
- Mixed-version failures do not degrade into silent `ready_grace` or timeout symptoms without a diagnostic.

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
- Canonicalize `Need` payloads before encode.
- Reject noncanonical `Need` bodies at decode time.
- Reject malformed `Need` bodies that do not match manifest mode.
- Reject same-round changed payloads in Phase 1.

Acceptance criteria:

- `Need` is the only receiver-to-sender report.
- Same-round duplicate handling is deterministic.
- Same-round equality is byte-for-byte on canonical encoding.
- Canonicality is enforced on both encode and decode.

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
- Freeze quorum when the first payload burst for round `0` is emitted, or when `SourceDone(0)` is emitted for a zero-byte object.
- Require `Ready.node_id == transport peer_id` while `Ready.node_id` remains on the wire.
- Store quorum membership keyed only by transport `peer_id`.
- Treat `Ready` or `Need` with no transport `peer_id` as invalid and drop them with logs and metrics.
- Define late `Ready` as non-participating for that session.
- Define fixed-interval solicitation and `peer_report_timeout` abort behavior for silent frozen peers.
- Define `Need` from non-quorum peers as ignored with logging and metrics.
- Add tests for `ready_grace` expiration, final completion with non-ready configured receivers, silent frozen-peer timeout, `Ready.node_id` mismatch, and missing transport `peer_id`.

Acceptance criteria:

- Final completion quorum is unambiguous.
- `ready_grace` no longer creates hidden completion ambiguity.
- Frozen quorum peers have a defined failure path instead of indefinite wait.
- Phase 1 does not allow split identity between readiness and later round reporting.

### T8. Rewrite receiver logic around cached per-round `Need`

- depends_on: [T2, T4, T5, T7]
- Plain receiver:
  - compute one missing-range snapshot on `SourceDone(round_id)`
  - canonicalize and cache it
  - replay it on duplicate `SourceDone(round_id)`
- FEC receiver:
  - compute one deficit snapshot on `SourceDone(round_id)`
  - canonicalize and cache it
  - replay it on duplicate `SourceDone(round_id)`
- Keep accepting late symbols after `SourceDone(round_id)`, but do not mutate the cached same-round snapshot.
- Drop stale older `SourceDone(j < k)` once a later round has already been handled.
- Define zero-byte object receiver behavior as immediate empty `Need(0)` after `SourceDone(0)`.

Acceptance criteria:

- Receiver emits exactly one distinct `Need` per round.
- Duplicate `SourceDone` produces exact replay, not recomputation drift.
- Duplicate `SourceDone` after additional late data still replays the original cached snapshot.
- Receiver-side stale `SourceDone` is dropped, not answered with a newer cached `Need`.

### T11. Define passive-complete receiver lifetime and future-round behavior

- depends_on: [T2, T4, T5, T7, T8]
- Split receiver lifecycle into `object_complete`, `passive_complete`, and `session_finished`.
- Define the passive-complete receiver state once local completion becomes monotonic.
- Require the live receiver to remain available and emit canonical empty `Need(round_id)` on every later `SourceDone(round_id)`.
- Keep future-round empty replies owned by the live receiver in Phase 1; do not synthesize them in runtime after receiver exit.
- Define zero-byte object behavior and the empty-active-quorum sender fast path explicitly.
- Define `session_finish_timeout` and the local GC rule for passive-complete receivers.
- Add tests for later-round empty replies from locally complete receivers and for passive-complete local GC.

Acceptance criteria:

- Local receiver completion does not create a future-round protocol hole.
- Passive-complete receivers can satisfy later quorum rounds without inventing new wire messages.
- Receiver termination is explicit rather than hand-waved.

### T9. Rewrite the plain sender around the revised round rules

- depends_on: [T2, T4, T5, T7, T8, T11]
- Remove the current all-receiver barrier implementation.
- Start retransmission after the first useful `Need(round_id)` from a quorum peer.
- Merge additional same-round peer snapshots into one block retransmit set.
- Allow burst `r + 1` emission to begin while feedback for round `r` remains open.
- If a late useful `Need(round_id)` arrives after burst `r + 1` appeared locally exhausted, extend the merged plain retransmit set and resume burst `r + 1` emission in place.
- If work is locally exhausted and some quorum peers still have not reported:
  - keep the round open
  - retransmit `SourceDone(round_id)`
  - do not advance the round
- Abort on `PeerReportTimeout` instead of waiting forever for a silent frozen peer.
- If the round closes:
  - complete if all reports are empty
  - otherwise emit `SourceDone(r + 1)` only after round `r` is closed and burst `r + 1` is locally exhausted

Acceptance criteria:

- Plain sender begins repair early without losing slow-peer correctness.
- Plain sender no longer opens empty follow-up rounds.
- Plain sender never has two feedback-open rounds at once.
- Plain sender accounts for late useful same-round reports without reopening feedback.

### T10. Rewrite the FEC sender around the revised round rules

- depends_on: [T2, T4, T5, T7, T8, T11]
- Remove the current all-receiver report barrier implementation.
- Start repair symbol transmission after the first useful `Need(round_id)` from a quorum peer.
- Merge same-round peer snapshots by taking max per-block deficit.
- Allow burst `r + 1` emission to begin while feedback for round `r` remains open.
- If a late useful `Need(round_id)` arrives after burst `r + 1` appeared locally exhausted, extend the merged FEC deficit map and resume burst `r + 1` emission in place.
- Keep the round open until every quorum peer reported and merged work is locally exhausted.
- Retransmit `SourceDone(round_id)` while waiting for missing peer reports.
- Abort on `PeerReportTimeout` instead of waiting forever for a silent frozen peer.
- Scope fast-tree utilization expectations explicitly to tree-visible ingress behavior.

Acceptance criteria:

- FEC sender starts repair early.
- FEC sender still gives slow peers a correct reporting window.
- The implementation does not over-claim behavior on shared-queue ingress.
- FEC sender never opens feedback for burst `r + 1` before round `r` closes.
- FEC sender treats oversend and extra rounds as explicit latency tradeoffs, not accidental side effects.

### T12. Define replay ownership and no-gap handoff

- depends_on: [T2, T7, T8, T11]
- Specify whether replay state is owned by the live receiver task, runtime handoff state, or completed replay state at each lifecycle point.
- Implement an explicit no-gap handoff from receiver-owned replay to runtime-owned replay during teardown and completion.
- Define delivery precedence so the live receiver remains authoritative until handoff completes.
- Rewrite the existing runtime handoff tests that currently prefer completed replay over live delivery so they go red under the new semantics first.
- Add tests for receiver/runtime replay handoff races and for preventing handoff while future rounds may still require live passive-complete replies.

Acceptance criteria:

- Replay ownership is explicit at every lifecycle point.
- Duplicates during teardown cannot fall into a replay hole.
- Live delivery and replay cannot both answer the same duplicate.
- Runtime handoff does not preempt a passive-complete receiver that may still need to answer later rounds.

### T13. Update runtime replay and stale-control handling

- depends_on: [T3, T4, T5, T7, T8, T9, T10, T11, T12]
- Rewrite completed and partially-complete receiver replay to be round-aware.
- Replay the latest cached `Need(round_id)` only on duplicate `SourceDone(round_id)` for the relevant round.
- Reject stale control for closed rounds.
- Drop future-round `Need(round_id)` deterministically.
- Treat changed same-round `Need` from a quorum peer as a session-fatal protocol violation.
- Add tests for:
  - duplicate `SourceDone`
  - late `Need(round_id)` while round still open
  - stale `Need(round_id)` after round closure
  - changed same-round `Need` from a quorum peer aborts the session
  - feedback for round `r + 1` not opening until round `r` is closed
  - non-quorum peer `Need` after freeze
  - duplicate payload after burst `r + 1` has begun does not trigger wrong-round replay
  - future-round `Need` dropped deterministically

Acceptance criteria:

- Replay is round-aware, not just completion-aware.
- Stale control handling is explicit and deterministic.
- Phase 1 does not use duplicate payload as an authoritative replay trigger.

### T14. Behavioral integration coverage and observability

- depends_on: [T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]
- Add the full behavioral integration matrix before any cleanup work.
- Implement the required logs and metrics for round ids, burst ids, solicitation, quorum membership, stale drops, abort reason, version mismatch, passive-complete state, and `Ready` identity mismatch.
- Make the quorum-freeze behavior change operator-visible in logs and metrics.
- Add explicit performance-oriented coverage for control-path latency asymmetry with tree-visible data paths and for slow-control/no-false-timeout behavior.

Acceptance criteria:

- The behavioral matrix is green before cleanup begins.
- Operators can observe round progress, drops, and aborts.

### T15. Phase 2a `payload_len` removal mini-project

- depends_on: [T14]
- Remove `payload_len` only.
- Add tests proving no semantic change.

Acceptance criteria:

- `payload_len` cleanup is isolated from every other wire change.
- No behavioral semantics change.

### T16. Phase 2b empty `Ready` mini-project

- depends_on: [T15]
- Prove transport-derived `peer_id` exists and is stable on every handshake/control path.
- Only then migrate `Ready` to an empty payload.
- Add tests and logs around late/non-quorum readiness.

Acceptance criteria:

- Empty `Ready` is backed by transport identity proof, not assumption.
- Handshake behavior remains observable.

### T17. Phase 2c tree-contract audit only

- depends_on: [T16]
- Audit all current uses of tree identity across:
  - manifest validation
  - sender preflight
  - processor ingress contract
  - routing behavior
  - control-path routing for `SourceDone` and `Need`
  - receiver validation
- Only after that audit:
- decide whether a later transport/routing redesign could ever remove `BlockSymbol.tree_id`
- Keep manifest `tree_ids` removal explicitly out of scope unless a transport/routing redesign proves it safe.
- Keep `BlockSymbol.tree_id` in place in this plan.
- Header simplification and inner `session_id` removal remain deferred unless a separate audit proves them safe and in-scope.

Acceptance criteria:

- Tree-related cleanup is driven by a real dependency audit.
- The plan no longer assumes tree identity is purely local when current code disproves that.
- Manifest `tree_ids` removal remains out of scope unless the transport contract changes.
- `BlockSymbol.tree_id` is preserved unless a later redesign proves otherwise.

## Validation Matrix

### Messages crate

- `cargo test -p nextmini_messages`

Must cover:

- `SourceDone(round_id)` roundtrip
- plain `Need(round_id, ranges)` roundtrip
- FEC `Need(round_id, deficits)` roundtrip
- empty `Need(round_id)` roundtrip
- canonical `Need` equality encoding
- noncanonical `Need` rejected
- malformed `Need` body rejected for the wrong manifest mode
- unsupported protocol version rejection
- removed dead control kinds rejected

### Dataplane crate

- `cargo test -p dataplane`

Must cover:

1. slow peer `Need(r)` arrives after fast-peer repair already started
2. duplicate `Need(r)` with identical payload is safe
3. changed same-round `Need(r)` from a quorum peer aborts the session
4. duplicate `SourceDone(r)` triggers exact replay of cached `Need(r)`
5. lost `SourceDone(r)` is repaired by solicitation retransmit
6. work is locally exhausted while some quorum peers are still missing does not advance the round
7. non-ready peers excluded from final quorum after gate open
8. sender starts repair after first useful `Need`
9. sender completes only after all quorum peers reported empty `Need`
10. tree-visible vs shared-queue ingress expectations are both covered
11. sender may emit burst `r + 1` early, but `SourceDone(r + 1)` does not open feedback until round `r` closes
12. closing an all-empty round completes the session without opening an empty follow-up round
13. silent frozen quorum peer triggers repeated solicitation and then `PeerReportTimeout`
14. late useful `Need(r)` after apparent burst `r + 1` local exhaustion extends the current work frontier in place
15. duplicate `SourceDone(r)` after more data arrived replays cached `Need(r)`, not a recomputed snapshot
16. non-quorum peer `Need` after freeze is dropped and logged
17. receiver/runtime replay handoff race does not create a replay hole
18. control-path latency asymmetry with tree-visible data paths is covered explicitly
19. zero-byte object session freezes quorum on `SourceDone(0)` and completes correctly
20. locally complete receiver answers later `SourceDone(r + 1)` with empty `Need(r + 1)`
21. future-round `Need` is dropped deterministically
22. duplicate payload after burst `r + 1` begins does not trigger wrong-round replay
23. slow control path with fast data path does not false-timeout when timeout is properly budgeted
24. empty active quorum at `SourceDone(0)` completes immediately
25. runtime handoff does not preempt a passive-complete receiver while future rounds are still possible
26. `Ready.node_id` and transport `peer_id` mismatch is dropped and logged
27. `Ready` or `Need` with no transport `peer_id` is dropped and logged
28. stale older `SourceDone(j < k)` is dropped on the receiver side
29. passive-complete receiver enters `session_finished` only after `session_finish_timeout` or explicit teardown
30. version mismatch logs remote version and session id before drop

### Full suite

- `cargo nextest run`

## Risks

- Early repair may oversend relative to the current all-receiver barrier.
- In FEC mode that oversend can be material, because deficits are heuristic snapshots and same-round immutable.
- Early immutable non-empty `Need` can also force extra follow-up rounds that later in-flight source data would have avoided.
- The burst-id versus feedback-open-round distinction must be implemented explicitly or the new protocol will race.
- Quorum freeze changes session semantics and must be clearly communicated.
- Silent frozen-peer handling now fails by abort rather than indefinite wait; operators must understand that behavior.
- Passive-complete receivers may stay alive longer because Phase 1 does not add a terminal sender control.
- Same-round `Need` immutability is a deliberate simplification; changing it later requires `report_seq`.
- Control-path latency may still dominate end-to-end repair latency even when payload trees are fast.
- Tree cleanup remains risky until the transport/scheduling contract is audited.

## Non-Goals

- No settle timer for repair start
- No attempt to infer full network drain before feedback
- No mid-session quorum shrink or peer eviction in Phase 1
- No sender terminal control in Phase 1
- No runtime synthesis of future-round empty `Need` after the live receiver has exited
- No public API expansion for abort reasons in Phase 1
- No mixed-version interoperability in Phase 1
- No aggressive header simplification in Phase 1
- No claim that shared-queue ingress can achieve tree-proportional throughput

## Recommended Execution Order

1. Freeze semantics and tests.
2. Introduce shared quorum/liveness state.
3. Add migration/versioning.
4. Add `SourceDone`.
5. Add `Need`.
6. Remove dead control kinds.
7. Implement quorum rules and liveness failure policy.
8. Rewrite receiver behavior.
9. Define passive-complete receiver lifetime.
10. Rewrite plain sender.
11. Rewrite FEC sender.
12. Define replay ownership and handoff.
13. Rewrite runtime replay.
14. Make the behavioral matrix and observability green.
15. Do `payload_len` cleanup only.
16. Do empty `Ready` only if transport identity proof is complete.
17. Audit tree-contract dependencies before any tree-field cleanup.
