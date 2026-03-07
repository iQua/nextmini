# Plan: Implement The New Block-First Lossless Session Subsystem

**Generated**: March 7, 2026  
**Target Repo**: `/Users/bli/Playground/nextmini`  
**Primary Design Reference**: `plans/fec-lossless-design.md`

## Objective

Replace the current lossless session subsystem with a new implementation that
matches the block-first design:

- `block` is the only canonical transfer unit
- plain mode is the default and uses a single tree
- FEC mode is optional and sends `BlockSymbol` within a block
- acknowledgements are per-block, not cumulative
- there is no sliding window
- there are no sender-side ACK timeouts
- there is no `BlockRepair` frame
- tree backpressure is observed at processor ingress, not through sender-local
  tree lanes

This is a rewrite, not an incremental cleanup of the old protocol model.

## Success Criteria

1. No public session boundary uses `chunk` terminology anymore; use `block`
   throughout runtime, wire protocol, config, and user-facing APIs.
2. Plain mode sends `BlockData` only and completes via `BlockAck`.
3. FEC mode sends `BlockSymbol` only, is source-first, and emits extra fountain
   symbols only in response to `BlockStatus`.
4. Control semantics are block-based in both modes.
5. The implementation has no cumulative ACK logic, no protocol sliding window,
   no sender ACK timeout mechanism, and no tree-lane abstraction.
6. Production session code under `dataplane/src/node/session` is reduced to a
   small modular design with a target budget below `2000` LOC excluding tests.
7. Existing callers are migrated to the new config/runtime surface and old
   session code paths are removed.
8. Regression coverage is rewritten around block semantics and passes for plain
   and FEC modes.

## Scope

In scope:

- `messages/src/lossless_session.rs`
- `dataplane/src/node/session/*`
- `dataplane/src/node/processor.rs`
- `dataplane/src/node/config.rs`
- `dataplane/src/node/session/unicast.rs` or its replacement location
- Python API/session-facing bindings if they expose session terminology or knobs
- lossless integration tests and docs

Out of scope:

- long-term dual-stack support for the old and new wire protocols
- preserving wire compatibility with mixed old/new peers
- retaining sender-local tree lanes as an implementation option
- preserving current chunk-oriented behavior behind compatibility shims

## Recommended Compatibility Policy

Use a single cutover policy for the rewritten protocol:

- no mixed-version peer support
- no attempt to speak both the old chunk protocol and the new block protocol in
  production
- keep the old code only long enough to land the new implementation and switch
  all internal callers
- delete the legacy code immediately after the new path is validated

This keeps the rewrite coherent and avoids carrying two incompatible transport
models in the same runtime.

## Recommended Target Module Layout

The new subsystem should be structured around small modules with clear
boundaries:

- `messages/src/lossless_session.rs`
  - new block-first wire types and codecs
- `dataplane/src/node/session/mod.rs`
  - narrow exports only
- `dataplane/src/node/session/plan.rs`
  - block geometry and offsets
- `dataplane/src/node/session/ledger.rs`
  - per-block shared sender/receiver state
- `dataplane/src/node/session/runtime.rs`
  - session registry, typed frame dispatch, lifecycle shell
- `dataplane/src/node/session/control.rs`
  - control-frame send/receive helpers only
- `dataplane/src/node/session/sender/plain.rs`
  - plain-mode block emission
- `dataplane/src/node/session/sender/fec.rs`
  - FEC symbol generation, source-first scheduling, tree selection
- `dataplane/src/node/session/receiver/plain.rs`
  - plain-mode block receive/ack
- `dataplane/src/node/session/receiver/fec.rs`
  - FEC symbol collection/decode/status/ack

Recommended removals or moves:

- remove `dataplane/src/node/session/api.rs`
- move orchestration out of `dataplane/src/node/session/unicast.rs` if it still
  mixes flow-management policy with transport implementation
- delete the current monolithic `sender.rs` and `receiver.rs`

## Config And API Direction

The rewrite should align the config and public surface with the design:

- rename `default_chunk_size` to `default_block_size`
- remove `fec_tree_lane_depth`
- remove `fec_dispatch_burst`
- remove any public cumulative-ACK or window-related knobs
- remove explicit public `symbol_size` knobs unless the codec truly requires one
  that cannot be derived from `block_size` and `symbols_per_block`
- prefer `block_size` and `symbols_per_block` as the primary transport geometry
  knobs
- keep plain mode as the default

If a Python or higher-level API still exposes `chunk_size`, it should be renamed
to `block_size` during the migration.

## Dependency Graph

- `T1 -> T2`
- `T1 -> T3`
- `T1 -> T4`
- `T1 -> T5`
- `T2 -> T6`
- `T3 -> T6`
- `T4 -> T6`
- `T5 -> T9`
- `T6 -> T7`
- `T6 -> T8`
- `T4 -> T8`
- `T6 -> T10`
- `T9 -> T10`
- `T8 -> T11`
- `T10 -> T11`
- `T7 -> T12`
- `T10 -> T12`
- `T11 -> T12`
- `T7 -> T13`
- `T8 -> T13`
- `T11 -> T13`
- `T12 -> T14`
- `T13 -> T14`
- `T14 -> T15`
- `T12 -> T15`
- `T15 -> T16`

## Task Matrix

| Task ID | Summary | depends_on |
|---|---|---|
| `T1` | Freeze protocol contract, naming, and cutover policy | `[]` |
| `T2` | Define the new config and public API surface | `[T1]` |
| `T3` | Rework the wire protocol in `messages/` | `[T1]` |
| `T4` | Build shared block geometry and shared ledgers | `[T1]` |
| `T5` | Add tree-visible non-blocking processor ingress contract | `[T1]` |
| `T6` | Rebuild the session runtime around typed block-first frames | `[T2, T3, T4]` |
| `T7` | Implement plain-mode sender/receiver end to end | `[T6]` |
| `T8` | Implement FEC codec adapter and symbol model | `[T4, T6]` |
| `T9` | Implement FEC tree scheduling and backpressure behavior | `[T5]` |
| `T10` | Implement FEC sender/receiver end to end | `[T6, T9]` |
| `T11` | Harden control semantics and duplicate/idempotent behavior | `[T8, T10]` |
| `T12` | Migrate runtime callers and public entrypoints | `[T7, T10, T11]` |
| `T13` | Delete legacy session code and enforce module/LOC budget | `[T7, T8, T11]` |
| `T14` | Rewrite and consolidate tests around block semantics | `[T12, T13]` |
| `T15` | Update docs, examples, and operational guidance | `[T12, T14]` |
| `T16` | Final validation and cutover | `[T15]` |

## Detailed Execution Plan

### T1 — Freeze Protocol Contract, Naming, And Cutover Policy
- `depends_on: []`
- `status: completed`
- Scope:
  - Freeze the implementation contract from `plans/fec-lossless-design.md`.
  - Explicitly ban legacy concepts in the new subsystem: `chunk`,
    cumulative ACK, sliding window, tree lanes, `BlockRepair`, sender ACK
    timeout.
  - Freeze compatibility policy: no mixed old/new peer support.
  - Freeze the target module layout and ownership boundaries.
- Deliverables:
  - Final glossary and invariants list.
  - Target file/module map.
  - Explicit migration policy for old callers and configs.
- Validation:
  - Team signoff on the task graph and invariants before code changes begin.
- Work log:
  - Rewrote `plans/fec-lossless-design.md` as a forward-looking spec for the new
    block-first subsystem.
  - Removed the `max_active_blocks` / local memory-bound caveat to keep the
    design simplicity-first.
  - Locked the cutover policy in this plan: no mixed old/new peer support, no
    compatibility shim protocol, and explicit removal of chunk/cumulative/window
    semantics.
- Files modified:
  - `plans/fec-lossless-design.md`
  - `plans/new-lossless-plan.md`
- Errors/gotchas:
  - None. This task is documentation/policy freeze only and does not change
    runtime code.

### T2 — Define The New Config And Public API Surface
- `depends_on: [T1]`
- Scope:
  - Replace chunk-first naming with block-first naming in runtime config and any
    public API surfaces.
  - Define the minimal external knobs:
    - `block_size`
    - plain/FEC enablement
    - `symbols_per_block`
    - FEC tree selection policy / `tree_ids`
  - Remove knobs that exist only because of the current implementation:
    - `fec_tree_lane_depth`
    - `fec_dispatch_burst`
    - public cumulative/window controls
    - public `symbol_size` unless proven necessary
  - Define migration behavior for Python bindings and flow-manager callers.
- Likely files:
  - `dataplane/src/node/config.rs`
  - `python-api/src/lib.rs`
  - flow/session start request structs under `dataplane/src/node/session`
- Deliverables:
  - New config struct shape.
  - Renamed public parameters (`block_size`, not `chunk_size`).
- Validation:
  - `rg -n "chunk_size|default_chunk_size|fec_tree_lane_depth|fec_dispatch_burst" dataplane python-api docs`

### T3 — Rework The Wire Protocol In `messages/`
- `depends_on: [T1]`
- Scope:
  - Replace the old wire model with a block-first protocol.
  - Define the frame set:
    - `Manifest`
    - `BlockData`
    - `BlockSymbol`
    - `Ready`
    - `BlockAck`
    - `BlockStatus`
    - `Eot`
  - Remove or retire:
    - chunk-indexed data frames
    - cumulative ACK frames
    - old FEC manifest split if it no longer matches the design
  - Keep the mode distinction in the manifest, not in two unrelated session
    models.
- Likely files:
  - `messages/src/lossless_session.rs`
- Deliverables:
  - Encoders/decoders for the new wire format.
  - Strict validation for illegal frame/mode combinations.
- Validation:
  - unit tests for encode/decode round-trips
  - grep confirms no old chunk/cumulative wire variants remain in the active API

### T4 — Build Shared Block Geometry And Shared Ledgers
- `depends_on: [T1]`
- `status: completed`
- Scope:
  - Implement `plan.rs` for:
    - `total_blocks`
    - per-block offsets
    - final-block sizing
    - symbol geometry derivation for FEC
  - Implement `ledger.rs` for:
    - per-block state
    - per-receiver ack status
    - duplicate ack/idempotence
    - session completion rule: all receivers ack all blocks
  - Make these types mode-agnostic.
- Likely files:
  - `dataplane/src/node/session/plan.rs`
  - `dataplane/src/node/session/ledger.rs`
- Deliverables:
  - shared block geometry and session state libraries
- Validation:
  - unit tests for offset math, final-block boundaries, duplicate ack behavior,
    and completion criteria
- Work log:
  - Added `dataplane/src/node/session/plan.rs` with shared block geometry,
    final-block sizing, and FEC source-symbol range derivation.
  - Added `dataplane/src/node/session/ledger.rs` with per-peer per-block ACK
    tracking, duplicate-ACK idempotence, and session-completion bookkeeping.
  - Exported the new modules from `dataplane/src/node/session/mod.rs`.
- Files modified:
  - `dataplane/src/node/session/mod.rs`
  - `dataplane/src/node/session/plan.rs`
  - `dataplane/src/node/session/ledger.rs`
- Errors/gotchas:
  - Initial ledger implementation borrowed `self` immutably after taking a
    mutable block borrow; fixed before validation.

### T5 — Add Tree-Visible Non-Blocking Processor Ingress Contract
- `depends_on: [T1]`
- `status: completed`
- Scope:
  - Make processor ingress the FEC backpressure boundary.
  - Expose non-blocking submission with explicit result values such as:
    - `Queued`
    - `WouldBlock`
    - `Closed`
  - Preserve the requirement that multi-tree FEC only runs where ingress remains
    tree-visible.
  - Reject or gate unsupported ingress modes during preflight.
- Likely files:
  - `dataplane/src/node/processor.rs`
  - `dataplane/src/node/route.rs` if route lookup behavior needs tightening
- Deliverables:
  - sender-usable non-blocking ingress API
  - clear runtime guardrails for sequential/tree-visible ingress
- Validation:
  - `cargo test -p nextmini processor::tests --lib`
- Work log:
  - Confirmed current `HEAD` already contains the explicit lossless-ingress
    contract surface in `dataplane/src/node/processor.rs`:
    `LosslessIngressContract`, `LosslessIngressSubmission`,
    `ProcessorHandle::lossless_ingress_contract`, and
    `ProcessorHandle::try_submit_lossless_packet`.
  - Validated that sequential ingress hashes FEC packets by `(flow_id, tree_id)`
    and reports `TreeVisibleNonBlocking` semantics.
  - Classified concurrent ingress and remote `OperatingMode::Max` connector
    routing as `SharedQueueNonBlocking`, making unsupported non-tree-visible
    paths explicit for later preflight/sender gating.
  - Confirmed targeted processor tests cover both per-tree backpressure and
    explicit shared-queue negative behavior.
- Files modified:
  - `dataplane/src/node/processor.rs`
  - `plans/new-lossless-plan.md`
- Errors/gotchas:
  - The repository already had `try_process_packet` and `SendOutcome`; T5 is
    the additional tree-visibility contract layered on top of that non-blocking
    result surface.
  - Sequential processor ingress is tree-visible, but remote
    `OperatingMode::Max` traffic still enters through the connector's shared
    queue and must not be treated as collaborative multi-tree capable.
  - By commit time, the processor-side code for this task had already landed in
    local `HEAD` as `254388f`, so this task commit only records validation and
    plan status.

### T6 — Rebuild The Session Runtime Around Typed Block-First Frames
- `depends_on: [T2, T3, T4]`
- Scope:
  - Rebuild `runtime.rs` as the main boundary for the new subsystem.
  - Route typed frames instead of raw bytes through ad hoc mode branches.
  - Implement the shared lifecycle:
    - `Manifest`
    - `Ready`
    - data/symbol traffic
    - `Eot`
    - final completion
  - Remove `api.rs` as a pseudo-boundary.
- Likely files:
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/session/mod.rs`
  - `dataplane/src/node/session/control.rs`
- Deliverables:
  - typed runtime shell with explicit plug points for plain and FEC mode
- Validation:
  - runtime tests for session registration, typed dispatch, and invalid-frame
    rejection

### T7 — Implement Plain-Mode Sender/Receiver End To End
- `depends_on: [T6]`
- Scope:
  - Build `sender/plain.rs` and `receiver/plain.rs`.
  - Plain sender:
    - emit `BlockData` in block order
    - use one tree only
    - retire only on `BlockAck`
  - Plain receiver:
    - validate `BlockData`
    - write completed blocks directly to their final offsets
    - send `BlockAck`
  - No sliding window, no cumulative ACK path, no contiguous-drain delivery path.
- Likely files:
  - `dataplane/src/node/session/sender/plain.rs`
  - `dataplane/src/node/session/receiver/plain.rs`
- Deliverables:
  - complete plain-mode path using the new runtime and wire contract
- Validation:
  - end-to-end plain transfer tests
  - duplicate block receive/duplicate ack tests

### T8 — Implement FEC Codec Adapter And Symbol Model
- `depends_on: [T4, T6]`
- Scope:
  - Build the per-block symbol model:
    - source symbols
    - additional fountain symbols
    - symbol id allocation
  - Ensure the symbol model aligns with block geometry and does not introduce a
    second top-level transfer unit.
  - Keep codec internals behind a small adapter boundary.
- Likely files:
  - `dataplane/src/node/session/fec.rs` or its replacement
  - `dataplane/src/node/session/sender/fec.rs`
  - `dataplane/src/node/session/receiver/fec.rs`
- Deliverables:
  - reusable FEC symbol encode/decode helpers
  - deterministic source-symbol geometry from block geometry
- Validation:
  - codec adapter tests
  - source symbol id / extra symbol id behavior tests

### T9 — Implement FEC Tree Scheduling And Backpressure Behavior
- `depends_on: [T5]`
- Scope:
  - Implement tree selection without tree lanes.
  - Use one scheduler that:
    - chooses the next block/symbol
    - picks the next candidate `tree_id`
    - retries another tree on `WouldBlock`
    - does not advance symbol state on backpressure
    - yields when all trees are backpressured
  - Keep the scheduling rule source-first.
- Likely files:
  - `dataplane/src/node/session/sender/fec.rs`
- Deliverables:
  - deterministic multi-tree scheduler
  - no per-tree worker/task design
- Validation:
  - backpressure unit tests
  - deterministic tree-selection tests
  - source-first scheduling tests

### T10 — Implement FEC Sender/Receiver End To End
- `depends_on: [T6, T9]`
- Scope:
  - FEC sender:
    - maintain `next_source_symbol`
    - maintain `next_fountain_symbol`
    - honor block-scoped extra-symbol demand
    - emit only `BlockSymbol`
  - FEC receiver:
    - collect symbols by `block_id`
    - decode blocks
    - emit `BlockAck`
    - emit `BlockStatus` with `deficit_symbols` when additional symbols are
      needed
  - Keep `BlockAck` as the only completion signal.
- Likely files:
  - `dataplane/src/node/session/sender/fec.rs`
  - `dataplane/src/node/session/receiver/fec.rs`
- Deliverables:
  - full FEC-mode session path on the new protocol
- Validation:
  - end-to-end FEC transfer tests with loss
  - tests proving extra symbols are sent only after the initial source pass

### T11 — Harden Control Semantics And Duplicate/Idempotent Behavior
- `depends_on: [T8, T10]`
- Scope:
  - Ensure control frames remain tree-independent.
  - Define duplicate-safe behavior for:
    - repeated `Ready`
    - repeated `BlockAck`
    - repeated `BlockStatus`
    - `BlockAck` re-advertisement after duplicate data/symbol arrivals
    - `Eot` after already-complete blocks
  - Ensure the receiver can re-advertise block completion while the session
    remains open, since the sender has no ACK timeout mechanism.
- Likely files:
  - `dataplane/src/node/session/control.rs`
  - `dataplane/src/node/session/runtime.rs`
  - sender/receiver control handlers
- Deliverables:
  - explicit idempotent control semantics
- Validation:
  - duplicate/loss/reordered control-frame tests

### T12 — Migrate Runtime Callers And Public Entrypoints
- `depends_on: [T7, T10, T11]`
- Scope:
  - Move all session callers onto the new runtime contract.
  - Update flow orchestration and Python bindings to use:
    - `block_size`
    - block-first semantics
    - new mode/config selection
  - Remove any callsite assumptions tied to:
    - chunk ordering
    - cumulative ACK progress
    - tree-lane tuning
  - If `unicast.rs` still mixes orchestration and transport concerns, move or
    shrink it.
- Likely files:
  - `dataplane/src/node/session/unicast.rs`
  - `python-api/src/lib.rs`
  - any flow/session request structs or helpers
- Deliverables:
  - all internal entrypoints speaking the new session contract
- Validation:
  - grep for old boundary terms in active callers
  - end-to-end flow tests through the main dataplane entrypoints

### T13 — Delete Legacy Session Code And Enforce Module/LOC Budget
- `depends_on: [T7, T8, T11]`
- Scope:
  - Delete old monolithic sender/receiver logic and dead compatibility helpers.
  - Delete obsolete policy/config helpers tied only to the old design.
  - Ensure the new session directory matches the target layout and code-size
    budget.
- Likely files:
  - legacy `sender.rs`
  - legacy `receiver.rs`
  - `api.rs`
  - obsolete pieces of `fec_policy.rs` / `fec.rs`
- Deliverables:
  - new session module tree only
  - explicit LOC snapshot confirming the target budget
- Validation:
  - `find dataplane/src/node/session -type f | xargs wc -l`
  - grep confirms legacy wire/control concepts are gone from active code

### T14 — Rewrite And Consolidate Tests Around Block Semantics
- `depends_on: [T12, T13]`
- Scope:
  - Replace existing session tests that encode the old model.
  - Consolidate tests around:
    - block geometry
    - plain-mode block delivery
    - FEC block decode and `BlockStatus`
    - duplicate ack behavior
    - per-tree backpressure behavior
    - control-frame duplicate/idempotent handling
    - end-to-end completion for multiple receivers
  - Reduce test sprawl by using shared helpers/harnesses.
- Likely files:
  - `dataplane/tests/*`
  - `dataplane/src/tests/mod.rs`
  - new shared test harness modules if needed
- Deliverables:
  - rewritten integration and unit test matrix
- Validation:
  - targeted test runs for plain, FEC, and backpressure suites
  - full relevant dataplane test run

### T15 — Update Docs, Examples, And Operational Guidance
- `depends_on: [T12, T14]`
- Scope:
  - Rewrite docs to match the new design and migration status.
  - Update configuration docs to use `block_size`.
  - Update examples and higher-level docs that still explain the old chunk or
    tree-lane model.
  - Document the new operational expectations:
    - plain default
    - optional FEC
    - block-level ACKs
    - tree-visible ingress requirement for multi-tree FEC
- Likely files:
  - `docs/content/docs/config/lossless.mdx`
  - `docs/content/docs/design/dataplane.md`
  - `docs/content/docs/design/multicast-groups.md`
  - `plans/fec-lossless-design.md`
  - examples/docs that reference the session transport
- Deliverables:
  - docs with no drift against the new implementation
- Validation:
  - `cd docs && bun run types:check`
  - grep for stale `chunk` / tree-lane terminology in lossless docs

### T16 — Final Validation And Cutover
- `depends_on: [T15]`
- Scope:
  - Run the final validation matrix.
  - Confirm legacy code is gone.
  - Confirm the config/API/docs/test surface all agree on the new protocol.
  - Gate release on explicit failure criteria.
- Validation matrix:
  - `cargo fmt --all`
  - `cargo check`
  - `cargo test --workspace`
  - `cargo nextest run --no-default-features --features python-extension --features dev-tests`
  - docs validation
  - targeted plain/FEC/backpressure tests
- Exit criteria:
  - all session entrypoints use `block` terminology
  - all tests/docs/configs align with the new design
  - no active code depends on chunk/cumulative/window/tree-lane semantics

## Parallelization Guidance

Recommended execution waves:

1. Wave A: `T1`
2. Wave B: `T2`, `T3`, `T4`, `T5`
3. Wave C: `T6`
4. Wave D: `T7`, `T8`, `T9`
5. Wave E: `T10`, `T11`
6. Wave F: `T12`, `T13`
7. Wave G: `T14`, `T15`
8. Wave H: `T16`

## Key Risks And Mitigations

- Risk: the new protocol leaks old terminology or semantics through config and
  caller APIs.
  - Mitigation: treat `chunk` and cumulative/window semantics as migration
    failures in `T2`, `T12`, and `T15`.

- Risk: FEC mode quietly reintroduces a second top-level transfer unit.
  - Mitigation: keep block geometry in shared code and keep FEC-specific code
    limited to symbols/scheduling/feedback only.

- Risk: per-tree backpressure is not actually observable in some ingress modes.
  - Mitigation: `T5` must make the ingress contract explicit and `T16` must
    reject unsupported runtime combinations.

- Risk: removing ACK timeouts leaves sessions stuck if block completion signals
  are lost.
  - Mitigation: `T11` must require duplicate-safe `BlockAck` re-advertisement
    while the session is open.

- Risk: the rewrite lands but code size and complexity remain close to the old
  subsystem.
  - Mitigation: `T13` includes explicit deletion and LOC budget enforcement
    before final validation.
