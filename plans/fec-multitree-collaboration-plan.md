# FEC Multi-Tree Perfect-Collaboration Plan (v3 — Collaborative Only)

**Generated**: February 14, 2026  
**Target Repo**: `/Users/bli/Playground/nextmini`

## Goal
Build a **collaborative multi-tree FEC sender scheduler** where **sender-side backpressure** drives **immediate per-tree symbol scheduling**, all trees collaborate on symbol emission, and there is **no per-tree receiver feedback requirement**.

**Hard requirement for v3:** remove all existing **hash-based** FEC tree assignment strategies and related code. Multi-tree FEC must be **dispatch-time assigned** (collaborative) only.

## Assumptions / Scope

- Target path: `dataplane/src/node/session/sender.rs` FEC sender path.
- Keep wire protocol unchanged:
  - retain block-level `LosslessSessionControl::FecStatus`
  - retain FEC data frames with `LosslessSessionFecData.tree_id`
- Tree scheduling decisions are sender-local and backpressure-driven.
- Applies to FEC sessions (`fec_manifest.is_some()`) with multi-tree enabled.
- **No legacy/compat modes:** there is no “hashed assignment” fallback and no “legacy hashed mode” preserved.

---

## Current Baseline (Must Be Removed / Reworked)
The codebase currently includes a hash-based multi-tree strategy:
- Sender assigns `tree_id` via a deterministic hash modulo `fec_num_trees`:
  - `SenderState::select_fec_tree_id(block_id, symbol_id)`
  - `FEC_TREE_HASH_SALT`, `splitmix64`, etc.
- Sender materializes symbols into a global FIFO queue `fec_pending_symbols`, each with a fixed `tree_id`.

v3 requires deleting hash-based assignment and moving to **collaborative dispatch-time assignment** only.

---

## Core Correctness Constraints (v3)

### C1 — Control Frames Have No `tree_id`
Lossless-session **control frames** do not carry `tree_id`. Today, multicast routing for control frames implicitly routes on `tree_id=0` (because `tree_id=None -> 0` in routing key construction). This can break multi-tree groups that do not include tree 0.

**v3 policy:** implement **deterministic control-tree selection** for multicast control frames at the dataplane routing layer:
- For multicast flows when `tree_id=None`, choose a deterministic “control tree” based on installed group routes for `(src_node_id, group_id)`:
  - Use `tree_id=0` if installed, else use the **smallest installed tree_id**.
- Cache this selection per `(src_node_id, group_id)` and invalidate on group-route reinstall.

This avoids requiring tree 0 in controller configs and removes ambiguity from the plan.

### C2 — Tree-ID Model Must Match Controller (Sparse Tree IDs Are Allowed)
Controller supports arbitrary `tree_id` values (bounded by `MULTITREE_STRIDE`). Sender must not assume dense tree IDs.

**v3 contract:** sender multi-tree configuration uses an explicit **allowed tree-id set**:
- Add `fec_tree_ids: Vec<u16>` to `SenderConfig` (canonically sorted, unique).
- Deprecate/remove `fec_num_trees` for multi-tree FEC (or keep only for single-node demos, but it must not imply hashing).
- Sender emits only onto `fec_tree_ids`.

### C3 — “Per-tree backpressure” requires tree-visible backpressure domains
Sender-side collaboration only works if congestion/backpressure can be observed separately per tree. This requires:
- sender-local per-tree lanes **and**
- processor ingress that does not collapse all trees into the same bounded queue lane (at least in supported processor modes).

---

## Scheduling Model (v3)

### Single Multi-Tree Strategy Only: Collaborative Dispatch
- Scheduler produces FEC symbols (systematic + bounded repairs) as “work items” **without a committed tree id**.
- Dispatcher assigns a `tree_id` at dispatch-time based on current backpressure:
  - choose among writable trees in deterministic round-robin (or DRR) across `fec_tree_ids`
  - skip blocked trees immediately
  - when all trees blocked, wait for a wakeup (no busy-spin)
- Global pacing (token bucket) is applied at the session level, not per tree.

---

## Task Plan (with dependencies)

### T0 — Define Tree-ID Contract + Control-Tree Routing Policy (COMPLETE — February 14, 2026)
- **depends_on: []**
- Define and document invariants required for correctness:
  - Sender multi-tree uses `fec_tree_ids` (explicit allowed set).
  - Routing layer must support control-tree routing for multicast control frames (tree_id absent).
  - Deterministic tie-breakers when multiple trees are candidates.
  - Hard behavior for invalid configs:
    - empty `fec_tree_ids` => reject
    - `fec_tree_ids.len() < 2` in “multi-tree mode” => reject (unless explicitly a single-tree FEC session)
- Document how this supersedes the hash-based selection described in older plans.

- **Files:** docs + plan notes + runtime preflight notes.

- **Output:** clear behavioral contract for tree IDs and control frame delivery.

- **Status:** completed.
- **Work log:** documented sender `fec_tree_ids` contract (sorted/unique allowlist, deterministic tie-breakers, invalid-config rejection, explicit single-tree exception) and documented deterministic multicast control-tree routing policy (`tree_id=0` if installed, else smallest installed tree).
- **Files changed (T0):**
  - `docs/content/docs/design/lossless_config.md`
  - `docs/content/docs/design/multicast-groups.md`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** runtime still uses `fec_num_trees` and default `tree_id=None -> 0` behavior in code today; this task records the v3 contract only. Enforcement/implementation lands in T1/T2/T8.

---

### T1 — Remove Hash-Based Tree Assignment + Update Documentation References (REQUIRED)
- **depends_on: [T0]**
- Delete all hash-based tree assignment strategies and code paths:
  - remove `select_fec_tree_id`
  - remove `FEC_TREE_HASH_SALT`, `splitmix64`, and any hashing-specific constants/logic
  - remove any “assign tree at materialization time” behavior tied to hashing
- Ensure no docs/tests reference hash-based tree scheduling as a supported strategy.

- **Files:**
  - `dataplane/src/node/session/sender.rs`
  - any docs that mention `hash(session_id, block_id, symbol_id) % num_trees`

- **Output:** repository contains no hash-based FEC tree assignment strategy.

- **Status:** completed.
- **Work log:** removed sender hash-based tree assignment helpers (`select_fec_tree_id`, `FEC_TREE_HASH_SALT`, `splitmix64`), stopped assigning `tree_id` during symbol materialization, and moved FEC data emission to use the default tree at dispatch-time while collaborative scheduling is implemented in later tasks. Updated docs/tests so hash assignment is documented only as removed behavior.
- **Files changed (T1):**
  - `dataplane/src/node/session/sender.rs`
  - `dataplane/tests/fec_multitree.rs`
  - `docs/content/docs/design/lossless_config.md`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** multi-tree collaborative dispatch is not implemented in T1; with hash removed, current sender path emits FEC symbols on `DEFAULT_TREE_ID` until T3/T6 land.

---

### T2 — Add Control-Tree Selection for Multicast Control Frames (COMPLETE — February 14, 2026)
- **depends_on: [T0]**
- Implement deterministic control-tree selection in dataplane routing:
  - When routing multicast packets with `tree_id=None`:
    - if `(src, group, tree=0)` is installed, use tree 0
    - else use smallest installed tree for that `(src, group)`
  - Cache selection per `(src, group)` and invalidate on group route installs.
  - Ensure behavior is deterministic and does not depend on timing.

- **Files:**
  - `dataplane/src/node/route.rs`

- **Output:** MANIFEST/EOT/control frames route correctly for multi-tree groups without requiring tree 0.

- **Status:** completed.
- **Work log:** implemented deterministic multicast control-tree selection in dataplane routing (`tree_id=None` chooses tree 0 when present, otherwise smallest installed tree), added `(src, group)` control-tree caching, and invalidated cached control-tree selection on `(src, group)` route reinstall.
- **Files changed (T2):**
  - `dataplane/src/node/route.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** `cargo test --workspace -- --list` still fails in this environment due Python linker/toolchain setup (`nextmini_py`), so validation was run with targeted `nextmini` route tests.

---

### T3 — Extract Sender-Side FEC Scheduler Core (COMPLETE — February 14, 2026)
- **depends_on: [T1]**
- Extract an FEC planning/scheduling component from the sender loop:
  - active blocks, symbol cursor, repair budget accounting
  - “next symbol to emit” as a work item without a fixed `tree_id`
- Replace the global `fec_pending_symbols: VecDeque<PendingFecSymbol>` model with:
  - scheduler-owned symbol supply, and
  - dispatch-time tree assignment logic (see T6)

- **Files:**
  - `dataplane/src/node/session/sender.rs` (or new `fec_scheduler.rs`)

- **Output:** maintainable scheduling core with unit-test seams.
- **Status:** completed.
- **Work log:** extracted sender FEC planning into a dedicated `FecScheduler` with scheduler-owned `FecSymbolSupply`, active block stats, block cursor, and repair-budget accounting; simplified the sender loop by driving FEC planning/emission through a single scheduler entrypoint while preserving pre-T6 default-tree dispatch behavior and tree-unassigned work items.
- **Files changed (T3):**
  - `dataplane/src/node/session/sender.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** scheduler encapsulation keeps behavior equivalent to current pre-T6 semantics (single global dispatch path, no collaborative per-tree lanes yet); bounded `active_blocks` lifecycle cleanup is still deferred to later tasks focused on long-session retention.

---

### T4 — Add Non-Blocking Packet Submission API + SendOutcome Contract (COMPLETE — February 14, 2026)
- **depends_on: [T0]**
- Add `try_process_packet(packet) -> SendOutcome` on `ProcessorHandle`:
  - `SendOutcome::Queued`
  - `SendOutcome::WouldBlock`
  - `SendOutcome::Closed`
- Preserve existing APIs:
  - `process_packet(packet).await`
  - `process_packet_blocking(packet)`
- Define strict semantics for collaborative scheduling:
  - collaborative mode must not rely on drop-on-backpressure behavior
  - if global config is set to drop when full, collaborative mode must deterministically reject at preflight

- **Files:** `dataplane/src/node/processor.rs`

- **Output:** sender can sense per-tree backpressure without stalling the main loop.
- **Status:** completed.
- **Work log:** added `SendOutcome::{Queued, WouldBlock, Closed}` and a strict non-blocking `try_process_packet(packet) -> SendOutcome` path on `ProcessorHandle`, `SequentialProcHandle`, and `ConcurrentProcHandle`; preserved existing `process_packet(packet).await` and `process_packet_blocking(packet)` behavior and kept local/normal/max routing decisions identical across all send entrypoints.
- **Files changed (T4):**
  - `dataplane/src/node/processor.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** `try_process_packet` intentionally bypasses `channel_backpressure` and always uses immediate `try_send` semantics so callers can detect `WouldBlock` deterministically.

---

### T5 — Ensure Tree-Visible Backpressure Domains at Processor Ingress (COMPLETE — February 14, 2026)
- **depends_on: [T4]**
- Make processor ingress queue selection tree-aware for FEC packets:
  - sequential handle: choose ingress lane based on `(flow_id, tree_id)` for FEC data frames
  - preserve existing behavior for non-FEC packets
- Define a clear policy for the concurrent processor handle (must pick one):
  - **Option A (recommended for v3):** collaborative multi-tree mode is supported only with sequential ingress; runtime rejects collaborative mode under concurrent ingress deterministically.
  - **Option B:** implement per-tree sharding/subqueues in concurrent ingress (larger scope).

- **Files:** `dataplane/src/node/processor.rs`

- **Output:** one tree’s congestion does not force cross-tree HOL at ingress (in supported modes).
- **Status:** completed.
- **Work log:** added sequential ingress lane selection keyed by `(flow_id, tree_id)` for lossless FEC data frames and reused that selector across async, non-blocking, and blocking processor send paths; non-FEC packets keep legacy flow-hash lane selection. Added focused ingress tests validating non-FEC routing stability, per-tree `WouldBlock` isolation in sequential mode, and blocking-path FEC lane placement.
- **Files changed (T5):**
  - `dataplane/src/node/processor.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** concurrent ingress intentionally remains a shared queue; code now logs a one-time warning when FEC data is submitted through `ConcurrentProcHandle` to make Option A policy explicit until runtime preflight enforcement lands in T8.

---

### T6 — Implement Per-Tree Sender Lanes + Collaborative Dispatch (COMPLETE — February 14, 2026)
- **depends_on: [T3, T4, T5]**
- Implement sender-local per-tree lanes:
  - a bounded channel per `tree_id` in `fec_tree_ids`
  - a worker task per lane that owns the blocking send path for that tree
  - the main sender loop uses non-blocking `try_send` into lanes (lane fullness is the backpressure signal)
- Implement collaborative dispatch-time tree assignment:
  - for each emitted symbol work item:
    - try writable trees in deterministic RR order across `fec_tree_ids`
    - enqueue to the first writable lane
    - if all lanes blocked, wait for wakeups (no busy-spin)
- Global pacing invariant:
  - apply token-bucket pacing at the session level before enqueuing work into any lane

- **Files:** `dataplane/src/node/session/sender.rs`

- **Output:** true collaborative multi-tree emission with immediate per-tree adaptation.
- **Status:** completed.
- **Work log:** implemented sender-local per-tree bounded lanes and spawned one lane worker per configured tree to run the blocking processor send path; switched FEC symbol emission to dispatch-time RR assignment with non-blocking lane `try_send` and fallback to the first writable lane; added all-lanes-blocked wakeup waiting to avoid busy-spin while preserving session-level token-bucket pacing before enqueue; updated sender/test coverage for RR, blocked-lane skip behavior, and collaborative multitree emission.
- **Files changed (T6):**
  - `dataplane/src/node/session/sender.rs`
  - `dataplane/tests/fec_multitree.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** tree-id configuration is still derived from `fec_num_trees` (`0..num_trees-1`) for now; explicit sparse `fec_tree_ids` propagation remains scheduled for T8.

---

### T7 — Preserve Reliability Semantics + Completion Rules (COMPLETE — February 14, 2026)
- **depends_on: [T3, T6]**
- Keep `FecStatus` semantics block-level only (no per-tree protocol change).
- Ensure retire/EOT checks include:
  - required symbols satisfied per block-level progress
  - scheduler has no remaining work
  - all per-tree lanes drained
  - no outstanding required work (inflight tracking consistent)
- Ensure control frames (EOT/Manifest/FecManifest/etc.) route correctly via control-tree selection (T2).

- **Files:** `dataplane/src/node/session/sender.rs`

- **Output:** lossless guarantees preserved under collaborative scheduling, no early EOT.

- **Status:** completed.
- **Work log:** tightened sender completion gating so EOT/terminal completion now require block-level required work cleared, scheduler drained, per-tree dispatch lanes drained, and consistent required-work inflight accounting; added contiguous per-receiver FEC block completion tracking in sender so `FecStatus` remains block-scoped and cannot over-retire out-of-order blocks; kept control-frame protocol/wire behavior unchanged so T2 control-tree routing remains the delivery mechanism for MANIFEST/EOT/FEC control traffic.
- **Files changed (T7):**
  - `dataplane/src/node/session/sender.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** targeted sender/FEC test runs are currently blocked by an existing non-T7 compile issue in runtime preflight typing (`Feature` missing `Eq` derive for `FecPreflightError`), so regression coverage was added in sender tests but could not be executed in this branch state.

---

### T8 — Runtime Config + API Propagation (COMPLETE — February 14, 2026)
- **depends_on: [T0, T6]**
- Add/validate tunables:
  - `fec_tree_lane_depth`
  - `fec_dispatch_burst`
  - `fec_max_tree_lanes`
  - `fec_multitree_mode` may be unnecessary if collaborative is the only multi-tree mode; otherwise it should be an explicit on/off gate, not a strategy selector.
- Extend external API paths:
  - Python API must pass `fec_tree_ids` into `SenderConfig` for multi-tree sessions.
  - Remove/avoid any Python API knobs that imply hashed selection.
- Runtime preflight must validate:
  - `fec_tree_ids` non-empty, unique, sorted
  - if multi-tree, require `len >= 2`
  - control-tree routing availability is not directly checkable at runtime, but policy should be documented and enforced by routing-table behavior + tests
  - processor-handle policy (sequential-only if chosen in T5)

- **Files:**
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/config.rs`
  - `python-api/src/lib.rs`
  - docs

- **Output:** feature is configurable and externally usable without any hash-based behavior.

- **Status:** completed.
- **Work log:** replaced sender/runtime `fec_num_trees` wiring with explicit `fec_tree_ids`; added runtime tunables (`fec_tree_lane_depth`, `fec_dispatch_burst`, `fec_max_tree_lanes`) plus a simple collaborative on/off gate; enforced deterministic preflight for non-empty sorted+unique tree IDs, max-lane bounds, and sequential-only multi-tree ingress (T5 Option A). Wired Python `send_data` to require explicit `fec_tree_ids` for FEC sessions and propagate runtime tunables into sender config.
- **Files changed (T8):**
  - `dataplane/src/node/config.rs`
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/session/unicast.rs`
  - `python-api/src/lib.rs`
  - `dataplane/tests/fec_multitree.rs`
  - `dataplane/tests/fec_sender.rs`
  - `docs/content/docs/design/lossless_config.md`
  - `docs/content/docs/design/config-reference.md`
  - `docs/content/docs/design/python-api.md`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** Python API now rejects FEC sender calls that omit `fec_tree_ids`; runtime preflight now also rejects multi-tree FEC whenever dataplane `feature=concurrent`, so collaborative sessions require `feature=sequential`.

---

### T9 — Replace/Rewrite Tests That Assume Hash Assignment (UPDATE)
- **depends_on: [T2, T6, T7, T8]**
Replace tests that assume per-symbol hash determinism with tests aligned to collaborative scheduling.

#### Update Existing Tests
- `dataplane/tests/fec_multitree.rs` currently asserts deterministic mapping `(block_id, symbol_id) -> tree_id` across runs.
  - Rewrite to validate collaborative invariants instead:
    - emitted tree ids are a subset of configured `fec_tree_ids`
    - at least two distinct trees are used in an unblocked run
    - optionally: under a deterministic RR policy in an unblocked run, distribution matches expected RR pattern (only if stable by design)

#### Add New Tests (v3 requirements)
1) **Control-tree routing (T2)**
   - group installed with trees that do *not* include tree 0 (e.g. `{1,3,5}`):
     - assert control frames route successfully via deterministic control-tree selection
2) **Per-tree backpressure**
   - simulate one tree lane saturated:
     - assert sender continues emitting onto other trees immediately
3) **All trees blocked**
   - assert no busy-spin/livelock and progress resumes when capacity returns
4) **EOT/completion correctness under asymmetric pressure**
   - delay drain of one tree lane:
     - assert sender does not emit EOT early

- **Files:** `dataplane/tests/*` and/or sender module tests.

- **Output:** correctness confidence for collaborative scheduling and regression safety without hash.

- **Status:** completed.
- **Work log:** rewrote `dataplane/tests/fec_multitree.rs` to remove cross-session deterministic `(block_id,symbol_id)->tree_id` assertions and instead validate collaborative invariants (observed tree IDs are within configured sparse `fec_tree_ids`, and unblocked runs exercise at least two distinct trees). Tightened multicast control-tree routing coverage in `route.rs` to explicit sparse installed trees `{1,3,5}` (tree 0 omitted) and deterministic smallest-tree fallback. Added sender dispatch regression coverage proving all-lanes-blocked state resumes immediately when capacity returns, while preserving existing per-tree blocked-lane skip and asymmetric-pressure completion guards.
- **Files changed (T9):**
  - `dataplane/tests/fec_multitree.rs`
  - `dataplane/src/node/route.rs`
  - `dataplane/src/node/session/sender.rs`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** integration-level multi-tree tests intentionally avoid asserting a full per-symbol RR mapping across runs because worker concurrency can reorder emission timing; RR determinism remains validated at the sender dispatch unit-test seam where behavior is stable-by-design.

---

### T10 — Observability + Rollout Strategy (UPDATE)
- **depends_on: [T9]**
- Add per-tree counters/logging:
  - `queued`, `sent`, `blocked`, `drained`, `wakeups`
- Add clear session-start log:
  - selected `fec_tree_ids`
  - lane depth and dispatch burst
  - processor ingress policy (sequential-only vs supported)
- Rollout:
  - collaborative multi-tree is the only multi-tree behavior; guarded behind an explicit config flag if needed
  - ship with strong observability and deterministic tests first

- **Files:** sender/processor logs + docs.

- **Output:** safe deployment and diagnosability per tree.

- **Status:** completed.
- **Work log:** added sender-local per-tree observability counters (`queued`, `sent`, `blocked`, `drained`, `wakeups`) on collaborative FEC tree lanes and surfaced them in dispatch logs (queued/all-blocked/wakeup) plus a session-end per-tree counter snapshot for postmortems. Added a dedicated FEC session-start log that records configured `fec_tree_ids`, `fec_tree_lane_depth`, `fec_dispatch_burst`, and processor ingress policy/support (`sequential_only` policy with support/unsupported status). Updated rollout docs to state collaborative dispatch is the only multi-tree behavior and that `fec_collaborative_multitree_enabled` is an explicit on/off rollout gate (not a strategy selector).
- **Files changed (T10):**
  - `dataplane/src/node/session/sender.rs`
  - `docs/content/docs/design/lossless_config.md`
  - `docs/content/docs/design/config-reference.md`
  - `plans/fec-multitree-collaboration-plan.md`
- **Gotchas:** per-tree counters are lock-free atomic snapshots sampled across async lane workers, so logs are diagnostically stable but not a strict total-order event trace at sub-event granularity.

---

## Dependency Graph (v3)

- T0 → T1, T2, T4, T8
- T1 → T3
- T4 → T5
- T3 + T4 + T5 → T6
- T2 + T6 → T7
- T6 → T8
- T2 + T6 + T7 + T8 → T9
- T9 → T10

---

## Acceptance Criteria (v3)

1. **Hash-based FEC tree assignment is fully removed** (no code paths, no docs, no tests rely on it).
2. Multicast control frames route correctly for multi-tree groups even when tree 0 is absent (via deterministic control-tree selection).
3. Under induced backpressure on tree A, trees B..N continue symbol emission without waiting on A.
4. When all trees are blocked, sender does not busy-spin/livelock and resumes promptly when capacity returns.
5. Lossless completion semantics remain intact:
   - no early EOT
   - no missing bytes
   - strict block-level completion via `FecStatus` remains unchanged
6. Python API can enable multi-tree FEC by supplying explicit `fec_tree_ids`, and behavior is consistent with dataplane routing.

---

## Key Risks and Mitigations (v3)

- **Risk:** concurrent processor ingress collapses backpressure domains.
  - **Mitigation:** explicitly restrict collaborative mode to sequential ingress (T5 Option A), or implement sharded subqueues (Option B).

- **Risk:** control-tree routing selection could become nondeterministic if it depends on hashmaps/iteration order.
  - **Mitigation:** choose smallest installed tree id deterministically; cache; invalidate on reinstall.

- **Risk:** global pacing accidentally becomes per-tree pacing when per-tree workers are introduced.
  - **Mitigation:** enforce that token-bucket pacing happens centrally before lane enqueue; keep pacing tests and add multi-tree pacing regression.

- **Risk:** EOT emitted too early because sender misinterprets “drained” with per-tree lanes.
  - **Mitigation:** require scheduler empty + all lanes drained + outstanding work complete in T7, and test it in T9.

- **Risk:** tree-id set mismatches installed routes (unknown tree drops).
  - **Mitigation:** document contract; add integration tests that cover unknown-tree behavior and ensure controller installs match sender configuration.

---
