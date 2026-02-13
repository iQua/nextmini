# FEC Multi-Tree Perfect-Collaboration Plan

## Goal
Build a "perfect collaboration across multiple trees" FEC scheduler where sender-side backpressure drives immediate per-tree symbol scheduling, all trees collaborate on symbol emission, and there is no per-tree feedback requirement.

## Assumptions / Scope

- Target path: `dataplane/src/node/session/sender.rs` FEC sender path.
- Keep wire protocol unchanged (retain block-level `LosslessSessionControl::FecStatus`).
- Tree scheduling decisions are sender-local and backpressure-driven.
- Applies to FEC sessions (`fec_manifest.is_some()` and `fec_num_trees >= 2`).

---

## Task Plan (with dependencies)

### T1 — Define mode + invariants
- **depends_on: []**
- Add explicit collaborative multi-tree FEC scheduling mode and document invariants:
  - Lossless semantics unchanged.
  - No per-tree control feedback.
  - Deterministic tie-breaking under equal conditions.
- **Files:** `dataplane/src/node/config.rs`, docs.
- **Output:** clear mode gate and behavioral contract.

### T2 — Add sender-side FEC scheduler core (separate from loop)
- **depends_on: [T1]**
- Extract FEC planning from `SenderState` loop into scheduler abstraction:
  - active blocks
  - symbol generation cursor
  - per-block repair budget state
- Replace global `fec_pending_symbols: VecDeque<PendingFecSymbol>` model with scheduler-owned symbol supply.
- **Files:** `dataplane/src/node/session/sender.rs` (or new `fec_scheduler.rs` + `mod.rs`).
- **Output:** maintainable scheduling core.

### T3 — Add non-blocking packet submission API for backpressure sensing
- **depends_on: [T1]**
- Add `try_process_packet(...) -> SendOutcome` on `ProcessorHandle` and both sequential/concurrent impls.
- Retain existing `process_packet(...).await` for compatibility.
- **Files:** `dataplane/src/node/processor.rs`.
- **Output:** sender can sense immediate backpressure without stalling loop progress.

### T4 — Ensure per-tree processing isolation at processor ingress
- **depends_on: [T3]**
- Make processor ingress queue selection tree-aware for FEC packets (using `packet.lossless_fec_tree_id()`), so trees do not collapse into a single queue lane.
- Keep existing hash path for non-FEC packets.
- Add explicit guard/fallback behavior when isolation cannot be guaranteed.
- **Files:** `dataplane/src/node/processor.rs`.
- **Output:** tree congestion does not induce cross-tree head-of-line (HOL) blocking at ingress.

### T5 — Implement per-tree sender lanes/workers
- **depends_on: [T2, T3, T4]**
- Add bounded lane/channel per `tree_id` with a worker task per lane.
- Worker owns blocking send path for tree-specific packets.
- Main sender loop pushes to lanes via non-blocking/try path; lane fullness is tree-local backpressure signal.
- **Files:** `dataplane/src/node/session/sender.rs`.
- **Output:** immediate per-tree adaptation under backpressure.

### T6 — Replace hash-based tree assignment with collaborative dispatch
- **depends_on: [T5]**
- Replace/augment `select_fec_tree_id(block_id, symbol_id)` behavior:
  - assign symbols at dispatch-time, not pre-materialization time
  - dispatch to currently writable tree lanes
  - skip blocked lanes and continue with others
  - deterministic RR/DRR tie-breakers for fairness
- Keep optional legacy hash mode for debugging/fallback.
- **Files:** `dataplane/src/node/session/sender.rs`.
- **Output:** true collaborative multi-tree symbol emission.

### T7 — Preserve reliability semantics and completion rules
- **depends_on: [T2, T6]**
- Keep `FecStatus` semantics block-level only (no per-tree protocol change).
- Ensure retire/EOT checks include:
  - required symbols satisfied per block-level progress
  - all tree lanes drained
  - no outstanding required work
- **Files:** `dataplane/src/node/session/sender.rs`, optional helper cleanup in `dataplane/src/node/session/control.rs`.
- **Output:** lossless guarantees preserved under collaborative scheduling.

### T8 — Runtime config + API propagation
- **depends_on: [T1, T6]**
- Add/validate tunables:
  - per-tree lane depth
  - scheduler dispatch burst
  - max tree lanes
- Extend external API paths to pass tree count cleanly (e.g., Python API `send_data(..., fec_num_trees=...)`).
- **Files:** `dataplane/src/node/session/runtime.rs`, `dataplane/src/node/config.rs`, `python-api/src/lib.rs`.
- **Output:** feature is configurable and externally usable.

### T9 — Regression + feature tests
- **depends_on: [T6, T7, T8]**
- Add tests for:
  1. one tree blocked ⇒ others continue immediately,
  2. all trees blocked ⇒ no busy-spin/livelock,
  3. EOT/completion correctness under asymmetric pressure,
  4. no wire-protocol change regression,
  5. fallback/guard behavior.
- **Files:** `dataplane/src/node/session/sender.rs` tests, `dataplane/src/node/processor.rs` tests, optional integration tests.
- **Output:** correctness confidence and regression safety.

### T10 — Observability + rollout strategy
- **depends_on: [T9]**
- Add per-tree counters/logging (`queued`, `sent`, `blocked`, `dropped`, `drained`).
- Update docs for config/API and rollout notes.
- Roll out behind mode/flag (default-off first, then progressive enablement).
- **Files:** sender/processor logs + docs.
- **Output:** safe deployment and diagnosability.

---

## Dependency Graph

- T1 → T2, T3, T8
- T3 → T4
- T2 + T3 + T4 → T5
- T5 → T6
- T2 + T6 → T7
- T1 + T6 → T8
- T6 + T7 + T8 → T9
- T9 → T10

---

## Acceptance Criteria

1. Under induced backpressure on tree A, trees B..N continue symbol emission without waiting on A.
2. No per-tree feedback/control-plane protocol additions are required.
3. Lossless completion semantics remain intact (no early EOT, no missing bytes).
4. Existing non-FEC and legacy FEC behavior remains unchanged unless collaborative mode is enabled.
5. Feature is configurable, test-covered, and observable per tree.

---

## Key Risks and Mitigations

- **Risk:** resource explosion with many trees (tasks/channels/memory).
  - **Mitigation:** enforce `fec_max_tree_lanes` in runtime preflight and bounded lane depths.

- **Risk:** hidden ingress serialization still creates HOL.
  - **Mitigation:** tree-aware processor ingress selection and explicit guard behavior.

- **Risk:** scheduler livelock when all lanes are full.
  - **Mitigation:** event-driven wakeups + bounded fallback tick path.

- **Risk:** non-deterministic scheduling complicates debugging.
  - **Mitigation:** deterministic tie-breakers and optional legacy hash scheduling mode.

- **Risk:** completion logic sends EOT too early under delayed trees.
  - **Mitigation:** EOT gate includes lane-drain + required-work checks, not just source drain.
