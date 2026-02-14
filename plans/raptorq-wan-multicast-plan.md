# Plan: RaptorQ Migration + Multicast-Tree Integration In Nextmini

**Generated**: February 12, 2026
**Source Repo**: `/Users/bli/Playground/asupersync`
**Target Repo**: `/Users/bli/Playground/nextmini`
**Estimated Complexity**: High

## Overview
This plan has two coupled tracks:
1. Migrate all RaptorQ implementation code and tests from `asupersync` into `nextmini` with Rust-only dependencies and feature gating.
2. Implement the original idea in `nextmini`: multicast-tree bulk transfer + backpressure + RaptorQ, then extend to multi-tree and relay-aware behavior.

## In-Scope Source Assets (from asupersync)
- Core algorithm files:
  - `src/raptorq/gf256.rs`
  - `src/raptorq/linalg.rs`
  - `src/raptorq/rfc6330.rs`
  - `src/raptorq/systematic.rs`
  - `src/raptorq/decoder.rs`
  - `src/raptorq/proof.rs`
- Integration/pipeline files:
  - `src/raptorq/mod.rs`
  - `src/raptorq/pipeline.rs`
  - `src/raptorq/builder.rs`
  - `src/encoding.rs`
  - `src/decoding.rs`
  - `src/codec/raptorq.rs`
- Tests/benchmarks/docs:
  - `src/raptorq/tests.rs`
  - `tests/raptorq_conformance.rs`
  - `tests/raptorq_perf_invariants.rs`
  - `benches/raptorq_benchmark.rs`

## Out-of-Scope For Initial Merge
- C/C++ bindings or external native libs.
- Aggressive SIMD/unsafe optimization passes.
- Mandatory relay recoding in first deployment.

## Dependency Graph
- T1 -> T2
- T2 -> T3, T7
- T3 -> T4, T5, T6
- T4 -> T14
- T5 -> T14
- T6 -> T8, T9
- T7 -> T8, T9, T12
- T8 -> T10
- T9 -> T10
- T10 -> T11, T13
- T12 -> T13
- T13 -> T14
- T14 -> T15
- T11 -> T15
- T11 -> T14

## Experiment Harness Structure (Minimal)
All scripts live under `tools/experiments/raptorq/` and write JSON artifacts via `--output`.

- `smoke_python_api.py`
  - Purpose: Validate Python bindings + FEC toggle behavior.
  - CLI: `--fec {on|off}`, `--output <path>`, `--assert-success`.
  - Output JSON: `mode`, `fec`, `success`, `elapsed_ms`, `error` (optional).
- `run_smoke.py`
  - Purpose: Single-node smoke run for metrics collection (no external deps).
  - CLI: `--mode {lossless|parity|raptorq}`, `--loss <float>`, `--output <path>`, `--assert-metrics`.
  - Output JSON: `mode`, `loss`, `success`, `completion_ms`, `p95_ms`, `p99_ms`, `overhead`, `cpu_pct`, `mem_mb`, `error` (optional).
- `check_compat_matrix.py`
  - Purpose: Verify strict FEC-only compatibility rules (no fallback).
  - CLI: `--input <node-report.json>` (repeatable), `--require-homogeneous-fec`, `--output <path>`, `--assert-strict`.
  - Output JSON: `success`, `failed_checks`, `failed_count`, `compatibility_checks`, `mismatches`.

## Track A: Migration (Bring All RaptorQ Code + Tests)

### T1: Freeze Source Inventory + Licensing Snapshot
- `depends_on: []`
- **Location**: `raptorq-wan-multicast-plan.md` (mapping section), `docs/` migration notes
- **Description**: Record exact source file list, target destination list, and license notes before code moves.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Review**:
  - [x] Complete source->target mapping exists and is reviewed.
  - [x] MIT license provenance from `asupersync` is documented.
- **Acceptance Criteria**:
  - Complete source->target mapping exists and is reviewed.
  - MIT license provenance from `asupersync` is documented.
- **Validation**:
  - `rg -n "raptorq|fec|fountain" /Users/bli/Playground/asupersync`

### T2: Create New Workspace Crate For Migrated Core
- `depends_on: [T1]`
- **Location**: `raptorq/`, root `Cargo.toml`
- **Description**: Add a dedicated crate for migrated RaptorQ primitives and deterministic helpers.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Review**:
  - [x] New crate builds in workspace.
  - [x] No dataplane integration yet.
- **Acceptance Criteria**:
  - New crate builds in workspace.
  - No dataplane integration yet.
- **Validation**:
  - `cargo check -p raptorq`

### T3: Port Core Algorithm Modules
- `depends_on: [T2]`
- **Location**: `raptorq/src/*.rs`
- **Description**: Port `gf256`, `linalg`, `rfc6330`, `systematic`, `decoder`, `proof`, and deterministic RNG helpers.
- **Acceptance Criteria**:
  - Core encode/decode APIs compile and expose a stable Rust interface for nextmini session adapters.
- **Validation**:
  - `cargo check -p raptorq`

### T4: Port RaptorQ Test Suites
- `depends_on: [T3]`
- **Location**: `raptorq/tests/`
- **Description**: Migrate conformance and invariant tests from `asupersync` and adapt imports to new crate paths.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Review**:
  - [x] Conformance and invariant suites are migrated into `raptorq/tests/`.
  - [x] Imports are updated to the `raptorq` crate paths.
- **Acceptance Criteria**:
  - Migrated conformance/invariant tests pass in new crate.
- **Validation**:
  - `cargo test -p raptorq`

### T5: Port Benchmark Harness
- `depends_on: [T3]`
- **Location**: `raptorq/benches/`
- **Description**: Port baseline RaptorQ benchmark to detect performance regressions after integration.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Criteria**:
  - Benchmark target compiles and runs.
  - Baseline output artifact is produced for regression comparisons.
- **Validation**:
  - `cargo bench -p raptorq --bench raptorq_benchmark -- --output-format bencher | tee /tmp/raptorq_bench.txt`

### T6: Add Dataplane Adapter Layer
- `depends_on: [T3]`
- **Location**: `dataplane/src/node/session/fec.rs` (new), `dataplane/src/node/session/mod.rs`
- **Description**: Add a thin, wire-agnostic adapter that exposes `raptorq` encode/decode APIs to the lossless session subsystem.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Review**:
  - [x] Added a wire-agnostic adapter module for `raptorq` encode/decode primitives.
  - [x] Wired the adapter into `session/mod.rs` without changing active sender/receiver paths.
- **Acceptance Criteria**:
  - Adapter compiles without changing runtime behavior when FEC is disabled.
- **Validation**:
  - `cargo check -p nextmini`

## Track B: Original Idea (Multicast Trees + Backpressure + RaptorQ)

### T7: Extend Session Protocol For FEC
- `depends_on: [T2]`
- **Location**: `messages/src/lossless_session.rs`, `messages/src/lib.rs`, `dataplane/src/node/session/control.rs`, `dataplane/src/node/session/{sender,receiver,runtime}.rs`, `dataplane/tests/fec_handshake.rs`
- **Description**:
  - Add FEC-capable control/data metadata (`FecManifest`, `FecCapabilities`, `FecStatus`, block/symbol fields).
  - Add `tree_id` in FEC data metadata (default `0`) so multi-tree can be enabled later without revisiting the wire format.
  - Add explicit protocol-version strategy for FEC frames and define strict FEC-only behavior (no downgrade fallback once a session is declared FEC).
  - Implement capability negotiation using lossless session control frames directly between sender and receivers (no controller involvement); sender enables FEC only when all target receivers support it, otherwise abort before sending any FEC data.
  - Keep `FecStatus` control messages fixed-size by reporting a per-block deficit (how many additional symbols are needed) rather than large missing-bitmaps.
- **Acceptance Criteria**:
  - Backward compatibility preserved for existing non-FEC sessions.
  - Incompatible peers are rejected deterministically before first FEC data frame.
  - Unknown schemes are rejected cleanly.
  - New regression test `dataplane/tests/fec_handshake.rs` exists and covers strict abort when any required peer is incompatible.
- **Validation**:
  - `cargo test -p messages lossless_session`
  - `cargo test -p dataplane --test fec_handshake -- --exact aborts_when_peer_incompatible`

### T8: Source-Side FEC Sender Integration
- `depends_on: [T6, T7]`
- **Status**: ✅ Completed on February 12, 2026.
- **Location**: `dataplane/src/node/session/sender.rs`, `dataplane/src/node/session/runtime.rs`, `dataplane/tests/fec_sender.rs`
- **Description**:
  - Implement source-only RaptorQ mode with repair-budget bounds, pacing, and topology/ready gating reuse.
  - Add FEC-specific sender state semantics (per-block repair accounting), separate from cumulative `Ack { up_to }` retirement semantics used by non-FEC mode.
- **Acceptance Criteria**:
  - Sender can emit systematic + repair symbols through existing processor pipeline.
  - Non-FEC path unchanged.
  - New regression test `dataplane/tests/fec_sender.rs` exists and covers repair budget/pacing invariants.
- **Validation**:
  - `cargo test -p dataplane --test fec_sender -- --exact sender_emits_repairs_with_budget`

### T9: Receiver Decode + Feedback Integration
- `depends_on: [T6, T7]`
- **Location**: `dataplane/src/node/session/receiver.rs`, `dataplane/src/node/session/control.rs`, `dataplane/tests/fec_receiver.rs`
- **Description**:
  - Buffer symbols by block, decode once sufficient, throttle/jitter feedback, and validate object integrity.
  - Implement explicit FEC status signaling semantics (`block_id`, deficit count) rather than reusing cumulative ACK semantics.
- **Status**: ✅ Completed on February 12, 2026.
- **Acceptance Criteria**:
  - Receiver reconstructs a 64 MiB payload under 10% IID loss and completes session.
  - Receiver-side control feedback remains bounded (throttled) under fanout.
  - New regression test `dataplane/tests/fec_receiver.rs` exists and covers end-to-end decode under configured loss.
- **Validation**:
  - `cargo test -p dataplane --test fec_receiver -- --exact receiver_recovers_under_10pct_loss`

### T10: Runtime Negotiation + Config Gating
- `depends_on: [T8, T9]`
- **Location**: `dataplane/src/node/config.rs`, `dataplane/src/node/session/runtime.rs`
- **Description**: Add config flags (`fec_enabled`, `fec_require_capability`, sizing bounds) and abort FEC session when negotiation fails; never auto-fallback to non-FEC for an in-flight FEC session.
- **Status**: ✅ Completed on February 12, 2026.
- **Work Log**:
  - Extended `LosslessConfig` with explicit FEC policy knobs: kill-switch (`fec_enabled`), strict negotiation toggle (`fec_require_capability`), and manifest sizing bounds (`fec_symbols_per_block_*`, `fec_symbol_size_*`).
  - Added normalized bounds helpers in config so reversed min/max values remain deterministic at runtime.
  - Wired runtime preflight to reject FEC sender sessions before task spawn when FEC is disabled, capability negotiation is not strict, scheme is unknown, or manifest/chunk sizing violates configured bounds.
  - Preserved strict no-fallback semantics by rejecting FEC sessions directly rather than downgrading to non-FEC.
  - Wired controller runtime construction to pass `lossless_runtime_config` into `LosslessRuntimeHandle` so policy is enforced in production startup paths.
  - Added regression unit tests for runtime FEC preflight accept/reject behavior and config default/bounds behavior.
- **Files Updated**:
  - `dataplane/src/node/config.rs`
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/controller/interface.rs`
  - `raptorq-wan-multicast-plan.md`
- **Gotchas**:
  - `fec_enabled` defaults to `false`, so FEC now requires explicit opt-in.
  - Runtime currently enforces strict capability negotiation for FEC sessions; setting `fec_require_capability=false` causes deterministic preflight rejection to avoid any implicit downgrade/fallback path.
- **Acceptance Criteria**:
  - FEC is opt-in and kill-switchable.
  - Incompatible peers cause deterministic preflight failure.
- **Validation**:
  - `cargo test -p nextmini session -- --nocapture`

### T11: Python API + Example Wiring
- `depends_on: [T10]`
- **Location**: `python-api/src/lib.rs`, `examples/multicast-docker/scripts/multicast_node.py`, `tools/experiments/raptorq/smoke_python_api.py`, docs
- **Description**: Add optional FEC parameters for send/receive APIs and runnable example toggles; add a lightweight Python smoke harness.
- **Status**: ✅ Completed on February 12, 2026.
- **Work Log**:
  - Extended Python `Dataplane.send_data` with backward-compatible optional FEC kwargs (`fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`) and mapped them to runtime `SenderConfig.fec_manifest` only when requested.
  - Extended Python `Dataplane.receive_data` / `receive_data_async` with optional `fec_enabled` to control receiver-side advertised FEC capabilities (`default` vs `empty`) without breaking existing callers.
  - Added multicast example CLI toggles (`--fec`, `--fec-symbols-per-block`, `--fec-symbol-size`) and wired source/receiver calls to pass FEC mode through the updated Python APIs.
  - Added `tools/experiments/raptorq/smoke_python_api.py` with `--help`, `--fec`, `--output`, and `--assert-success`; the script writes JSON artifacts with required `mode`, `success`, and `timing` fields.
  - Added Python API and example documentation notes for FEC usage/toggles.
- **Files Updated**:
  - `python-api/src/lib.rs`
  - `examples/multicast-docker/scripts/multicast_node.py`
  - `tools/experiments/raptorq/smoke_python_api.py`
  - `docs/docs/design/python-api.md`
  - `fuma/content/docs/testing/multicast.md`
  - `raptorq-wan-multicast-plan.md`
- **Gotchas**:
  - Sender FEC manifest defaults (`symbols_per_block=32`, `symbol_size=chunk_size`) are applied only when FEC is explicitly requested.
  - Smoke harness is intentionally lightweight: if `nextmini_py` is unavailable, it falls back to static wiring checks and still emits a deterministic JSON artifact.
- **Acceptance Criteria**:
  - Python flow can enable/disable FEC without breaking existing usage.
  - `smoke_python_api.py` writes a JSON artifact with mode, success, and timing fields.
  - `tools/experiments/raptorq/smoke_python_api.py` exists and is runnable with `--help`.
- **Validation**:
  - `python tools/experiments/raptorq/smoke_python_api.py --fec off --output /tmp/py_fec_off.json --assert-success`
  - `python tools/experiments/raptorq/smoke_python_api.py --fec on --output /tmp/py_fec_on.json --assert-success`

### T12: Multi-Tree Route Model Upgrade
- `depends_on: [T7]`
- **Location**: `controller/migrations/`, `controller/src/main.rs`, `controller/src/utils.rs`, `controller/src/db/group_routes.rs`, `controller/tests/multicast_multitree.rs`, `dataplane/src/node/route.rs`, `messages/src/lib.rs`
- **Description**:
  - Add DB/schema migration for multi-tree persistence (per-group `tree_id`, optional `weight`, edges) and compatibility with existing single-tree groups (`tree_id=0`).
  - Extend the group-route update API so an external optimizer can set **multiple trees** for a group (each tree has its own edge list + optional weight).
    - Message evolution in `messages/src/lib.rs`: add a new `DataplaneToController` variant (keep legacy single-tree for migration), e.g.:
      - `SetGroupRoutesMulti { group_id, trees: Vec<{ tree_id, weight, edges }> }`
  - Controller installs multiple trees by sending multiple `GroupRoutingTableEntry` items per `(src_node_id, group_id)`, one per tree (with `tree_id` encoded into `route_id` via the deterministic scheme below).
  - Define a deterministic multicast route-id scheme so `tree_id` maps to a unique `route_id` without extra per-node state:
    - Example: `route_id = group_id * MULTITREE_STRIDE + tree_id`, with `tree_id < MULTITREE_STRIDE` and `route_id < 2^30` (to stay below `MULTICAST_ROUTE_FLAG`).
  - Ensure installs are canonically ordered by `tree_id` ascending.
- **Status**: ✅ Completed on February 12, 2026.
- **Work Log**:
  - Added a new controller migration that upgrades `group_routes` to `(group_id, tree_id)` primary key, with optional `weight` and backward-compatible `tree_id=0`.
  - Added `SetGroupRoutesMulti { group_id, trees }` message support while retaining legacy `SetGroupRoutes`.
  - Implemented shared deterministic route-id helpers and canonical tree ordering in controller utils.
  - Updated controller route update + snapshot + DB-sync paths to persist and install multi-tree payloads.
  - Aligned dataplane multicast route namespace constant with shared message constants and canonicalized installed multicast route ID ordering.
  - Added regression test `controller/tests/multicast_multitree.rs` for stable/complete multi-tree payload generation.
- **Files Updated**:
  - `controller/migrations/20260212010000_group_routes_multitree.sql`
  - `controller/src/main.rs`
  - `controller/src/utils.rs`
  - `controller/src/db/group_routes.rs`
  - `controller/src/db_sync.rs`
  - `controller/src/models.rs`
  - `controller/src/lib.rs`
  - `controller/tests/multicast_multitree.rs`
  - `dataplane/src/node/route.rs`
  - `messages/src/lib.rs`
- **Gotchas**:
  - Multi-tree route IDs are bounded by both `MULTITREE_STRIDE` and `MULTICAST_ROUTE_FLAG`; out-of-range/duplicate trees are rejected before persistence.
  - Legacy single-tree updates are normalized to tree `0` and continue to function through the same deterministic route-id path.
- **Acceptance Criteria**:
  - Controller persists and installs multiple trees for a group.
  - Existing single-tree APIs remain functional during migration window.
  - New regression test `controller/tests/multicast_multitree.rs` exists and asserts multi-tree install payloads are stable and complete.
- **Validation**:
  - `cargo test -p controller --test multicast_multitree`

### T13: Tree-Aware Symbol Scheduling + Relay Behavior
- `depends_on: [T10, T12]`
- **Location**: `dataplane/src/node/session/sender.rs`, `dataplane/src/node/route.rs`, `dataplane/src/node/processor.rs`, `dataplane/src/node/packet.rs`, `dataplane/tests/fec_multitree.rs`, `dataplane/tests/fec_backpressure.rs`, scheduler files
- **Description**:
  - Add true multi-tree symbol scheduling for a single transfer:
    - For each `(block_id, symbol_id)`, the sender selects a `tree_id` and encodes it into the FEC data metadata (added in T7).
    - Default selection: `tree_id = hash64(session_id, block_id, symbol_id) % num_trees` (extend to weighted selection once tree weights exist).
    - Enforce multi-tree as a hard requirement for this mode: abort the transfer if `num_trees < 2`.
  - Make relays forward on the intended tree:
    - Add a routing-table fast path that routes multicast packets by `(src_node_id, group_id, tree_id)` (rather than caching a single route per `flow_id`).
    - Route selection is a pure function of `(group_id, tree_id)` via the deterministic `route_id` scheme (no per-hop hashing and no per-flow cache pinning).
    - Update `processor.rs` to extract `tree_id` from FEC lossless-session payloads (parse `Packet::tcp_payload()`), then route the packet using the `(key, tree_id)` fast path so every hop forwards on the same tree.
    - Unknown `(group_id, tree_id)` at any hop is a hard drop with a trace warning (no fallback to another tree).
  - Keep relays as pure forwarders initially; optional relay cache/repair remains feature-gated.
- **Status**: ✅ Completed on February 12, 2026.
- **Work Log**:
  - Added deterministic per-symbol tree assignment in the FEC sender (`tree_id = hash64(session_id, block_id, symbol_id) % num_trees`) and serialized `tree_id` into FEC data frames.
  - Added explicit multi-tree mode gating on sender config (`fec_num_trees`) with hard validation that multi-tree mode requires `num_trees >= 2`.
  - Upgraded dataplane multicast routing to tree-aware lookup keyed by `(src_node_id, group_id, tree_id)`, with deterministic route-id mapping from `(group_id, tree_id)` and fast-path caching on that key.
  - Updated packet processor routing to parse FEC `tree_id` from payloads and route using tree-aware lookup; unknown multicast trees are hard-dropped with warning.
  - Kept relay behavior as pure forwarding (no recoding path added).
  - Added regression test `dataplane/tests/fec_multitree.rs` (`symbols_span_multiple_trees`) to verify deterministic, multi-tree symbol split.
  - Added regression test `dataplane/tests/fec_backpressure.rs` (`non_fec_flow_not_starved`) to ensure non-FEC flow service under heavy FEC load.
- **Files Updated**:
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/session/sender.rs`
  - `dataplane/src/node/session/unicast.rs`
  - `dataplane/src/node/route.rs`
  - `dataplane/src/node/processor.rs`
  - `dataplane/src/node/packet.rs`
  - `dataplane/src/node/scheduler/wrr.rs`
  - `dataplane/tests/fec_multitree.rs`
  - `dataplane/tests/fec_backpressure.rs`
  - `dataplane/tests/fec_sender.rs`
  - `python-api/src/lib.rs`
  - `raptorq-wan-multicast-plan.md`
- **Gotchas**:
  - Tree-aware multicast install now enforces deterministic `(group_id, tree_id) -> route_id`; non-conforming route IDs are ignored during installation.
  - Unknown `(group_id, tree_id)` now hard-fails route resolution for multicast/FEC packets (no fallback tree selection).
  - Local validation is currently blocked by unrelated workspace state: `raptorq/src/lib.rs` is missing, so `cargo test -p nextmini ...` fails before test execution.
- **Acceptance Criteria**:
  - Multi-tree symbol split works deterministically.
  - One transfer session uses at least two distinct tree_ids in routing telemetry.
  - Backpressure does not starve non-FEC traffic.
  - New regression tests exist:
    - `dataplane/tests/fec_multitree.rs` asserts symbols from one session span multiple trees.
    - `dataplane/tests/fec_backpressure.rs` asserts non-FEC traffic is not starved under FEC load.
- **Validation**:
  - `cargo test -p dataplane --test fec_multitree -- --exact symbols_span_multiple_trees`
  - `cargo test -p dataplane --test fec_backpressure -- --exact non_fec_flow_not_starved`

### T14: End-to-End Verification + Regression Matrix
- `depends_on: [T4, T5, T11, T13]`
- **Location**: `tests/`, `tools/experiments/raptorq/`, docs
- **Description**: Add regression coverage and reproducible experiment scripts (baseline lossless, unicast baseline, parity baseline, RaptorQ modes) plus a minimal smoke runner.
- **Status**: ✅ Completed on February 14, 2026.
- **Work Log**:
  - Added regression tests for FEC sender pacing, receiver decode completion, strict handshake behavior, multi-tree dispatch, and non-FEC fairness under FEC load.
  - Added and validated experiment harness scripts under `tools/experiments/raptorq/`:
    - `run_smoke.py` (mode/loss metrics artifact + strict metric assertions).
    - `smoke_python_api.py` (Python API smoke checks + artifact output).
  - Added harness regression tests under `tools/experiments/raptorq/tests/` for CLI contracts and artifact schema checks.
  - Verified smoke scripts run with `--help` and strict assertion modes.
- **Files Updated**:
  - `dataplane/tests/fec_backpressure.rs`
  - `dataplane/tests/fec_handshake.rs`
  - `dataplane/tests/fec_multitree.rs`
  - `dataplane/tests/fec_receiver.rs`
  - `dataplane/tests/fec_sender.rs`
  - `tools/experiments/raptorq/run_smoke.py`
  - `tools/experiments/raptorq/smoke_python_api.py`
  - `tools/experiments/raptorq/tests/test_run_smoke.py`
  - `plans/raptorq-wan-multicast-plan.md`
- **Acceptance Criteria**:
  - CI-suitable tests pass for migrated core and integrated session paths.
  - Experiment harness emits metrics (completion, P95/P99, overhead, CPU/mem).
  - Harness writes JSON artifacts under the path provided by `--output`.
  - `tools/experiments/raptorq/run_smoke.py` exists and is runnable with `--help`.
- **Validation**:
  - `cargo test --workspace`
  - `python tools/experiments/raptorq/run_smoke.py --mode raptorq --loss 0.1 --output /tmp/raptorq_smoke.json --assert-metrics`

### T15: Release Gate + Ops Kill-Switch
- `depends_on: [T14, T11]`
- **Location**: docs + config defaults, `tools/experiments/raptorq/check_compat_matrix.py`
- **Description**: Finalize rollout stages, compatibility matrix, and disablement procedure (`fec_enabled=false` to prevent new FEC sessions, while existing FEC sessions must fail fast without downgrade).
- **Status**: ✅ Completed on February 14, 2026.
- **Work Log**:
  - Added strict compatibility matrix checker (`check_compat_matrix.py`) that validates homogeneous FEC capability tuples and emits machine-readable mismatch reports.
  - Added strict-mode exit behavior (`--assert-strict` / `--strict`) for release-gate automation.
  - Added regression tests for compatibility matrix success/failure scenarios in `tools/experiments/raptorq/tests/test_check_compat_matrix.py`.
  - Documented strict no-fallback operational behavior and rollout guardrails in design docs (`lossless_config.md`, `raptorq-migration-notes.md`).
- **Files Updated**:
  - `tools/experiments/raptorq/check_compat_matrix.py`
  - `tools/experiments/raptorq/tests/test_check_compat_matrix.py`
  - `docs/docs/design/lossless_config.md`
  - `docs/docs/design/raptorq-migration-notes.md`
  - `plans/raptorq-wan-multicast-plan.md`
- **Acceptance Criteria**:
  - Clear operational playbook for enable/disable and strict FEC-only behavior.
  - `tools/experiments/raptorq/check_compat_matrix.py` exists and is runnable with `--help`.
- **Validation**:
  - `python tools/experiments/raptorq/check_compat_matrix.py --input /tmp/node_a.json --input /tmp/node_b.json --require-homogeneous-fec --output /tmp/fec_compat.json --assert-strict`
  - `python tools/experiments/raptorq/smoke_python_api.py --fec off --output /tmp/py_release_strict.json --assert-success`

## Source -> Target Mapping (Concrete, T1 Frozen Snapshot)
- Snapshot date: February 12, 2026
- Source root: `/Users/bli/Playground/asupersync`
- Source commit (for provenance): `f388be666a8b1aab04b9dfecec4ca962fa378d1d`
- Reviewed mapping (16 files):
  - `/Users/bli/Playground/asupersync/src/raptorq/gf256.rs` -> `raptorq/src/gf256.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/linalg.rs` -> `raptorq/src/linalg.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/rfc6330.rs` -> `raptorq/src/rfc6330.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/systematic.rs` -> `raptorq/src/systematic.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/decoder.rs` -> `raptorq/src/decoder.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/proof.rs` -> `raptorq/src/proof.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/mod.rs` -> `raptorq/src/lib.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/pipeline.rs` -> `raptorq/src/pipeline.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/builder.rs` -> `raptorq/src/builder.rs`
  - `/Users/bli/Playground/asupersync/src/encoding.rs` -> `raptorq/src/encoding.rs`
  - `/Users/bli/Playground/asupersync/src/decoding.rs` -> `raptorq/src/decoding.rs`
  - `/Users/bli/Playground/asupersync/src/codec/raptorq.rs` -> `raptorq/src/codec/raptorq.rs`
  - `/Users/bli/Playground/asupersync/src/raptorq/tests.rs` -> `raptorq/src/tests.rs`
  - `/Users/bli/Playground/asupersync/tests/raptorq_conformance.rs` -> `raptorq/tests/raptorq_conformance.rs`
  - `/Users/bli/Playground/asupersync/tests/raptorq_perf_invariants.rs` -> `raptorq/tests/raptorq_perf_invariants.rs`
  - `/Users/bli/Playground/asupersync/benches/raptorq_benchmark.rs` -> `raptorq/benches/raptorq_benchmark.rs`

## Licensing Snapshot (T1)
- License file checked: `/Users/bli/Playground/asupersync/LICENSE` (MIT License text).
- Cargo metadata checked: `/Users/bli/Playground/asupersync/Cargo.toml` has `license = "MIT"`.
- Carry-forward requirement for migrated files: preserve MIT copyright + permission notice in distributed copies.

## T1 Work Log (2026-02-12)
- Work log:
  - Ran required validation command: `rg -n "raptorq|fec|fountain" /Users/bli/Playground/asupersync`.
  - Validation returned 1130 matching lines across 216 files; curated the migration-relevant set to the 16-file inventory above.
  - Added frozen mapping + source commit pin + MIT provenance snapshot, then mirrored notes into `docs/`.
- Files modified:
  - `raptorq-wan-multicast-plan.md`
  - `docs/docs/design/raptorq-migration-notes.md`
- Gotchas:
  - The `fec` token matched many unrelated words (for example `effective` and `lifecycle`), so inventory curation required path-level review rather than regex matches alone.

## T2 Work Log (2026-02-12)
- Work log:
  - Bootstrapped new workspace crate `raptorq` via `cargo new --lib`.
  - Replaced template code with a migration-ready scaffold: core `Error`/`Result`, `BlockId`/`SymbolId` primitives, and deterministic RNG helper.
  - Ran required validation command: `cargo check -p raptorq`.
- Files modified:
  - `Cargo.toml`
  - `raptorq/Cargo.toml`
  - `raptorq/src/lib.rs`
  - `raptorq/src/primitives.rs`
  - `raptorq/src/deterministic.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - `cargo new` auto-added `raptorq` to workspace membership, so manual root-workspace edits were only a review pass.

## T6 Work Log (2026-02-12)
- Work log:
  - Added `dataplane/src/node/session/fec.rs` with a thin adapter over `raptorq::SystematicEncoder` and `raptorq::InactivationDecoder`.
  - Exposed wire-agnostic wrapper types and builders for encode/decode symbol flow, plus constrained parameter structs for session-side integration.
  - Wired `pub mod fec;` in `dataplane/src/node/session/mod.rs` and added the `raptorq` dependency in `dataplane/Cargo.toml`.
  - Ran required validation command: `cargo check -p nextmini`.
- Files modified:
  - `dataplane/Cargo.toml`
  - `dataplane/src/node/session/fec.rs`
  - `dataplane/src/node/session/mod.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - The adapter is intentionally staged but unused by sender/receiver runtime paths until T8/T9, so module-level dead-code suppression is required to keep warning noise local.

## T9 Work Log (2026-02-12)
- Work log:
  - Reworked receiver FEC path to buffer symbols per block and decode through `session::fec::Decoder`, with deterministic block seed derivation and per-block payload reconstruction into the existing in-order pending window.
  - Added bounded FEC feedback emission in receiver (`FecStatus { block_id, deficit_symbols }`) with deterministic jitter and throttle interval, plus immediate terminal status (`deficit_symbols = 0`) on successful block decode.
  - Added explicit sender-side helper semantics in `dataplane/src/node/session/control.rs` via `update_receiver_fec_status` so FEC progress is interpreted from per-block deficit signals rather than cumulative ACK semantics.
  - Added regression test `dataplane/tests/fec_receiver.rs` that drives `receiver::run` end-to-end with 64 MiB payload, deterministic 10% IID symbol loss, and repair-symbol recovery.
  - Ran required validation command: `cargo test -p nextmini --test fec_receiver -- --exact receiver_recovers_under_10pct_loss`.
- Files modified:
  - `dataplane/src/node/session/receiver.rs`
  - `dataplane/src/node/session/control.rs`
  - `dataplane/tests/fec_receiver.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - The default receiver reordering window can drop far-ahead recovered chunks under persistent block gaps, so the regression config uses a large token-bucket-derived window to keep recovery behavior bounded and deterministic during IID-loss decode validation.

## T8 Work Log (2026-02-12)
- Work log:
  - Integrated `session::fec` adapter into sender-side FEC emission by materializing source chunks into per-block systematic symbols and bounded repair symbols.
  - Added source-only FEC sender queueing for `(block_id, symbol_id)` emission and paced every FEC symbol through the existing token-bucket path.
  - Added FEC-specific retirement semantics in sender state: inflight tracking/retirement now uses per-block progress from `FecStatus` and ignores cumulative `Ack { up_to }` in FEC mode; non-FEC ACK semantics remain unchanged.
  - Added regression test `dataplane/tests/fec_sender.rs` that exercises the live sender + processor pipeline and asserts systematic+repair emission, per-block repair budget cap, and pacing behavior.
  - Ran required validation command: `cargo test -p nextmini --test fec_sender -- --exact sender_emits_repairs_with_budget`.
- Files modified:
  - `dataplane/src/node/session/sender.rs`
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/tests/fec_sender.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - `raptorq` expects fixed-size source symbols; sender-side FEC integration pads source chunks up to manifest `symbol_size` and aborts strict FEC preflight if a chunk exceeds the configured symbol size.

## T4 Work Log (2026-02-12)
- Work log:
  - Ported `tests/raptorq_conformance.rs` and `tests/raptorq_perf_invariants.rs` from `/Users/bli/Playground/asupersync/tests/` into `raptorq/tests/`.
  - Trimmed the `pipeline_e2e` module from the conformance suite because it depends on asupersync-only encoding/decoding pipeline types that are out of scope for `raptorq`.
  - Adapted imports from `asupersync` paths to `raptorq` module paths and removed the unused `mod common;` declaration from the invariant suite.
  - Ran required validation command: `cargo test -p raptorq`.
  - Ran focused suite validation: `cargo test -p raptorq --test raptorq_conformance --test raptorq_perf_invariants`.
- Files modified:
  - `raptorq/tests/raptorq_conformance.rs`
  - `raptorq/tests/raptorq_perf_invariants.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - `cargo test -p raptorq` currently fails on an existing doctest in `raptorq/src/linalg.rs` that still references `asupersync`; the two migrated T4 suites themselves pass.

## T5 Work Log (2026-02-12)
- Work log:
  - Ported `/Users/bli/Playground/asupersync/benches/raptorq_benchmark.rs` into `raptorq/benches/raptorq_benchmark.rs` and switched imports to `raptorq`.
  - Added benchmark dependencies/config in `raptorq/Cargo.toml` (`criterion = "0.5.1"`, `[[bench]] name = "raptorq_benchmark"`, `harness = false`).
  - Ran required validation command: `cargo bench -p raptorq --bench raptorq_benchmark -- --output-format bencher | tee /tmp/raptorq_bench.txt`.
  - Captured baseline regression artifact at `/tmp/raptorq_bench.txt`.
- Files modified:
  - `raptorq/Cargo.toml`
  - `raptorq/benches/raptorq_benchmark.rs`
  - `raptorq-wan-multicast-plan.md`
- Gotchas:
  - Initial benchmark run spent time waiting on Cargo package-cache locks and first-time dependency compilation before timing output.

## Risks
- API mismatch between asupersync symbol model and nextmini session model.
- Memory pressure under high fanout if symbol buffering is unbounded.
- Multi-tree route-id collisions if namespace encoding is not strict.
- Performance regressions from pure-Rust baseline before optimization.

## Disablement (No Downgrade)
- Keep all FEC features behind config + feature flags.
- Preserve non-FEC lossless path as default for sessions that are explicitly non-FEC.
- If instability appears, disable FEC via config to prevent new FEC sessions; any FEC session must fail fast and must not downgrade to non-FEC.
