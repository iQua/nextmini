# Plan: RaptorQ Migration + Multicast-Tree Integration In Nextmini

**Generated**: February 12, 2026
**Source Repo**: `/Users/winifred/asupersync`
**Target Repo**: `/Users/winifred/nextmini`
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
- T3 -> T4, T6
- T4 -> T14
- T5 -> T14
- T6 -> T8, T9
- T7 -> T8, T9, T12
- T8 -> T10
- T9 -> T10
- T10 -> T11, T13
- T11 -> T14
- T12 -> T13
- T13 -> T14
- T14 -> T15

## Track A: Migration (Bring All RaptorQ Code + Tests)

### T1: Freeze Source Inventory + Licensing Snapshot
- `depends_on: []`
- **Location**: `raptorq-wan-multicast-plan.md` (mapping section), `docs/` migration notes
- **Description**: Record exact source file list, target destination list, and license notes before code moves.
- **Acceptance Criteria**:
  - Complete source->target mapping exists and is reviewed.
  - MIT license provenance from `asupersync` is documented.
- **Validation**:
  - `rg -n "raptorq|fec|fountain" /Users/winifred/asupersync`

### T2: Create New Workspace Crate For Migrated Core
- `depends_on: [T1]`
- **Location**: `fec-raptorq/`, root `Cargo.toml`
- **Description**: Add a dedicated crate for migrated RaptorQ primitives and deterministic helpers.
- **Acceptance Criteria**:
  - New crate builds in workspace.
  - No dataplane integration yet.
- **Validation**:
  - `cargo check -p fec-raptorq`

### T3: Port Core Algorithm Modules
- `depends_on: [T2]`
- **Location**: `fec-raptorq/src/*.rs`
- **Description**: Port `gf256`, `linalg`, `rfc6330`, `systematic`, `decoder`, `proof`, and deterministic RNG helpers.
- **Acceptance Criteria**:
  - Core encode/decode APIs compile and expose a stable Rust interface for nextmini session adapters.
- **Validation**:
  - `cargo check -p fec-raptorq`

### T4: Port RaptorQ Test Suites
- `depends_on: [T3]`
- **Location**: `fec-raptorq/tests/`
- **Description**: Migrate conformance and invariant tests from `asupersync` and adapt imports to new crate paths.
- **Acceptance Criteria**:
  - Migrated conformance/invariant tests pass in new crate.
- **Validation**:
  - `cargo test -p fec-raptorq`

### T5: Port Benchmark Harness (Optional But Planned)
- `depends_on: [T3]`
- **Location**: `fec-raptorq/benches/`
- **Description**: Port baseline RaptorQ benchmark to detect performance regressions after integration.
- **Acceptance Criteria**:
  - Benchmark target compiles and runs.
- **Validation**:
  - `cargo bench -p fec-raptorq --bench raptorq_benchmark`

### T6: Add Dataplane Adapter Layer
- `depends_on: [T3]`
- **Location**: `dataplane/src/node/session/fec.rs` (new), `dataplane/src/node/session/mod.rs`
- **Description**: Add a thin adapter that maps nextmini session blocks/chunks to `fec-raptorq` encode/decode APIs.
- **Acceptance Criteria**:
  - Adapter compiles without changing runtime behavior when FEC is disabled.
- **Validation**:
  - `cargo check -p dataplane`

## Track B: Original Idea (Multicast Trees + Backpressure + RaptorQ)

### T7: Extend Session Protocol For FEC
- `depends_on: [T2]`
- **Location**: `messages/src/lossless_session.rs`, `messages/src/lib.rs`
- **Description**: Add FEC-capable control/data metadata (`FecManifest`, `FecCapabilities`, `FecStatus`, block/symbol fields).
- **Acceptance Criteria**:
  - Backward compatibility preserved for existing non-FEC sessions.
  - Unknown schemes are rejected cleanly.
- **Validation**:
  - `cargo test -p messages lossless_session`

### T8: Source-Side FEC Sender Integration
- `depends_on: [T6, T7]`
- **Location**: `dataplane/src/node/session/sender.rs`, `dataplane/src/node/session/runtime.rs`
- **Description**: Implement source-only RaptorQ mode with repair-budget bounds, pacing, and topology/ready gating reuse.
- **Acceptance Criteria**:
  - Sender can emit systematic + repair symbols through existing processor pipeline.
  - Non-FEC path unchanged.
- **Validation**:
  - `cargo test -p dataplane sender -- --nocapture`

### T9: Receiver Decode + Feedback Integration
- `depends_on: [T6, T7]`
- **Location**: `dataplane/src/node/session/receiver.rs`, `dataplane/src/node/session/control.rs`
- **Description**: Buffer symbols by block, decode once sufficient, throttle/jitter feedback, and validate object integrity.
- **Acceptance Criteria**:
  - Receiver reconstructs payload under controlled loss and completes session.
- **Validation**:
  - `cargo test -p dataplane receiver -- --nocapture`

### T10: Runtime Negotiation + Fallback + Config Gating
- `depends_on: [T8, T9]`
- **Location**: `dataplane/src/node/config.rs`, `dataplane/src/node/session/runtime.rs`
- **Description**: Add config flags (`fec_enabled`, `fec_require_capability`, sizing bounds) and fallback to classic lossless when negotiation fails.
- **Acceptance Criteria**:
  - FEC is opt-in and kill-switchable.
  - Mixed-version fallback path is explicit.
- **Validation**:
  - `cargo test -p dataplane session -- --nocapture`

### T11: Python API + Example Wiring
- `depends_on: [T10]`
- **Location**: `python-api/src/lib.rs`, `examples/multicast-docker/scripts/multicast_node.py`, docs
- **Description**: Add optional FEC parameters for send/receive APIs and runnable example toggles.
- **Acceptance Criteria**:
  - Python flow can enable/disable FEC without breaking existing usage.
- **Validation**:
  - Python smoke run of multicast example with FEC on and off.

### T12: Multi-Tree Route Model Upgrade
- `depends_on: [T7]`
- **Location**: `controller/src/main.rs`, `controller/src/utils.rs`, `controller/src/db/group_routes.rs`, `dataplane/src/node/route.rs`, `messages/src/lib.rs`
- **Description**: Support multiple trees per group (tree_id/weight), route-id namespace safety, and per-node install payload updates.
- **Acceptance Criteria**:
  - Controller persists and installs multiple trees for a group.
- **Validation**:
  - `cargo test -p controller`

### T13: Tree-Aware Symbol Scheduling + Relay Behavior
- `depends_on: [T10, T12]`
- **Location**: `dataplane/src/node/session/sender.rs`, `dataplane/src/node/processor.rs`, scheduler files
- **Description**: Distribute symbols across trees and keep relays forwarding; optional relay cache/repair remains feature-gated.
- **Acceptance Criteria**:
  - Multi-tree symbol split works deterministically.
  - Backpressure does not starve non-FEC traffic.
- **Validation**:
  - Targeted dataplane tests + multicast integration scenario.

### T14: End-to-End Verification + Regression Matrix
- `depends_on: [T4, T5, T11, T13]`
- **Location**: `tests/`, `tools/experiments/`, docs
- **Description**: Add regression coverage and reproducible experiment scripts (baseline lossless, unicast baseline, parity baseline, RaptorQ modes).
- **Acceptance Criteria**:
  - CI-suitable tests pass for migrated core and integrated session paths.
  - Experiment harness emits metrics (completion, P95/P99, overhead, CPU/mem).
- **Validation**:
  - `cargo test --workspace`
  - experiment smoke command documented and runnable.

### T15: Release Gate + Rollback Artifacts
- `depends_on: [T14]`
- **Location**: docs + config defaults
- **Description**: Finalize rollout stages, compatibility matrix, and rollback procedure (`fec_enabled=false` path).
- **Acceptance Criteria**:
  - Clear operational playbook for enable/disable and mixed-version behavior.
- **Validation**:
  - Checklist review in docs; dry-run toggling FEC in example deployment.

## Source -> Target Mapping (Concrete)
- `asupersync/src/raptorq/gf256.rs` -> `fec-raptorq/src/gf256.rs`
- `asupersync/src/raptorq/linalg.rs` -> `fec-raptorq/src/linalg.rs`
- `asupersync/src/raptorq/rfc6330.rs` -> `fec-raptorq/src/rfc6330.rs`
- `asupersync/src/raptorq/systematic.rs` -> `fec-raptorq/src/systematic.rs`
- `asupersync/src/raptorq/decoder.rs` -> `fec-raptorq/src/decoder.rs`
- `asupersync/src/raptorq/proof.rs` -> `fec-raptorq/src/proof.rs`
- `asupersync/tests/raptorq_conformance.rs` -> `fec-raptorq/tests/conformance.rs`
- `asupersync/tests/raptorq_perf_invariants.rs` -> `fec-raptorq/tests/perf_invariants.rs`
- `asupersync/benches/raptorq_benchmark.rs` -> `fec-raptorq/benches/raptorq_benchmark.rs`

## Risks
- API mismatch between asupersync symbol model and nextmini session model.
- Memory pressure under high fanout if symbol buffering is unbounded.
- Multi-tree route-id collisions if namespace encoding is not strict.
- Performance regressions from pure-Rust baseline before optimization.

## Rollback
- Keep all FEC features behind config + feature flags.
- Preserve non-FEC lossless path as default.
- If instability appears, disable FEC via config and continue with existing multicast/lossless pipeline.
