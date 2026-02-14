# Plan: Refactor Dataplane Python API To Be FEC-Oblivious

**Generated**: February 14, 2026  
**Target Repo**: `/Users/bli/Playground/nextmini`  
**Primary Goal**: remove all FEC-specific knobs/types from the Python dataplane API so FEC remains an internal runtime implementation detail.

## Problem Summary

Today, the Python extension exposes FEC internals directly:
- `python-api/src/lib.rs` `Dataplane::send_data` exposes `fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`, `fec_tree_ids`.
- `python-api/src/lib.rs` `Dataplane::receive_data` and `receive_data_async` expose `fec_enabled`.
- `python-api/src/lib.rs` imports and constructs `FecManifest` / `FecCapabilities` via `sender_fec_manifest`, `sender_fec_tree_ids`, `receiver_fec_capabilities`.
- Docs/examples/tooling teach users to pass FEC arguments (`docs/docs/design/python-api.md`, `examples/multicast-docker/scripts/multicast_node.py`, `tools/experiments/raptorq/smoke_python_api.py`).

This breaks encapsulation: the Python layer currently owns policy/validation that should live in dataplane runtime internals.

## Success Criteria

1. Python API signatures contain no FEC-specific parameters.
2. `python-api/src/lib.rs` no longer imports or constructs `FecManifest` / `FecCapabilities`.
3. FEC selection, manifest derivation, capability handling, and tree-ID policy are decided inside dataplane runtime/config code.
4. Existing non-FEC Python call flows continue working with no callsite changes beyond removed FEC kwargs.
5. FEC regression tests still pass (behavior preserved), and new regression coverage prevents re-exposing FEC in Python API.

## Target Design

- Python API becomes transfer-oriented only (`send_data`, `receive_data`, `receive_data_async` accept transport/session inputs, not codec internals).
- Dataplane runtime introduces an internal policy boundary that derives sender/receiver FEC behavior from runtime config + session metadata.
- Runtime preflight keeps strict validation and all FEC invariants, but callers only see generic session-start errors.
- Documentation shifts FEC guidance from Python callsite knobs to runtime/configuration-level behavior.

## Dependency Graph

- `T1 -> T2`
- `T2 -> T3`
- `T2 -> T4`
- `T3 -> T5`
- `T4 -> T5`
- `T5 -> T6`
- `T5 -> T7`
- `T7 -> T8`
- `T7 -> T9`
- `T6 -> T10`
- `T8 -> T10`
- `T9 -> T10`
- `T10 -> T11`

## Task Plan

### T1 — Baseline Inventory + Behavior Snapshot
- `depends_on: []`
- Scope:
  - Freeze current Python FEC surface and current runtime preflight behavior.
  - Capture all FEC-exposed callsites in docs/examples/tooling.
- Files:
  - `python-api/src/lib.rs`
  - `docs/docs/design/python-api.md`
  - `examples/multicast-docker/scripts/multicast_node.py`
  - `tools/experiments/raptorq/smoke_python_api.py`
- Deliverables:
  - Short checklist of existing API parameters and expected error semantics to preserve.
- Validation:
  - `rg -n "fec_enabled|fec_symbols_per_block|fec_symbol_size|fec_tree_ids" python-api/src/lib.rs docs/docs/design/python-api.md examples tools`
- Status:
  - Completed on February 14, 2026.
- Work log:
  - Captured current Python API FEC surface in `python-api/src/lib.rs:231`, `python-api/src/lib.rs:333`, `python-api/src/lib.rs:396`:
    - `send_data(..., fec_enabled=None, fec_symbols_per_block=None, fec_symbol_size=None, fec_tree_ids=None)`
    - `receive_data(..., fec_enabled=None)`
    - `receive_data_async(..., fec_enabled=None)`
  - Captured Python-side FEC mapping helpers in `python-api/src/lib.rs:969`, `python-api/src/lib.rs:1020`, `python-api/src/lib.rs:1040`:
    - `sender_fec_manifest(...)` constructs `FecManifest` with validation.
    - `sender_fec_tree_ids(...)` enforces FEC tree-id policy.
    - `receiver_fec_capabilities(...)` maps `fec_enabled` to `FecCapabilities`.
  - Captured current runtime preflight/error behavior:
    - Generic sender validation errors (`PyRuntimeError`): `receiver_ids must contain at least one entry.`, `chunk_size must be positive.`, `buffer is empty; nothing to transmit.`, `invalid congestion control: {mode}` from `python-api/src/lib.rs:249`, `python-api/src/lib.rs:255`, `python-api/src/lib.rs:260`, `python-api/src/lib.rs:273`.
    - Sender FEC-specific validation errors (`PyRuntimeError`) from `python-api/src/lib.rs:975` and `python-api/src/lib.rs:1024`:
      - `fec_enabled=False cannot be combined with fec_symbols_per_block or fec_symbol_size.`
      - `fec_symbols_per_block must be positive.`
      - `chunk_size {chunk_size} exceeds default FEC symbol_size range; set fec_symbol_size explicitly.`
      - `fec_symbol_size must be positive.`
      - `chunk_size ({chunk_size}) cannot exceed fec_symbol_size ({symbol_size}).`
      - `fec_tree_ids requires FEC; set fec_enabled=True (or FEC sizing kwargs).`
      - `fec_tree_ids must be non-empty for FEC sessions.`
      - `fec_tree_ids must be provided explicitly for FEC sessions.`
    - Sender runtime preflight rejection wrapping preserved as `lossless sender preflight rejected session {sid}: {err}` in `python-api/src/lib.rs:312`.
    - Receiver-side validation errors (`PyRuntimeError`) captured in `python-api/src/lib.rs:347`, `python-api/src/lib.rs:351`, `python-api/src/lib.rs:409`, `python-api/src/lib.rs:412`: `expected_bytes must be positive.` and `chunk_size must be positive.`
  - Captured FEC-exposed docs/examples/tooling callsites:
    - Docs teach FEC kwargs in `docs/docs/design/python-api.md:85`, `docs/docs/design/python-api.md:94`, `docs/docs/design/python-api.md:95`, `docs/docs/design/python-api.md:103`.
    - Example CLI and callsites in `examples/multicast-docker/scripts/multicast_node.py:41`, `examples/multicast-docker/scripts/multicast_node.py:47`, `examples/multicast-docker/scripts/multicast_node.py:57`, `examples/multicast-docker/scripts/multicast_node.py:67`, `examples/multicast-docker/scripts/multicast_node.py:370`, `examples/multicast-docker/scripts/multicast_node.py:421`.
    - Smoke harness explicitly asserts FEC kwargs/helpers in `tools/experiments/raptorq/smoke_python_api.py:115`, `tools/experiments/raptorq/smoke_python_api.py:122`, `tools/experiments/raptorq/smoke_python_api.py:129`, `tools/experiments/raptorq/smoke_python_api.py:136`, `tools/experiments/raptorq/smoke_python_api.py:179`, `tools/experiments/raptorq/smoke_python_api.py:188`, and runtime probe calls `send_data(..., fec_enabled=False)` at `tools/experiments/raptorq/smoke_python_api.py:218`.
- T1 checklist (baseline to preserve/migrate deliberately):
  - Existing API params currently exposed:
    - Sender: `fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`, `fec_tree_ids`.
    - Receiver sync/async: `fec_enabled`.
  - Existing user-visible validation/error semantics:
    - Keep non-FEC input checks and error text stability where feasible (`receiver_ids`, `chunk_size`, `expected_bytes`, empty buffer, invalid congestion).
    - Preserve sender preflight rejection framing (`lossless sender preflight rejected session {sid}: ...`) unless intentionally revised in later tasks.
    - Preserve FEC invariant enforcement semantics when moved into runtime (conflicting toggle/size args, symbol bounds, chunk/symbol relation, FEC tree-id requirements).
- Errors/gotchas:
  - `receive_data` and `receive_data_async` currently call `start_receiver` without a sender-style `map_err` preflight wrapper (`python-api/src/lib.rs:383`, `python-api/src/lib.rs:457`), so receiver rejection semantics are asymmetric today.
  - `receive_data_async` defers `dest_ip` parse into the async future (`python-api/src/lib.rs:433`), so invalid IP errors surface on await rather than at method call time.
  - `tools/experiments/raptorq/smoke_python_api.py` is intentionally coupled to current FEC kwargs and will fail once T7 lands unless updated in T8/T10.

### T2 — Define FEC-Oblivious Public Contract
- `depends_on: [T1]`
- Scope:
  - Define the new Python API signatures (remove `fec_*` kwargs from sender/receiver methods).
  - Define compatibility/migration behavior (breaking change with explicit docs, or short-lived compatibility shim).
  - Define invariant: Python cannot supply or override manifest/capability/tree-ID internals.
- Files:
  - `python-api/src/lib.rs`
  - `docs/docs/design/python-api.md` (contract section)
- Deliverables:
  - Final method signatures and migration note approved in plan/doc.
- Validation:
  - Signature checklist documented before code edits.
- Signature checklist (target contract):
  - `send_data(group_id, dest_ip, receiver_ids, buffer, *, chunk_size=8500, src_port=None, dst_port=None, congestion=None) -> int`
  - `receive_data(group_id, dest_ip, source_node_id, expected_bytes, *, chunk_size=8500, src_port=None, dst_port=None) -> int`
  - `receive_data_async(group_id, dest_ip, source_node_id, expected_bytes, *, chunk_size=8500, src_port=None, dst_port=None) -> Awaitable[int]`
- Contract decisions:
  - Migration mode: explicit breaking change, no compatibility shim for removed `fec_*` kwargs.
  - Legacy keyword behavior: passing `fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`, or `fec_tree_ids` raises Python `TypeError` (unexpected keyword argument) once T7 lands.
  - Invariant boundary: Python callsites cannot provide or override `FecManifest`, `FecCapabilities`, tree IDs, or any equivalent FEC policy internals.
  - Runtime ownership: FEC enablement, manifest/capability derivation, validation, and tree-ID policy are runtime/config responsibilities (T3/T4/T5).
- Status:
  - Completed on February 14, 2026.
- Work log:
  - Confirmed current public signatures in `python-api/src/lib.rs` still include `fec_*` kwargs; used this as baseline for the target contract checklist.
  - Locked final FEC-oblivious signatures and migration policy wording in this plan for downstream tasks T3/T4/T7/T8/T9/T10.
  - Added explicit boundary invariant that Python cannot supply manifest/capability/tree-ID internals.
  - Added contract section + migration notes in `docs/docs/design/python-api.md` to keep implementation and docs aligned.
- Files modified:
  - `plans/python-api-fec-oblivious-refactor-plan.md`
  - `docs/docs/design/python-api.md`
- Errors/gotchas:
  - Current `python-api/src/lib.rs` still contains `fec_*` kwargs and helper plumbing by design at this stage; removal is deferred to T7.
  - Existing docs/examples still include legacy FEC kwargs outside the new contract section and will be cleaned in later tasks (T8/T9).

### T3 — Introduce Internal Runtime FEC Policy Layer
- `depends_on: [T2]`
- Scope:
  - Add an internal module (for example `dataplane/src/node/session/fec_policy.rs`) that owns:
    - sender FEC decision + manifest derivation,
    - sender tree-ID derivation/validation,
    - receiver capability derivation.
  - Move Python helper logic into runtime-owned policy functions.
- Files:
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/session/mod.rs`
  - `dataplane/src/node/session/fec_policy.rs` (new)
- Deliverables:
  - Runtime-only FEC policy API consumed by session startup paths.
- Validation:
  - Unit tests for policy input/output edge cases (chunk-size bounds, disabled FEC, tree-ID validation).

### T4 — Move FEC Defaults/Overrides To Runtime Config
- `depends_on: [T2]`
- Scope:
  - Ensure all FEC tuning currently supplied by Python is sourced from runtime internals/config.
  - Add/adjust `LosslessConfig` fields if needed for defaults previously passed by Python (for example symbols/block, symbol-size policy, tree-ID allowlist source).
  - Keep strict preflight rules centralized in runtime.
- Files:
  - `dataplane/src/node/config.rs`
  - `docs/docs/design/lossless_config.md`
  - `docs/docs/design/config-reference.md`
- Deliverables:
  - Config-backed internal FEC policy with no Python dependency.
- Validation:
  - Runtime config tests cover defaults/bounds/canonicalization.
- Status:
  - Completed on February 14, 2026.
- Work log:
  - Extended `LosslessConfig` runtime FEC knobs in `dataplane/src/node/config.rs` to cover the Python-owned defaults/overrides being internalized:
    - `fec_default_symbols_per_block`
    - `fec_symbol_size_policy` (`chunk_size` or `fixed`)
    - `fec_default_symbol_size`
    - `fec_tree_ids_source` (`config` or `installed_routes`)
    - `fec_default_tree_ids`
  - Added canonicalization helpers in `LosslessConfig`:
    - `canonical_fec_default_symbols_per_block()` clamps defaults into configured bounds.
    - `canonical_fec_default_symbol_size(chunk_size)` derives policy-driven defaults and clamps to bounds.
    - `canonical_fec_default_tree_ids()` enforces sorted+unique allowlists.
  - Added/updated config-focused tests for defaults, bounds normalization, symbol-size policy behavior, and tree-id canonicalization in `dataplane/src/node/config.rs`.
  - Updated runtime-config docs/examples in:
    - `docs/docs/design/lossless_config.md`
    - `docs/docs/design/config-reference.md`
- Files modified:
  - `dataplane/src/node/config.rs`
  - `docs/docs/design/lossless_config.md`
  - `docs/docs/design/config-reference.md`
  - `plans/python-api-fec-oblivious-refactor-plan.md`
- Errors/gotchas:
  - Runtime session-start still consumes explicit sender FEC fields today; T5 remains responsible for wiring these new config defaults into startup flow end-to-end.

### T5 — Refactor Runtime Session Start APIs To Be FEC-Agnostic At Boundary
- `depends_on: [T3, T4]`
- Scope:
  - Update sender/receiver start flow so API-facing request structs do not require FEC fields.
  - Derive internal `fec_manifest`, `fec_tree_ids`, `fec_capabilities` inside runtime startup path.
  - Preserve existing telemetry semantics (including `fec_used`) in control/controller path.
- Files:
  - `dataplane/src/node/session/runtime.rs`
  - `dataplane/src/node/session/sender.rs`
  - `dataplane/src/node/session/receiver.rs`
  - `controller/src/main.rs` (if telemetry mapping adjustments are required)
- Deliverables:
  - Runtime startup paths no longer require FEC input from Python caller.
- Validation:
  - Existing `dataplane/tests/fec_*` suites still pass with equivalent behavior.

### T6 — Add Regression Coverage For Internalized FEC Policy
- `depends_on: [T5]`
- Scope:
  - Add/adjust tests to ensure FEC behavior remains correct when derived internally.
  - Add regression test(s) for previous Python-helper validation rules now enforced in runtime.
- Files:
  - `dataplane/src/node/session/runtime.rs` tests
  - `dataplane/tests/fec_handshake.rs`
  - `dataplane/tests/fec_sender.rs`
  - `dataplane/tests/fec_receiver.rs`
- Deliverables:
  - Test evidence that FEC correctness does not depend on Python-provided manifest/capability data.
- Validation:
  - `cargo test -p dataplane --test fec_handshake`
  - `cargo test -p dataplane --test fec_sender`
  - `cargo test -p dataplane --test fec_receiver`

### T7 — Remove FEC From Python API Surface
- `depends_on: [T5]`
- Scope:
  - Remove FEC kwargs from `send_data`, `receive_data`, `receive_data_async` signatures.
  - Remove Python-side helper functions and message-type imports:
    - `sender_fec_manifest`
    - `sender_fec_tree_ids`
    - `receiver_fec_capabilities`
    - `FecManifest` / `FecCapabilities` imports
  - Keep error paths user-oriented (session preflight rejection without exposing runtime internals unnecessarily).
- Files:
  - `python-api/src/lib.rs`
- Deliverables:
  - Python extension API is FEC-oblivious.
- Validation:
  - `rg -n "fec_enabled|fec_symbols_per_block|fec_symbol_size|fec_tree_ids|FecManifest|FecCapabilities|sender_fec_manifest|receiver_fec_capabilities" python-api/src/lib.rs` returns no API-surface hits.

### T8 — Update Python Examples/Tooling To New Contract
- `depends_on: [T7]`
- Scope:
  - Remove FEC kwargs from example and tooling callsites.
  - Update smoke checks that currently assert presence of FEC kwargs in signatures.
  - Ensure examples still exercise lossless transfer using runtime-config-driven behavior.
- Files:
  - `examples/multicast-docker/scripts/multicast_node.py`
  - `tools/experiments/raptorq/smoke_python_api.py`
  - Any other Python callers found in T1 inventory.
- Deliverables:
  - No user-facing Python usage pattern depends on FEC kwargs.
- Validation:
  - `rg -n "fec_enabled|fec_symbols_per_block|fec_symbol_size|fec_tree_ids" examples tools`

### T9 — Documentation + Migration Notes
- `depends_on: [T7]`
- Scope:
  - Rewrite Python API docs to show FEC-oblivious usage.
  - Move FEC tuning guidance to runtime config docs.
  - Document migration steps for removed kwargs and expected failure mode (Python `TypeError` on old kwargs, if no shim).
- Files:
  - `docs/docs/design/python-api.md`
  - `docs/docs/design/lossless_config.md`
  - `docs/docs/design/config-reference.md` (if Python examples or references mention old kwargs)
- Deliverables:
  - Consistent docs with zero instruction to pass FEC parameters in Python API calls.
- Validation:
  - `rg -n "send_data\\(|receive_data\\(|fec_enabled|fec_tree_ids|fec_symbol" docs/docs/design`

### T10 — Add Guardrails To Prevent FEC Re-Exposure In Python API
- `depends_on: [T6, T8, T9]`
- Scope:
  - Add a lightweight CI/test check that fails if FEC-specific Python API parameters are reintroduced.
  - Gate on `python-api/src/lib.rs` signatures and docs/examples surface.
- Files:
  - `tools/experiments/raptorq/smoke_python_api.py` (or a dedicated API-contract check script/test)
  - CI config if available in repo workflow.
- Deliverables:
  - Automated detection of regressions in API encapsulation boundary.
- Validation:
  - Contract check fails when `fec_*` params are reintroduced; passes on clean branch.

### T11 — End-to-End Validation + Cutover
- `depends_on: [T10]`
- Scope:
  - Run full validation matrix and finalize cutover.
  - Confirm build/test/docs/tooling all align with new contract.
- Validation Commands:
  - `cargo check --workspace`
  - `cargo test --workspace`
  - `cargo nextest run --no-default-features --features python-extension --features dev-tests`
  - `maturin develop --release -m python-api/Cargo.toml` (or `maturin build --release -m python-api/Cargo.toml`)
  - Targeted Python smoke flow using updated scripts in `examples/` and `tools/`.
- Exit Criteria:
  - No FEC knobs in Python API, all tests green, docs/examples updated, guardrails active.

## Risks And Mitigations

- Risk: Python API breakage for existing callers that pass `fec_*` kwargs.
  - Mitigation: explicit migration notes + optional short-lived compatibility shim (if release policy demands).
- Risk: Behavioral drift after moving validation out of Python helpers.
  - Mitigation: regression tests in `dataplane` runtime and FEC integration suites before removing helpers.
- Risk: Hidden dependency in tooling/docs still expects FEC kwargs.
  - Mitigation: repository-wide grep checks and contract guardrail test in T10.

## Definition Of Done

- `python-api/src/lib.rs` has no FEC-specific API parameters, helpers, or FEC message imports.
- Dataplane runtime internally owns FEC policy + preflight.
- FEC integration behavior remains correct under runtime-config-driven operation.
- Docs/examples/tools align with the new FEC-oblivious Python API contract.
