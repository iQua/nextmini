# Namespace Lossless Integration Test Plan

## Goal

Add one simple, explicit Linux end-to-end integration harness that runs Nextmini in namespace mode, sends a real file through the lossless session subsystem, and verifies that every receiver writes exactly the same bytes as the sender.

The harness must cover:
- plain mode and FEC mode
- varying receiver count
- varying tree count
- varying block size
- varying symbol geometry

The harness should stay small. It should not become a second test framework, and it should not be wired into the default `cargo test` path.

## Constraints

- This is Linux-only and requires `sudo`.
- Namespace mode itself should not depend on Docker.
- The local Postgres service can be bootstrapped with [utils/start-database.sh](/Users/bli/Playground/nextmini/utils/start-database.sh).
- The controller should run as a host-local process.
- The dataplane must run in actual namespace mode through [main.rs](/Users/bli/Playground/nextmini/dataplane/src/main.rs) and [manager.rs](/Users/bli/Playground/nextmini/dataplane/src/node/namespace/manager.rs), not through a separate hand-rolled namespace stack.
- The current namespace launcher only spawns Rust dataplane children. It cannot directly reuse the existing Python lossless demo driver in [multicast_node.py](/Users/bli/Playground/nextmini/examples/multicast-docker/scripts/multicast_node.py).
- `symbol_size` is not a first-class runtime config knob today. It is derived from `block_size` and `symbols_per_block` in [plan.rs](/Users/bli/Playground/nextmini/dataplane/src/node/session/plan.rs).

## Design Decisions

1. Keep the existing namespace-mode transport substrate.
   Reuse the built-in namespace deployment path instead of introducing a second `ip netns` orchestration layer.

2. Add a narrow native e2e role inside selected namespace children.
   Source and receiver behavior should run in-process on top of the child node's existing `Conductor`, `ControllerInterfaceHandle`, and `LosslessRuntimeHandle`, rather than through `nextmini_py`.

3. Keep the test harness explicit.
   The test should run through a dedicated script and generated config under `examples/ns-flow/`, not through the default unit/integration test path.

4. Verify hashes on disk.
   The sender writes one source artifact plus SHA256, and every receiver writes one sink artifact plus SHA256. The verifier compares receiver files directly against the sender artifact.

5. Vary symbol geometry through `symbols_per_block`, not a new `symbol_size` config field.

## Scope

In scope:
- host-local orchestration for Postgres, controller, and namespace dataplane
- one namespace-mode file transfer harness for plain and FEC sessions
- SHA256 verification
- a small default case matrix
- short user-facing docs

Out of scope:
- CI integration
- broad performance benchmarking
- rewriting the existing Python multicast Docker harness
- adding a new production runtime config just for testing

## Architecture

### Runtime shape

- One host-local controller process started from the checked-in controller config.
- One namespace-mode dataplane parent process started via `nextmini --config-path ...` with `n_nodes > 1`.
- Namespace children continue to be created by [manager.rs](/Users/bli/Playground/nextmini/dataplane/src/node/namespace/manager.rs).
- A new optional test-role config block determines whether a child node is:
  - idle router
  - lossless source
  - lossless receiver

### Why not reuse the Python harness directly?

The Python helper in [multicast_node.py](/Users/bli/Playground/nextmini/examples/multicast-docker/scripts/multicast_node.py) is useful as a behavioral reference, but the namespace child path does not embed `nextmini_py`. Reusing it directly would require building a second namespace orchestration layer outside the current namespace manager. That is more moving parts than adding one narrow in-process test role.

### Minimal native role behavior

The native role should only implement what this harness needs:
- source role
  - wait for topology readiness
  - create one multicast group
  - install one tree or multiple trees
  - wait for receiver readiness
  - read source file from disk
  - start one sender session through `LosslessRuntimeHandle`
  - wait for completion
  - write sender SHA256 metadata
- receiver role
  - wait for topology readiness
  - join group
  - start one receiver session
  - wait for completion
  - write sink file to disk
  - write receiver SHA256 metadata
- router role
  - no app logic beyond normal dataplane operation

## Files To Add Or Change

Core runtime:
- [dataplane/src/node/config.rs](/Users/bli/Playground/nextmini/dataplane/src/node/config.rs)
  Add a minimal `[integration_test]` config block for namespace e2e roles and artifact paths.
- [dataplane/src/node/conductor.rs](/Users/bli/Playground/nextmini/dataplane/src/node/conductor.rs)
  Start the native e2e role task when the config enables it.
- `dataplane/src/node/integration_test/mod.rs`
  New small module that implements source/receiver/router role behavior.
- `dataplane/src/node/integration_test/controller_helpers.rs`
  Extract or mirror only the controller-side helpers needed for group creation, join, route install, and readiness waiting.
- `dataplane/src/node/integration_test/hash.rs`
  Small SHA256 helper and artifact writer.

Namespace harness:
- [examples/ns-flow/generate.py](/Users/bli/Playground/nextmini/examples/ns-flow/generate.py)
  Extend or add a sibling generator for the small e2e topology and case matrix.
- `examples/ns-flow/run-integration.sh`
  New explicit runner for the namespace lossless matrix.
- [examples/ns-flow/cleanup.sh](/Users/bli/Playground/nextmini/examples/ns-flow/cleanup.sh)
  Extend cleanup to remove generated e2e artifacts and any new pid/log files.
- `examples/ns-flow/verify_hashes.py`
  New verifier that compares sender and receiver artifacts.
- `examples/ns-flow/artifacts/`
  Generated runtime output only.

Docs:
- `plans/integration-test.md`
  This implementation plan.
- [docs/content/docs/examples/networking/ns-flow.md](/Users/bli/Playground/nextmini/docs/content/docs/examples/networking/ns-flow.md)
  Add a short section for the lossless namespace harness.

## Dependency Graph

```text
T1 -> T2, T3
T2 -> T4
T3 -> T5
T4 -> T5, T6
T5 -> T7
T6 -> T7
T7 -> T8
```

## Tasks

- `T1` `depends_on: []`
  Freeze the contract and topology.
  Define one fixed small namespace topology with one source, two routers, and up to two receivers. Define the success criteria: every receiver file hash must equal the sender file hash.

- `T2` `depends_on: [T1]`
  Add the minimal runtime config surface for the namespace e2e harness in [config.rs](/Users/bli/Playground/nextmini/dataplane/src/node/config.rs).
  Keep it small:
  - role: `source`, `receiver`, or `router`
  - case metadata: group label, source node id, receiver ids, artifact dir, ports
  - source file path or generated payload size
  - receive timeout and group timeout
  - route/tree metadata needed by the source role

- `T3` `depends_on: [T1]`
  Extract the controller/runtime helpers needed by the native role.
  Reuse existing behavior from [python-api/src/lib.rs](/Users/bli/Playground/nextmini/python-api/src/lib.rs) as the reference for:
  - `create_group`
  - `join_group`
  - `set_group_routes`
  - `set_group_routes_multi`
  - waiting for topology readiness
  - waiting for group creation and route installation
  The implementation should live in Rust inside the dataplane crate so namespace children can call it directly.

- `T4` `depends_on: [T2]`
  Implement the native namespace e2e role module.
  The source role should:
  - wait for topology ready
  - create the group and install trees
  - wait for receiver readiness markers
  - read the source file
  - start one sender session with the requested block size
  - wait for completion
  - write sender artifact metadata and SHA256
  The receiver role should:
  - wait for topology ready
  - join the group
  - start one receiver session with a sink buffer
  - wait for completion
  - write the received file and SHA256

- `T5` `depends_on: [T3]`
  Add the namespace integration runner under `examples/ns-flow/`.
  It should:
  - ensure Linux and `sudo`
  - start local Postgres via [utils/start-database.sh](/Users/bli/Playground/nextmini/utils/start-database.sh) if needed
  - start the controller locally with the generated config
  - start `nextmini` in namespace mode with the generated dataplane config
  - wait for artifacts and final status
  - always call cleanup on exit
  The runner should use host-local processes, not `docker compose`.

- `T6` `depends_on: [T4]`
  Add result verification.
  `verify_hashes.py` should compare:
  - source file size and SHA256
  - each receiver file size and SHA256
  and fail fast on any mismatch or missing artifact.

- `T7` `depends_on: [T4, T5, T6]`
  Define the default case matrix and generated configs.
  Keep the matrix short:
  1. plain, 1 receiver, 1 tree, moderate block size
  2. fec, 1 receiver, 1 tree, same payload
  3. fec, 2 receivers, 2 trees, different block size
  4. fec, 2 receivers, 2 trees, different `symbols_per_block`
  The generator should translate each case into controller config, dataplane config, and artifact paths.

- `T8` `depends_on: [T7]`
  Document the harness in [ns-flow.md](/Users/bli/Playground/nextmini/docs/content/docs/examples/networking/ns-flow.md).
  Include:
  - prerequisites
  - how to start local Postgres
  - how to run one case
  - how to run the default matrix
  - where artifacts land
  - what the harness proves
  - that symbol geometry is varied through `symbols_per_block`

## Validation Plan

Build validation:
- `cargo build -p controller --release`
- `cargo build -p nextmini --release --features python-extension`

Harness validation:
- run one plain case end-to-end
- run one one-tree FEC case end-to-end
- run one two-tree FEC case end-to-end
- run the default matrix

Per-case pass criteria:
- controller starts and accepts all namespace child connections
- topology-ready signal is observed
- source session completes successfully
- every receiver session completes successfully
- every receiver file size matches the sender file size
- every receiver SHA256 matches the sender SHA256

## Risks And Mitigations

- Risk: controller/group helper logic is currently concentrated in the Python API path.
  Mitigation: extract the minimum needed group/control wait logic into a Rust helper module instead of duplicating broad Python API behavior.

- Risk: namespace mode currently targets dataplane-only children, not application roles.
  Mitigation: keep the native role module tiny and opt-in through config so it is only active for this harness.

- Risk: the existing `examples/ns-flow` scripts assume Docker for controller/Postgres.
  Mitigation: add a separate runner for the integration harness instead of mutating the existing large-flow demo in place.

- Risk: symbol size request could tempt a new runtime knob.
  Mitigation: do not add one; vary `symbols_per_block` and document the derived relationship.

## Non-Goals

- No CI wiring in this change.
- No new general-purpose namespace orchestration framework.
- No attempt to make this a cross-platform test.
- No expansion of controller-managed unicast flow tests to cover multicast/FEC file verification.
