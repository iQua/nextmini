# Lossless Test Coverage Plan

## Goals

Add the missing subsystem coverage for the rewritten block-first lossless session
design:

- plain-mode end-to-end sender/receiver transfer
- multi-block transfer in both plain and FEC modes
- multi-receiver sender completion semantics
- topology-ready and ready-grace runtime behavior

Keep the work scoped to tests unless a bug is uncovered. Prefer one integration
test file per coverage area so work can run in parallel with disjoint write
sets.

## Dependency Graph

`T1`, `T2`, `T3`, and `T4` can run in parallel.

`T5` depends on `T1`, `T2`, `T3`, and `T4`.

## Tasks

### T1: Add plain-mode end-to-end coverage

- **depends_on**: []
- **Location**: `dataplane/tests/plain_transfer.rs`
- **Description**:
  Add integration coverage for the plain block-first path. Exercise a complete
  sender/receiver exchange using `BlockData`, `BlockAck`, and `Eot`, including
  sink writes on the receiver and sender completion after the receiver acks.
- **Acceptance Criteria**:
  - Covers a successful plain-mode transfer end to end.
  - Verifies the receiver sends `Ready`, receives `BlockData`, writes the sink,
    and emits `BlockAck`.
  - Verifies the sender completes only after the block is acknowledged.
  - Uses the current shared harness or local test helpers without changing
    session runtime behavior.
- **Validation**:
  - `cargo test -p nextmini --test plain_transfer -- --nocapture`
- **Status**: Completed
- **Work Log**:
  - Added `dataplane/tests/plain_transfer.rs`.
  - Covered plain receiver `Ready`/`BlockAck`/sink-write behavior and plain sender
    completion after `BlockAck`.
  - Shifted from a bridged sender+receiver test to focused sender/receiver tests
    after the bridge harness proved flaky and unnecessary for this coverage gap.

### T2: Add multi-block transfer coverage

- **depends_on**: []
- **Location**: `dataplane/tests/multiblock_transfer.rs`
- **Description**:
  Add integration tests for multi-block transfers in both plain and FEC modes.
  Validate block ordering/completion semantics across more than one block rather
  than the mostly single-block coverage that exists now.
- **Acceptance Criteria**:
  - Includes one plain-mode multi-block transfer test.
  - Includes one FEC-mode multi-block transfer test.
  - Verifies all blocks are delivered/acknowledged and final sink contents match
    the source payload.
  - Verifies block IDs beyond `0` are exercised by the integration path.
- **Validation**:
  - `cargo test -p nextmini --test multiblock_transfer -- --nocapture`
- **Status**: Completed
- **Work Log**:
  - Added `dataplane/tests/multiblock_transfer.rs`.
  - Covered multi-block plain sender emission, plain receiver writes/acks, FEC
    sender symbol emission across blocks, and FEC receiver decode/ack across
    multiple blocks.
  - Exercised block IDs `0`, `1`, and `2` through the integration path.

### T3: Add multi-receiver completion coverage

- **depends_on**: []
- **Location**: `dataplane/tests/multi_receiver.rs`
- **Description**:
  Add sender/runtime integration coverage for multiple receivers. The new design
  requires per-block acknowledgements from each receiver before the sender
  considers the session complete; that is only unit-tested today in the ledger.
- **Acceptance Criteria**:
  - Starts a sender with at least two receivers.
  - Verifies the sender does not complete after only one receiver acknowledges a
    block.
  - Verifies completion occurs only after every receiver has acknowledged every
    block.
  - Keeps coverage aligned with per-block, non-cumulative ACK semantics.
- **Validation**:
  - `cargo test -p nextmini --test multi_receiver -- --nocapture`
- **Status**: Completed
- **Work Log**:
  - Added `dataplane/tests/multi_receiver.rs`.
  - Covered sender completion gating across two receivers and two blocks.
  - Verified the session remains incomplete until every receiver has acknowledged
    every block.

### T4: Add topology-ready and ready-grace coverage

- **depends_on**: []
- **Location**: `dataplane/tests/runtime_ready.rs`
- **Description**:
  Add runtime-focused integration coverage for topology gating and ready-grace
  behavior. The sender should wait for topology readiness when required and
  should respect the ready grace behavior around receiver readiness.
- **Acceptance Criteria**:
  - Covers sender behavior while topology is not yet ready.
  - Covers sender behavior once topology readiness is signaled.
  - Covers ready-grace handling without introducing any legacy sliding-window or
    timeout semantics.
  - Verifies the tests use the current runtime/session APIs rather than private
    internals.
- **Validation**:
  - `cargo test -p nextmini --test runtime_ready -- --nocapture`
- **Status**: Completed
- **Work Log**:
  - Added `dataplane/tests/runtime_ready.rs`.
  - Covered topology-ready gating before handshake start and ready-grace opening
    behavior when no `Ready` arrives.
  - Verified both tests use the public runtime/session APIs.

### T5: Integrate and validate the expanded suite

- **depends_on**: [T1, T2, T3, T4]
- **Location**:
  - `plans/lossless-test-coverage-plan.md`
  - `dataplane/tests/*.rs`
- **Description**:
  Review the new tests together, resolve any overlap or bugs uncovered by the
  new coverage, and run the lossless-focused validation plus the full required
  test command.
- **Acceptance Criteria**:
  - All prior tasks are marked complete with concise logs.
  - The new integration tests all pass together.
  - The standard project lossless test command passes.
  - Any bug fixes uncovered by the new tests are included and regression-tested.
- **Validation**:
  - `cargo test -p nextmini --test plain_transfer -- --nocapture`
  - `cargo test -p nextmini --test multiblock_transfer -- --nocapture`
  - `cargo test -p nextmini --test multi_receiver -- --nocapture`
  - `cargo test -p nextmini --test runtime_ready -- --nocapture`
  - `cargo nextest run --no-default-features --features python-extension --features dev-tests`
- **Status**: Completed
- **Work Log**:
  - Ran targeted integration tests for the new coverage files.
  - Ran `cargo nextest run --no-default-features --features python-extension --features dev-tests`.
  - Final suite result: `362` tests passed, `0` failed.
