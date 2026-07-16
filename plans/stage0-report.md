# Stage 0 completion report

Date: 2026-07-16  
Branch: `perfect-fec-runtime`  
Scope: Stage 0 only; Stage 1 was not started.

## What changed

### 0.1 Layered checked FEC geometry

- Added dependency-free `WireFecGeometry` validation to `nextmini-messages`.
- Derived the single 65,443-byte FEC symbol-payload ceiling from the IPv4, TCP, lossless-header, and `BlockSymbol` metadata constants.
- Kept wire geometry checks in `messages`, then layered codec constraints in dataplane `fec_policy` and session FEC construction.
- Enforced RaptorQ's `1..=56_403` source-symbol range and fallible OTI construction, and replaced FEC geometry truncation paths with checked conversions.
- Enforced the terminated METTLE stream's `u32` wire symbol-count bound.
- Rejected the audit configuration `block_size=2_097_152, K=32` during manifest validation because its derived 65,536-byte symbol exceeds the packet envelope.

### 0.2 Scheme-aware symbol-ID bounds

- Added one validated symbol-ID boundary per installed FEC scheme: 24-bit ESI space for RaptorQ and the finite terminated-stream end for METTLE.
- Validated peer-controlled IDs and payload lengths before storage, codec packet construction, or decode.
- Made the local RaptorQ repair generator stop before ESI wrap with `SymbolIdExhausted`.
- Added malformed block/control-frame coverage across body lengths, symbol IDs, and range counts, plus property-style adapter input coverage.

### 0.3 Fallible sink writes

- Made plain, Cloudcast, RaptorQ, and METTLE sink sizing/writes propagate I/O failures.
- Added the distinct `SessionOutcome::SinkError` abort result.
- Prevented failed writes from being marked complete or acknowledged as successful.
- Added an injected read-only sink test that verifies the abort outcome and absence of completion feedback.

### 0.4 Test baseline and documented contracts

- Supplied the new `sink_file` fixture field throughout affected session integration tests.
- Rewrote the METTLE block drain to satisfy the `while_let_loop` lint and resolved other workspace/all-target lint findings required by Gate 0.
- Updated `fec_mettle_session` to the documented rounds-mode contract: `SourceDone` follows the full terminated codeword, not only `initial_symbol_count`.
- Stabilized the FEC receiver fixture so it observes its queued Need report before closing receiver input.

### 0.5 Boundary matrix

- Covered RaptorQ `K=1` and `K=56_403` acceptance and `K=0` and `K=56_404` rejection.
- Covered one-byte, maximum-envelope, and maximum-plus-one symbol sizes.
- Covered checked `K*T`, padding, a partial final block, an empty object, exact-fit objects, and host `usize` conversion boundaries.
- Covered RaptorQ ESI `2^24-1` acceptance and `2^24` rejection through the adapter.
- Covered METTLE's final terminated bin acceptance and `terminal_end_exclusive`/larger-ID rejection.

## Commits

| Sub-stage | Commit | Message |
| --- | --- | --- |
| 0.1 | `194bed4` | `Add checked layered FEC geometry.` |
| 0.2 | `e384cc3` | `Enforce scheme-aware FEC symbol IDs.` |
| 0.3 | `681b0bc` | `Propagate receiver sink failures.` |
| 0.4 | `8c024ac` | `Restore the Stage 0 test baseline.` |
| 0.5 | `2e1e0f5` | `Pin the Stage 0 boundary matrix.` |
| 0.4 gate follow-up | `db45f6a` | `Stabilize the FEC receiver fixture.` |

## Test results

- `cargo fmt --check`: passed.
- `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run`: 655 passed, 0 failed, 17 skipped.
- Focused dataplane/messages run during Stage 0.5: 465 passed, 0 failed, 3 skipped.

The explicit Python path selects the repository-required CPython 3.13 for PyO3; automatic PyO3 discovery selected an installed Python 3.12 interpreter. Incremental artifacts were disabled after an initial full-suite attempt exhausted the host's remaining disk space while creating temporary sink fixtures. After cleaning generated Cargo artifacts, the final full gate completed successfully.

The 17 final skips are pre-existing ignored tests: 14 manual METTLE benchmark/reproduction checkpoints, one duplicate-coverage runtime test, and one Stage 1 red-test scaffold compiled in both the dataplane library and binary test targets. No selected controller test required an unavailable PostgreSQL instance, so no PostgreSQL skip was recorded and `plans/perfect-fec-runtime-questions.md` was not created.

## Open questions

None for Stage 0.

