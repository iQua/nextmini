# Stage 0 review (Claude)

Date: 2026-07-16
Scope: commits `194bed4..85ab8af` (6 code/report commits) against plan v2 Stage 0 (`plans/perfect-fec-runtime.md`).
Verdict: **APPROVED — proceed to Stage 1.** Two non-blocking findings recorded below; both land in code
that Stage 1.3 rewrites anyway.

## Verification performed

- Read the full diff (`git diff 55d2e8f..85ab8af`, +1789/−386 across 30 files).
- Independently re-ran Gate 0 on the final commit set: `cargo fmt --check` clean;
  `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run` → 655 passed,
  0 failed, 17 skipped (matches `plans/stage0-report.md`). Worktree clean; `main` untouched.

## Requirement-by-requirement check

- **0.1** ✓ `messages/src/lossless_session/fec_geometry.rs` derives the 65,443 symbol-payload ceiling
  from the IPv4/TCP/header/metadata constants in one place, pinned by test; `dataplane/node/packet.rs`
  and both `BlockSymbol` encoders now consume those constants instead of duplicating them.
  `WireFecGeometry::new` is fully checked (no lossy `as` on any peer-reachable path); codec caps are
  layered in `session/fec.rs::validate_fec_geometry` (RaptorQ `1..=56_403`, u16 symbol size; METTLE
  terminated-stream `u32` bound). `oti()` → `Result`. The audit config `block_size=2_097_152, K=32`
  is rejected at manifest validation (receiver now calls `manifest.validate()` on install, and
  `derive_sender_policy` validates geometry preflight — both directions covered).
- **0.2** ✓ `FecSymbolIdBounds` gives one validated boundary per scheme (RaptorQ 24-bit ESI, METTLE
  terminal end). Receiver validates peer `symbol_id` before storage/decode and counts it as invalid
  traffic; adapter `source_symbol`/`coded_symbol` asserts replaced with typed errors, so no crate
  assert is reachable from peer input (property-style test sweeps ~518 ESIs × payload lengths).
  Local repair generator stops before wrap via `next_after` → `SymbolIdExhausted`.
- **0.3** ✓ `SinkWriteError` propagates through `write_block`/`write_symbol_run`/`ensure_sink_len`;
  receiver run loop returns `SessionOutcome::SinkError` distinctly and never reports Complete after a
  failed write (injected read-only-file test asserts both). Bonus consistency fix: receiver exit now
  maps to Completed only when `reported_complete()`, aligned with replay-cache retention in
  `runtime.rs::finish_session`.
- **0.4** ✓ `sink_file` fixture field supplied across integration tests; `while_let_loop` fixed in
  `mettle/src/block.rs`; `fec_mettle_session` renamed/re-pinned to the documented rounds-mode contract
  (SourceDone after the full terminated codeword, contract stated in the sender doc comment and test);
  the full-suite-only fixture race fixed by moving `drop(harness.tx)` after the Need assertion.
- **0.5** ✓ Boundary matrix present: K ∈ {1, 56_403} accepted / {0, 56_404} rejected; symbol size
  1 / 65_443 / 65_444; checked `K*T` (u32::MAX × 100_000 → u64, no wire truncation); empty object,
  partial final block, exact fit; host `usize` boundaries behind `usize::BITS` guard; ESI 2^24−1 / 2^24
  through the adapter; METTLE final terminated bin accepted, `terminal_end_exclusive` and u32::MAX
  rejected.

## Non-blocking findings (fold into Stage 1.3)

1. **Premature abort at ESI exhaustion** (`sender/fec.rs::send_extra_symbol`): after emitting the last
   valid ESI (2^24−1), `next_repair_symbol_id` errors and sets `protocol_error` immediately, and the
   run loop checks `protocol_error` after `drain_controls` without re-checking `is_complete()` — a
   session whose final valid symbol completed the transfer would still be reported Aborted. Only
   reachable after ~16.7M repair symbols for one block, and the plan's 1.3 send-loop refactor
   ("completion re-checked after pacing and immediately before frame submission") removes this
   structurally. Requirement for Stage 1.3: exhaustion must abort only when another emission is
   *required*, not merely scheduled.
2. **Nit**: `sender/block_symbol_frame.rs::patch_tree_id` still computes `off + hdr.body_len as usize`
   unchecked. Only sender-local frames flow through it today, so not peer-reachable; align it with the
   checked form used in `messages/block_frames.rs` whenever the file is next touched.
