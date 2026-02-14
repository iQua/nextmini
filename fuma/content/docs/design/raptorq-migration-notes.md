---
title: "RaptorQ Migration Notes (T1 Snapshot)"
description: ""
---


Date: February 12, 2026

## Scope
This document freezes the pre-migration source inventory for RaptorQ/FEC-related code copied from `/Users/bli/Playground/asupersync` into `/Users/bli/Playground/nextmini`.

## Source Provenance
- Source repository root: `/Users/bli/Playground/asupersync`
- Source commit: `f388be666a8b1aab04b9dfecec4ca962fa378d1d`
- Validation command:
  - `rg -n "raptorq|fec|fountain" /Users/bli/Playground/asupersync`
- Validation summary:
  - 1130 matching lines across 216 files (broad match because `fec` matches tokens such as `effective`).
  - Migration inventory curated by explicit path review.

## Frozen Source -> Target Mapping (16 Files)
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

## License Snapshot (MIT)
- `/Users/bli/Playground/asupersync/LICENSE` contains the MIT License text.
- `/Users/bli/Playground/asupersync/Cargo.toml` declares `license = "MIT"`.
- Migration requirement: preserve MIT notice text in redistributed/migrated source.
