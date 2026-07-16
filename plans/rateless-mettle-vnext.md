# Rateless METTLE (multi-pass reseed) — vNext research plan stub

Extracted from the perfect-fec-runtime master plan per the Codex review (C6): the idea is
incompatible with the current wire (`BlockSymbol.symbol_id: u32`; kernel bin ids are `u128` narrowed
via `u32::try_from` before transmission) and is not "unlimited" as previously framed. It must be a
separate, protocol-versioned effort. Default-off, experimental.

Goal: unbounded-in-practice fresh METTLE equations — pass p re-encodes the same source prefix with
seed_p = H(base_seed, p), and the decoder peels the union graph across passes, making carousel treat
METTLE like a true fountain (A3 holds unconditionally).

Decisions required BEFORE any runtime integration (from the review, all adopted):

1. Wire representation: explicit `{ pass_id, raw_bin_id }` (not u128 top-bits); new frame layout +
   protocol version; recompute the symbol-payload envelope ceiling if the frame grows (keeps Stage-0
   geometry non-stale).
2. Pass bound and validation: bounded pass count with explicit exhaustion semantics — never claim
   "unlimited"; raw-id validation per pass.
3. Seed derivation: stable hash with published test vectors; sender/receiver agreement tests.
4. Decoder architecture: N edge-generators sharing one recovered-source set; work is O(P·l) per
   recovery at P active passes — bound P; define equation/graph eviction with a correctness argument.
5. Recovered-source backing: the rolling decoder retains only a coupling-window prefix of released
   payloads, but a later pass restarted from bin zero may need older recovered payloads to reduce new
   equations — choose memory, sink read-back, or a dedicated store; enforce a measured memory cap as
   pass count grows.
6. Freshness is not free: a fresh namespaced id is not automatically a fresh degree of freedom.
   Measure equation identity / rank gain / decode gain across seeds before claiming A3; record curves
   (decode success vs cumulative overhead across passes) in `results/`.

Prerequisites: perfect-fec-runtime Stages 0–2 landed (checked geometry, carousel protocol,
object-stream METTLE, reorder-safe repair), plus the Stage 2.0 decoder memory budget as the baseline
for the multi-pass cap.
