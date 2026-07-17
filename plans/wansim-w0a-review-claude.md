# wansim W0a review (Claude)

Date: 2026-07-17
Scope: commits `199d5c2..f2a8c1a` against plan `plans/wansim-plan.md` incl. the discussion
resolutions (`29cf285`).
Verdict: **APPROVED — the days-based foundation is confirmed GO; W0b may proceed.**

## Verification performed

- Independent wansim gate: fmt clean, clippy `-D warnings` clean, `cargo nextest run` → 20/20.
- Golden trace independently reproduced: ran the committed scenario from the release binary and
  `cmp` against `tests/golden/w0a_chain.csv` — byte-identical.
- Spot checks: `exclude = ["wansim"]` present in the root manifest; the ACK-gap regression test
  (`cumulative_ack_does_not_advance_across_a_gap`) exists in the vendored fork;
  `wansim/vendor/DIVERGENCE.md` records exact upstream commit/tree identities and the
  `git archive` import method; root workspace regression re-run independently (828 expected).

## Assessment

- The foundation risk W0a existed to retire is retired with the strongest possible form of
  evidence: a closed-form-explainable backpressure plateau (exactly 5,120 admitted bytes = the five
  distinct 1,024-byte owners; no double-counting of in-flight bytes against send buffers) and an
  exactly-derivable serialization interval (552 B × 8 / 1 Mbit/s = 4,416,000 ns between saturated
  departures). Numbers that fall out of arithmetic, not curve-reading.
- The discussion round demonstrably paid for itself twice: (1) the fork's flow-control layer —
  which upstream days lacked — is what makes the plateau possible at all; (2) the registration-order
  metamorphic test caught a real same-time race in transient link occupancy during development,
  which was fixed by the local one-delta tie-resolution mechanism the resolutions prescribed.
- Fork hygiene is as ruled: every change general-purpose (rwnd + min(cwnd,rwnd), finite
  send/receive buffers, read credit, zero-window persist, NODELAY knob, wire-overhead accounting,
  the cumulative-ACK gap fix), each with its own enforcement test; provenance exact; no nextmini
  semantics in the vendor tree; nexosim unmodified.
- Honest model limits are declared rather than hidden (payload bytes reconstructed from stream
  offsets — no corruption modeling; the one-delta bookkeeping excluded from modeled latency;
  mailbox high-water marks instrumented and nonbinding at ≤2/256).

## Notes for W0b (non-blocking)

1. The relay's single application/frame buffer is one owner in the chain; W0b's per-child queues
   must keep byte-ownership accounting exact as fan-out multiplies owners — extend the closed-form
   plateau test to the tree shape.
2. Keep the W0b hybrid-drop gate anchored on the distinction the design review flagged: the drop
   occurs AFTER hop TCP has acknowledged the bytes (transport delivered, application refused) —
   assert that ordering explicitly in the test.
3. Mailbox instrumentation must stay on as fan-out increases event fan-in; the ≤2 high-water result
   will not survive trivially.
