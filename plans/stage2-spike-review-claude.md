# Stage 2.0 spike review (Claude)

Date: 2026-07-16
Scope: commits `779d199` (harness) + `0680b86` (report) against plan v2 item 2.0.
Verdict: **APPROVED — the layout decision is accepted and binding for Stage 2.1+.**

## Assessment

Method is sound: one layout/scenario per process isolates peak RSS; deterministic seed and nonzero
payload avoid zero-page artifacts; scenarios cover the plan's asks (source counts to 2^21, both
production symbol scales, ordered/loss/reorder, and the two worst-case shapes — permanent leading
stall and terminal-jump reorder); limitations are honestly stated (single ARM64 host, RSS vs logical
payload, preemption noise). Harness scope verified: only `mettle/examples/` plus measurement-only
test-support constructors; production decoder selection and wire surfaces untouched.

Both decisions follow from the data rather than preference:

- **Mega-prefixes over one unbounded object stream**: the permanent-leading-stall table shows the
  `O(N·T)` future-payload retention is layout-independent (2.8 GiB logical payload at 2^21 × 1400 in
  BOTH layouts) — only bounding `N·T` per negotiated stream can bound it, hence the 65,536-source and
  checked `N·T ≤ 96 MiB` caps.
- **Dense over rolling within the bounded prefix**: rolling merely defers graph cost onto the first
  far-ahead receive — a peer-influenceable, input-order-dependent synchronous cliff inside `push_bin`
  (18 ms at the cap, 673 ms at 2^21) exactly where the async loop needs bounded work; dense converts
  that to a predictable 8.4 ms / 13.7 MiB pre-Ready construction at the cap.

The construction/rejection contract matches P8-style discipline and adds the important catch that
Stage 2.1 must make all `O(N)` graph allocation fallible (`try_reserve`), since today's infallible
`Vec` allocation cannot produce a clean manifest rejection.

## Binding constraints carried into Stage 2.1+ (from the report, accepted)

1. Per-stream source count ≤ 65,536 AND checked `source_count × symbol_bytes ≤ 96 MiB.
2. Deterministic manifest-negotiated prefix geometry (count, final-prefix source count, seeds);
   nothing selected locally after Ready.
3. 192 MiB decoder reservation, four-permit / 768 MiB process aggregate, sequential decoder lifetime
   per session.
4. Fallible dense construction on a blocking worker BEFORE Ready; budget/allocation failure ⇒ no
   Ready, permit released, session exits Aborted, no partial install; sender follows the existing
   Ready-quorum timeout.

## Review notes (non-blocking)

- The 192 MiB / 4-permit budget was derived on an 18 GiB dev host. Make both knobs configurable with
  these as validated defaults, and re-check the aggregate against the actual WAN experiment nodes
  before Stage 3's benchmark sweep.
- Gate 2's "max simultaneous streams explicit and met" is satisfied by the permit mechanism; the
  Gate 2 test should assert permit exhaustion behaves as specified (fifth decoder rejected cleanly).
