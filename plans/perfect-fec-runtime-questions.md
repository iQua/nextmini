# Perfect FEC runtime open questions

## Stage 2 review follow-ups (2026-07-16)

### Process-wide sender-cache admission (review item 7)

The Carousel+METTLE sender retains an encoded prefix for targeted repair and replay. Its per-session
payload retention is `terminal_bin_count(N, coded_rate) * T`, plus container overhead, but there is
not yet a process-wide permit pool for concurrent sender caches.

This follow-up does not add a partial budget because correct enforcement needs a configurable
process-wide pool, admission before a sender begins payload emission, and a permit whose ownership
spans prefix replacement and every sender exit path. That is a new sender admission surface rather
than a local repair-path fix.

Until that pool is implemented, deployments must multiply the negotiated per-session bound above
by the configured maximum number of simultaneous Carousel+METTLE senders and keep that total inside
their sender memory allocation. Add the symmetric sender-cache budget before enabling unbounded
concurrent paper-native sessions in production; Stage 3 must not accidentally treat the receiver's
four-decoder pool as covering sender memory.

### Automated performance thresholds (review item 9)

The release spike's RSS and wall-clock construction results depend on allocator state, host load,
toolchain, and operating-system RSS accounting. A hard assertion in a normal test would therefore
be flaky or so generous that it would not catch a meaningful regression.

The accepted interim policy is to build the release spike and rerun its `construct`,
`terminal-jump`, and `prefix-stall` scenarios at every stage gate. Gate 2 follow-up measurements are
recorded in `plans/stage2-report.md`; all are below the 25 ms construction and 192 MiB RSS limits.
The same manual release measurements are mandatory at Gate 3. A stable dedicated benchmark runner
with controlled allocator and machine class would be required before promoting these limits to
automated pass/fail assertions.

## Stage 3.0/3.1 reservoir simulation (2026-07-16)

The reservoir construction evaluated here is an extension **BEYOND the METTLE paper**. Stage 3.2
must not start from these results.

### Define an efficacy threshold before any new reservoir sweep

The plan's mechanical Gate 3 criteria all pass, but it does not state a minimum completion
probability for the research gate. The strongest tested point used 11.0107% actual total overhead
and completed only 91.040% of the short-burst GE trials and 64.868% of the long-burst GE trials in
the 4,096-trial confirmation. Those results are treated as a no-go for integration.

Before revisiting this construction, define explicit channel-by-channel completion and latency
targets, including a burst-duration envelope. A new sweep should be judged against those targets
rather than choosing a threshold after observing the results.

### Sender memory admission remains broader than the reserve payload

The prototype gives reserve-only retention its own checked 8 MiB bound. At the largest swept
`c_reserve=7%` cap geometry, payload plus the prototype index used 6,641,976 bytes. This does not
solve the older process-wide sender-cache question: an integrated sender must budget source-object
lifetime, the existing full-bin replay cache (or replace it), reserve payloads, and concurrent
sessions together. The receiver's four-decoder pool covers none of this memory.

### Post-emission reserve loss remains Stage 2.4 traffic

The simulation counts only first global reserve emissions and measured zero fresh-emission
duplicates. It intentionally does not retransmit a reserve id lost after that emission; the design
classifies that as Stage 2.4 targeted retransmission, not another fresh reserve. If reservoir
research resumes, a multi-peer simulation must add that traffic and its duplicates as a separate
metric without weakening the global emit-once invariant.
