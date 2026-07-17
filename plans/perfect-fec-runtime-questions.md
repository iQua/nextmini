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
