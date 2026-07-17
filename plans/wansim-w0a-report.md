# wansim W0a report — transport and deterministic-chain foundation

Date: 2026-07-17  
Branch: `perfect-fec-runtime`  
Scope: W0a only (single chain; no fan-out, receiver inbox policy, DoF bucket, or §P feedback)  
Evidence class: deterministic model-level evidence, not a WAN measurement or calibrated digital twin

## Verdict

**GO for W0b.** The pinned days/nexosim foundation held up, but only after the transport gap
identified in the design review was implemented as a real socket-flow-control layer. The W0a chain
now has persistent, independent hop TCP state; finite link, send, receive, and relay buffers; TCP
receive-window backpressure; application read credit; loss recovery; frame-level store-and-forward;
integer-nanosecond scheduling; and byte-stable owned recording.

The strongest foundation result is the paused-reader falsifier: the source stops after exactly
5,120 admitted stream bytes, matching the sum of the five distinct byte owners. It does not stop
instantaneously when the receiver pauses. Resuming reads drains the chain and completes all 16
frames. A registration-order metamorphic test initially exposed a real same-time race in transient
link occupancy; the final link actor resolves each local timestamp one nanosecond later, applies
service completion before stable-sorted arrivals, and excludes that bookkeeping delta from modeled
timestamps.

## Delivered structure

- `wansim/` is a standalone Cargo workspace and is excluded by the root `Cargo.toml`. It has its
  own committed lockfile and explicit root `[patch.crates-io]` entry for the bundled nexosim.
- The requested module boundaries are present under `scenario/`, `determinism/`, `days_bridge/`,
  `transport/`, `overlay/`, and `metrics/`.
- `tests/golden/w0a_chain.toml` is the versioned golden scenario and
  `tests/golden/w0a_chain.csv` is its 999-line byte-for-byte output fixture.
- The executable reproduction surface is:

  ```sh
  cd /Users/winifred/nextmini-perfect-fec/wansim
  cargo run --release -- tests/golden/w0a_chain.toml /tmp/w0a-chain.csv
  cmp tests/golden/w0a_chain.csv /tmp/w0a-chain.csv
  ```

The simulator calls `SimInit::with_num_threads(1)` explicitly. Stable component and flow IDs are
wansim-owned; output is stable-sorted; no days global RNG, endpoint ID, or recorder enters an
artifact. The counter PRF is keyed by master seed and scenario, then domain-separated by component,
process, and draw index. Its committed vectors are conformance tests.

## Vendored identities and fork changes

`wansim/vendor/DIVERGENCE.md` is the authoritative provenance and patch ledger.

- days upstream commit: `d6a473b555d4f129c1eb62c36cb4525cdd5240ad`
- imported days tree: `33800f49fff44ee22a4e586cfef973aaad43bb06`
- upstream nexosim v1.0.0 commit: `d2207ab5c641e21c4faf3906bc74eb02e77c7e9d`
- exact days-bundled nexosim tree: `9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29`
- local nexosim changes: none

Every days change is general-purpose rather than nextmini-specific:

| Fork change | Why it is general-purpose | Enforcement |
|---|---|---|
| `TCPAck.advertised_window` and `min(cwnd, rwnd)` sender gating | Standard TCP flow control, independent of any overlay | Sender window-bound unit test |
| Finite socket send buffer with nonblocking application-write admission | Ordinary byte-stream socket behavior needed by any dynamic producer | Exact admission-bound unit test |
| Finite receive buffer and explicit application read credit | Separates transport receipt from application consumption | Window-close/reopen and bounded-delivery tests |
| Zero-window persist | Prevents a lost window update from deadlocking any TCP stream | One-byte persist-probe unit test |
| TCP_NODELAY-equivalent packetization knob | General socket packetization policy | Sub-MSS send/hold test |
| 40-byte TCP/IPv4 wire charge per segment and ACK | Correct physical serialization accounting | 600 payload bytes produce 680 wire bytes test |
| Cumulative ACK gap fix in the legacy sink | TCP correctness bug: a later segment must not ACK across a missing prefix | `[100,200)` before `[0,100)` regression |

The 4-byte big-endian frame prefix is intentionally not a TCP-fork feature. Wansim charges it once
per logical frame as application-stream data; TCP then charges its 40-byte header once per segment.
This keeps framing overhead and transport overhead separately attributable.

## W0a modeled chain

The physical graph is two forward and two reverse directed serialized links:

`source --TCP flow 10001--> relay --TCP flow 10002--> receiver`

Each hop owns a long-lived Reno sender/receiver pair and therefore independent congestion, receive
window, retransmission, and persist state. Links have finite byte queues, integer capacity/rate/
propagation configuration, and deterministic attempt-index loss. The relay grants upstream socket
read credit only as its finite application buffer has room. It reconstructs the deterministic byte
stream by TCP sequence offset, parses the 4-byte prefix, and will not admit any part of a frame to
the downstream socket until that entire frame is assembled. Once assembled, it copies bytes into
the downstream finite send buffer incrementally, matching a store-and-forward logical frame over a
stream socket.

Because days `Packet` intentionally carries no payload, W0a reconstructs deterministic test bytes
from the negotiated stream plan and delivered byte offsets. This is valid for reliable byte-stream
flow-control and framing tests, but it does not model corruption or catch a hypothetical bug that
associates the wrong payload with an otherwise correct TCP sequence. That is an explicit model
limit, not hidden shared transport state.

## Backpressure plateau and service rate

Golden geometry:

| Quantity | Value |
|---|---:|
| Logical frames | 16 |
| Innovative/application payload per frame | 508 B |
| Length prefix per frame | 4 B |
| TCP stream bytes per frame / MSS | 512 B |
| TCP/IP header per data segment | 40 B |
| Physical bytes per full segment | 552 B |
| Each hop's socket send buffer | 1,024 B |
| Each hop's socket receive buffer | 1,024 B |
| Relay application/frame buffer | 1,024 B |
| Receiver read pause | 0–200 ms |

At a steady pause, each unique byte has exactly one current owner. Bytes in a link queue or in
flight remain unacknowledged members of the corresponding send buffer, so they must not be counted
again. The closed form is:

```text
source sndbuf + hop-1 rcvbuf + relay appbuf + hop-2 sndbuf + receiver rcvbuf
= 1,024 + 1,024 + 1,024 + 1,024 + 1,024
= 5,120 bytes
```

The trace reaches exactly 5,120 admitted bytes at 37.648001 ms and admits no further source bytes
before the receiver resumes at 200 ms. After resume, it admits all 8,192 stream bytes and the
receiver completes at 264.144 ms.

At 1,000,000 bit/s, one 552-byte segment occupies the link for exactly:

```text
552 * 8 / 1,000,000 = 0.004416 seconds = 4,416,000 ns
```

The first two saturated departures are exactly 4,416,000 ns apart. This corresponds to
115,942.028986 TCP-stream B/s and 115,036.231884 logical-payload B/s for the 508-byte payload plus
4-byte-prefix frame. The latter is lower because both the TCP/IP header and length prefix are
charged. These are model service rates, not measured host or WAN throughput.

## W0a gates

The integration suite has 20 named gates covering dependency identity, stable PRF vectors, schema
validation, framing overhead, exact plateau, resume/drain, exact serialization rate, full-frame
handoff, independent hop state, deterministic loss recovery without an application gap or duplicate
frame, one-delta ACK/deadline behavior with stale generations, repeated-run CSV identity,
registration-order metamorphism, the committed golden, one worker, and mailbox capacity.

All seven modeled nexosim mailboxes reached a high-water mark of at most 2 events against capacity
256 (hop-2 reverse peaked at 1). Plumbing capacity is therefore nonbinding in this slice.

| Gate | Command | Result |
|---|---|---|
| Vendored fork format | `cd wansim/vendor/days && cargo fmt --all --check` | Green |
| Vendored fork lint | `cd wansim/vendor/days && cargo clippy --lib -- -D warnings` | Green |
| Vendored fork unit regression | `cd wansim/vendor/days && cargo test --lib` | 166 passed |
| Wansim format | `cd wansim && cargo fmt --check` | Green |
| Wansim lint | `cd wansim && cargo clippy --all-targets -- -D warnings` | Green |
| Wansim conformance | `cd wansim && cargo nextest run` | 20 passed, 0 skipped |
| Root workspace regression | `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run` | 828 passed, 17 pre-existing skips |

Cargo prints an expected warning that the vendored days package's own patch table is ignored when
days is a non-root dependency. The wansim workspace has the required explicit root patch; the
dependency-identity test and lockfile prove that both `days` and `nexosim` resolve to local paths.

## Commits

| Commit | Concern |
|---|---|
| `199d5c2` | Vendored the exact days tree and recorded provenance |
| `82f140c` | Added checked TCP socket flow control and the ACK-gap regression |
| `c1d6b6b` | Built the standalone workspace and single-chain models |
| `dfde369` | Added local tie resolution, W0a gates, scenario, and golden CSV |
| this commit | Added this report |

## Foundation assessment and W0b boundary

Days/nexosim is usable as the foundation. External models compose cleanly as a library dependency,
one-worker execution is explicit, virtual-time scheduling is fast, and a 20-test conformance
suite completes in hundredths of a second once compiled. The accepted concerns were real:

- days' original TCP source/sink could not express the required backpressure chain;
- nexosim only gives causal same-origin ordering, so cross-origin timestamp ties require local
  deferred decisions;
- its default mailbox capacity (16) is too easy to mistake for a modeled queue, so wansim assigns a
  deliberately loose 256-event plumbing capacity and instruments every external and self event;
- days uses floating-point packet timestamps, so wansim owns integer nanoseconds and converts only
  at the TCP/Packet boundary.

None is a remaining W0a wall. However, W0a does not validate the production choices that motivated
the overall simulator: sequential fan-out, per-child queues, hybrid inbox drop, DoF accounting, or
carousel/BlockAck. Those remain W0b and later work. No W0b code was started here.
