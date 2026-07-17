# wansim W0b report — fan-out, runtime admission, and DoF vertical slice

Date: 2026-07-17

Branch: `perfect-fec-runtime`

Scope: W0b only; no section-P feedback or W1 experiments

Evidence class: deterministic model-level evidence, not a WAN measurement or calibrated twin

## Verdict

**W0b is complete and ready for review.** The W0a source → relay → receiver chain now has a
separate W0b tree implementation with the planned topology:

```text
sender → relayA → receiver1
               ↘ relayB → receiver2
                        ↘ receiver3
```

All five overlay hops are independent, long-lived TCP connections with distinct flow IDs,
congestion-control state, send/receive buffers, retransmission timers, forward links, and reverse
ACK links. No TCP segment is multicast. Relay child vectors retain controller order exactly as
provided; the validation set checks membership but never rewrites the vector.

The receiver models the production boundary in this order:

```text
hop TCP receive and cumulative ACK
  → transport application read / frame assembly
  → node-local shared runtime-command mailbox
  → control/data split
  → bounded data inbox (drop on full) or exempt control lane
  → serial decoder/sink service center
  → ideal DoF bucket
```

W0b still sends exactly K unique source frames and has no protocol feedback. The codec remains an
explicit ideal-code abstraction: rank is `min(K, distinct innovative frame IDs)`.

## Implementation

- `TreeScenario` validates the fixed topology, K/frame-count equality, all byte/frame capacities,
  checked geometry, child-queue frame fit, receiver timing, and controller child membership.
- `FanoutRelayEndpoint` assembles a complete length-prefixed frame before creating child copies.
  Each child owns a bounded queue and a separate `TcpSocketSender`. Queue bytes are released only
  when copied into that child's finite socket send buffer.
- Sequential admission visits each frame's children in the configured order and stops on the first
  unavailable child. Concurrent admission advances each child's ordered frame stream independently.
- `TreeReceiverEndpoint` emits the TCP ACK before placing a completed frame in the modeled shared
  runtime-command mailbox. Runtime service later selects the control or data lane. A full data lane
  drops at that application boundary; it does not retract the prior TCP delivery or ACK. The
  control lane has its own capacity and never consumes data-lane capacity.
- Decoder/sink work is one explicit non-overlapping service center with configured service time.
  Inbox capacity is released when service takes a frame, matching an `mpsc` receive before decode.
- Twenty-five named byte owners are tracked separately. Physical-link queue/in-flight bytes are
  not counted again because they remain retained by the corresponding unacknowledged TCP send
  buffer.
- The CLI dispatches the original W0a scenario when `scenario_kind` is absent and the W0b tree
  when it is `tree`; the W0a golden remains byte-identical.

The receiver actors live on three different simulated nodes. Each therefore has its own node-local
runtime command mailbox. Production has one shared runtime actor per node; with one receiver
session per node in W0b, these are equivalent. Cross-session contention in that shared stage is
not claimed and belongs in a later scenario.

## Closed-form tree plateau

The committed golden uses 512-byte framed stream units, 1,024-byte socket buffers, 1,024-byte
per-child queues, and 1,024-byte relay application buffers. All receiver transport readers remain
paused through the ownership probe at 199 ms.

The unique source-stream admission plateau is controlled by relayA's direct first-child path:

```text
source sndbuf
  + relayA upstream rcvbuf
  + relayA application buffer
  + relayA→receiver1 child queue
  + relayA→receiver1 sndbuf
  + receiver1 rcvbuf
= 1,024 × 6
= 6,144 source bytes admitted
```

At steady state, the first 3,072 source bytes have already been replicated into all three paused
leaf chains, while the next 3,072 bytes occupy the source-to-relayA prefix. Resident byte copies
are therefore:

```text
source-to-relayA prefix + 3 × paused leaf chain
= (1,024 + 1,024 + 1,024)
  + 3 × (1,024 child queue + 1,024 sndbuf + 1,024 rcvbuf)
= 3,072 + 9,216
= 12,288 resident bytes
```

Every tracked owner at the probe is enumerated below; zeroes are intentional rather than omitted.

| Owner | Bytes |
|---|---:|
| `source.sndbuf` | 1,024 |
| `relay_a.upstream_rcv` | 1,024 |
| `relay_a.application` | 1,024 |
| `relay_a.receiver1.queue` | 1,024 |
| `relay_a.receiver1.sndbuf` | 1,024 |
| `receiver1.tcp_rcv` | 1,024 |
| `relay_a.relay_b.queue` | 0 |
| `relay_a.relay_b.sndbuf` | 0 |
| `relay_b.upstream_rcv` | 0 |
| `relay_b.application` | 0 |
| `relay_b.receiver2.queue` | 1,024 |
| `relay_b.receiver2.sndbuf` | 1,024 |
| `receiver2.tcp_rcv` | 1,024 |
| `relay_b.receiver3.queue` | 1,024 |
| `relay_b.receiver3.sndbuf` | 1,024 |
| `receiver3.tcp_rcv` | 1,024 |
| `receiver1.runtime_command` | 0 |
| `receiver1.data_inbox` | 0 |
| `receiver1.decoder_sink` | 0 |
| `receiver2.runtime_command` | 0 |
| `receiver2.data_inbox` | 0 |
| `receiver2.decoder_sink` | 0 |
| `receiver3.runtime_command` | 0 |
| `receiver3.data_inbox` | 0 |
| `receiver3.decoder_sink` | 0 |
| **Total** | **12,288** |

`paused_tree_reaches_the_exact_enumerated_byte_ownership_plateau` asserts the full map, both closed
forms, and the 6,144-byte source cursor rather than checking only a total.

## Sequential versus concurrent child admission

The matched externality scenario pauses receiver1's transport reader until 200 ms while receiver2
and receiver3 read immediately. RelayA's application buffer is provisioned to the complete
8,192-byte stream so this comparison isolates the child-order admission rule from a separate
finite-ingress-memory limit. Everything else, including TCP/link state and trace, is identical.

| Metric | Sequential | Concurrent |
|---|---:|---:|
| First receiver1 child block | 31.912001 ms | 31.912001 ms |
| Frame at first block | 6 | 6 |
| Accounted downstream bytes at block | 3,072 | 3,072 |
| Frames admitted toward relayB before 200 ms | 6 (IDs 0–5) | 16 (IDs 0–15) |
| Receiver2 completion | 258.742001 ms | 82.598002 ms |
| Receiver3 completion | 258.742001 ms | 82.598002 ms |
| Receiver1 completion | 264.254001 ms | 264.254001 ms |

The 3,072-byte first-block value is exactly receiver1's child queue + hop send buffer + hop receive
buffer. Thus the sequential externality is not injected instantaneously: six complete frames enter
both relayA children before the real downstream chain fills. Concurrent mode then lets the relayB
child continue through frame 15 despite receiver1's blocked child.

One important qualification was clearer in the implementation than in the prose plan: concurrent
admission removes the **configured-order** externality, not conservation-induced coupling under
arbitrarily long stalls and finite memory. A reliable relay must retain the blocked child's copy
somewhere. If relayA's application buffer were also exhausted, both variants would eventually
backpressure upstream. The test provisions that buffer to the finite stream size specifically to
measure the admission policy rather than pretending this physical limit disappears.

## Hybrid-drop ordering evidence

The drop-order scenario gives each data inbox one frame, holds decoder service until 500 ms, and
sets runtime-command service to 20 ms. Every receiver drops 15 later data frames, but all transport
connections remain reliable and live.

For receiver1's first dropped frame (frame 1, ending at TCP sequence 1,024):

| Causal event | Time |
|---|---:|
| Covering TCP ACK emitted by receiver1 | 15.248001 ms |
| Covering ACK arrived at relayA's hop sender | 16.568001 ms |
| Runtime command dispatched and data inbox refused frame | 50.832001 ms |

The drop record carries `acked_through = 1,024`, and the test requires a covering ACK to have
arrived back at the sender before the drop. This is stronger than checking only source-code order
inside the receiver handler. `shared_runtime_command_mailbox_precedes_the_control_data_split`
separately pins transport delivery → shared command enqueue → later data-lane admission for every
golden frame. The unit gate
`full_data_lane_drops_data_without_consuming_control_capacity` fills the data lane and proves a
control command is still admitted.

## Determinism, conservation, and mailbox gates

- The 2,939-line `w0b_tree.csv` fixture reproduces byte-for-byte across runs.
- Forward and reverse nexosim model-registration orders produce identical CSV.
- The provisioned no-drop scenario delivers frame IDs 0–15 exactly once to every receiver, reaches
  DoF rank K=16 at each, and preserves order across all four relay child streams.
- Each forward physical link sees exactly one of the five distinct TCP flow IDs; no emitted segment
  is broadcast to multiple links.
- All 16 nexosim mailboxes are explicitly instrumented. The maximum observed high-water mark is 3
  events out of capacity 256, on leaf reverse links; plumbing remains nonbinding under this fan-out
  and ACK fan-in.
- Decoder service start/finish records differ by exactly the configured 500,000 ns and service
  intervals never overlap.

The W0b tests are in `wansim/tests/w0b_gates.rs`; the hybrid-lane and DoF duplicate tests are
co-located with their owning modules.

## Reproduction and gates

Golden reproduction:

```sh
cd /Users/winifred/nextmini-perfect-fec/wansim
CARGO_INCREMENTAL=0 cargo run --release -- \
  tests/golden/w0b_tree.toml /tmp/w0b-tree.csv
cmp tests/golden/w0b_tree.csv /tmp/w0b-tree.csv
```

Final gates:

| Gate | Command | Result |
|---|---|---|
| Wansim format | `cd wansim && CARGO_INCREMENTAL=0 cargo fmt --check` | Green |
| Wansim lint | `cd wansim && CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings` | Green |
| Wansim conformance | `cd wansim && CARGO_INCREMENTAL=0 cargo nextest run` | 36 passed, 0 skipped |
| Root regression | `CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 cargo nextest run` | 828 passed, 17 skipped |

The expected Cargo warning about the nested vendored-days patch table remains unchanged from W0a;
the wansim workspace root supplies the effective nexosim path patch.

## Commits

| Commit | Concern |
|---|---|
| `af5abef` | W0b tree configuration, actors, independent hop transport, runtime admission, DoF, and ownership model |
| `2f32f0c` | W0b conformance tests and committed tree scenario/CSV golden |
| this commit | W0b measurements and report |

## W1 boundary

No W1 code was started. W0b has no BlockAck, carousel, rounds, control-route transport, repair
emission, or protocol completion handshake. It establishes only the WAN pipeline substrate those
endpoints will use after this stage is reviewed.
