# Vendored days / nexosim provenance and divergence

This directory is a source vendor, not a generated Cargo cache.

## Upstream identities

- `days/` upstream: <https://github.com/iQua/days>
- Imported days commit: `d6a473b555d4f129c1eb62c36cb4525cdd5240ad`
- Imported days tree: `33800f49fff44ee22a4e586cfef973aaad43bb06`
- Import method: `git archive` of that commit, so only files tracked by upstream are present.
- days license: `AGPL-3.0-only` (see `days/LICENSE`).

The days commit bundles its nexosim fork under `days/crates/nexosim` rather than using the
crates.io source selected by the dependency declaration. Its exact imported tree identity is:

- upstream project: <https://github.com/asynchronics/nexosim>
- declared upstream release: `v1.0.0`
- upstream release commit: `d2207ab5c641e21c4faf3906bc74eb02e77c7e9d`
- days-bundled nexosim tree: `9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29`
- nexosim license: `MIT OR Apache-2.0` (see `days/crates/nexosim/LICENSE-*`).

The bundled tree already differs from the pristine nexosim `v1.0.0` source. Those inherited
differences belong to the pinned days revision; wansim does not attempt to reconstruct or rewrite
their history. The days commit plus the subtree identity above pins them byte-for-byte.

## Wansim-local patches

The initial import commit has no source changes. The following patches are applied on top of it:

1. **Correct cumulative ACK generation across reassembly gaps.** The legacy TCP sink now advances
   `RCV.NXT` only through contiguous ranges rather than assigning the end of the first sorted
   range. A regression delivers sequence range `[100, 200)` before `[0, 100)` and pins ACKs `0`,
   then `200`. Affected file: `days/src/flows/tcp_sink.rs`.
2. **Carry and honor advertised receive windows.** `TCPAck` has a backwards-compatible
   `advertised_window`; the legacy fixed-flow sender gates its congestion window by the peer window
   and the legacy unbounded sink advertises `usize::MAX`. Affected files:
   `days/src/flows/packet.rs`, `days/src/flows/tcp_source.rs`,
   `days/src/flows/tcp_sink.rs`, and `days/tests/switch.rs`.
3. **Add a dynamic byte-stream socket state machine.** The general-purpose `tcp_socket` module adds
   finite send-buffer write admission, finite receive buffering, application read credit,
   `min(cwnd, rwnd)` gating, zero-window persist, a TCP_NODELAY-equivalent packetization option,
   retransmission timers, and explicit 40-byte TCP/IPv4 serialization overhead. It deliberately
   contains no nextmini frame or relay semantics. Affected files: `days/src/flows/tcp_socket.rs` and
   `days/src/flows/mod.rs`.

No wansim-local change is made to `days/crates/nexosim`.

The nextmini 4-byte logical-frame length prefix is charged by wansim's framing layer, not by the
TCP fork: it is application data once per logical frame, whereas the 40-byte TCP/IP charge is
transport overhead once per segment. Keeping those identities separate prevents double counting.
