# Python Payload Fragmentation

_Last updated: 2025-11-10 by PurpleStone_

This document captures the implementation plan for making large Python API injections respect the dataplane MTU while
remaining transparent to existing callers. It expands on `PLAN_TO_ADD_SEGMENTATION.md` with concrete data structures,
limits, and test expectations so Tracks A–D can execute in parallel without ambiguity.

## Goals

1. Let Python clients send byte buffers larger than the dataplane MTU without manual chunking.
2. Avoid touching the controller/dataplane fast path for non-Python flows.
3. Provide bounded memory usage and actionable telemetry when fragments are dropped.
4. Keep the rollout behind a feature flag so we can stage validation per environment.

## Header specification

```
struct PyPayloadSegHeader {
    magic: u16 = 0x5047;         // "PG" marker, little-endian
    version: u8 = 1;             // bump to reject incompatible writers
    flags: u8;                   // bit0=is_fragmented, bit1=is_last_fragment
    message_id: u64;             // per-dataplane monotonically increasing id
    total_len: u32;              // full payload size before fragmentation
    fragment_index: u16;         // zero-based index of this fragment
    fragment_count: u16;         // total fragment count (1 when unfragmented)
    fragment_payload_len: u32;   // user bytes that immediately follow the header
}
```

- All fields are encoded little-endian to match existing control-plane config structs.
- Receivers validate `magic`, `version` ≤ supported, `fragment_index < fragment_count`,
  `fragment_payload_len <= mtu_payload_budget`, and cumulative length == `total_len`.
- Future extensions can reserve flag bits 2–7.
- Single-fragment payloads keep both `is_fragmented` and `is_last_fragment` cleared; the last-fragment bit is only set
  in tandem with `is_fragmented` for multi-fragment messages.

## Sender pipeline (Track B)

1. `python-api/src/lib.rs` routes every outbound buffer through a new `FragmentedPayload` helper that:
   - Reads the live MTU from `LocalConfig` (over FFI) and computes `mtu_payload_budget = mtu - 64`.
   - Rejects buffers larger than `python_fragmentation.max_message_bytes` with a descriptive Python exception when the
     feature flag is enabled; when disabled we keep the current single-packet behavior and warn.
   - Allocates `ceil(len / budget)` fragments, stamps the header, and reuses `Packet::build_ipv4_tcp_packet` per chunk.
   - Shares a thread-safe message-id generator (e.g., `AtomicU64`) across `send_to_ip`, `send_to_node`, and
     `send_batch_to_node` so batches do not double-fragment.
2. Add unit tests in `python-api/src/lib.rs` (behind `#[cfg(test)]` using the existing mock dataplane) that cover:
   - Exact MTU boundary (payload == budget) stays in a single fragment.
   - Large payload splits into N fragments with consistent header fields.
   - Feature-flag-disabled behavior refuses to send and emits log.

## Reassembly & delivery (Track C)

1. Introduce `PythonReassemblyBuffer` under `dataplane/src/node/python/` that holds a `HashMap<(FlowId, message_id),
   FragmentAccumulator>` with:
   - Arrival timestamp (monotonic), expected fragment_count/total_len, and `Vec<Vec<u8>>` or contiguous `BytesMut`.
   - Aggregate byte counter per flow to enforce `reassembly_window_bytes`.
2. `processor.rs` inspects each packet destined for the Python interface; if the header magic/version match, it:
   - Drops + logs if fragmentation is disabled or header invalid.
   - Inserts the fragment into the buffer, marking completion when all fragments are present or `is_last_fragment` with
     contiguous byte count == `total_len`.
   - Once complete, enqueues a synthetic packet (without IPv4/TCP headers) to `PythonInterfaceHandle`.
3. `PacketReceiver::recv` gains an option (default `PayloadOnly`) that yields the reconstructed bytes, metadata about
   the flow (flow id, src/dst ip), and a `SegmentationInfo` struct for introspection. A compatibility mode toggles back
   to raw packet delivery for legacy tooling until migrations finish.

## Failure handling & telemetry

- Each buffer enforces `fragment_timeout_ms`; expired groups log `warn!` with flow + message id and emit a
  `DataplaneToController::PythonFragmentEvents` message with `kind = "timeout"` whenever `trace_flow_events` is enabled.
- When `reassembly_window_bytes` would be exceeded, the assembler drops the fragment and emits a matching controller
  event with `kind = "window_overflow"`. Parser/assembler guardrails report `kind = "invalid_header"` or
  `kind = "assembler_drop"` so operators can distinguish unhealthy senders.
- Every `PythonFragmentEvent` entry contains the originating `node_id`, `flow_id`, optional `message_id`,
  a human-readable `detail`, and (for timeouts) the number of `missing_fragments`. Controller logs now surface these
  records so downstream tooling can alert on repeated issues.
- `PythonInterfaceHandle` now maintains in-memory counters (accessible via `metrics_snapshot()` and ready for export
  through a future `metrics::Registry` hook). The dataplane also emits periodic
  `DataplaneToController::PythonFragmentMetrics` snapshots (every 5 s) so controllers and downstream alerting can
  ingest the totals. Each snapshot carries:
  - `python_fragments_received_total`
  - `python_fragments_dropped_invalid_header_total`
  - `python_reassembly_timeout_total`
  - `python_reassembly_window_overflow_total`

## Configuration surfaces (Track A)

```
[pipeline.python_fragmentation]
enabled = false
max_message_bytes = 65536
reassembly_window_bytes = 262144
fragment_timeout_ms = 1000
trace_flow_events = true
```

- Situated under `LocalConfig::python_fragmentation` and mirrored into controller config so that Python tooling can read
  the live values via the existing config RPC.
- Docs: `docs/docs/design/configuration.md`, `docs/examples/pytorch_python_api.md`, and the new smoke script should call
  out how to size the MTU and message limits.

## Testing & validation (Track D)

1. Rust unit tests (sender + reassembly) as described above.
2. Integration script `docs/testing/scripts/python_fragmentation_smoke.py`:
   - Creates a 2 MiB buffer, sends via python-api, asserts reassembled payload matches.
   - Parameterized MTU and timeout to stress edge cases.
3. CI hook: new make target `cargo test --package python-api --package dataplane --lib fragmentation` plus an optional
   `uv run` invocation for the smoke script (marked as `ignored` by default). Documentation will describe manual steps.

## Open questions

- Do we need a cross-host compatibility story if some nodes run old dataplanes lacking the header? (Current answer:
  flag remains `false` until every node is upgraded.)
- Should we reuse QUIC stream framing instead of custom headers? That would require deeper invasive changes, so we keep
  the payload header for now.
