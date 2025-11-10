# Python payload fragmentation

Large tensors injected via `nextmini_py` now travel through a purpose-built fragmentation pipeline so senders no longer
need to slice payloads manually. This document captures the shipped behavior, header format, configuration surface, and
operational guidance for the `PyPayloadSeg` feature.

## Why it exists

- Python callers frequently emit payloads that exceed the dataplane MTU (default 1 400 B). The legacy single-packet
  path silently violated the MTU, resulting in drops or truncated IPv4 headers.
- Manually chunking tensors in user space duplicated logic across scripts and made it hard to correlate drops with the
  rust-side transport.
- Operators needed telemetry whenever fragment groups timed out, were evicted, or exceeded buffers.

## Header layout

Every fragmented payload starts with a fixed 24-byte header encoded in little-endian order. Single-fragment payloads also
include the header so receivers can rely on a uniform format.

```
struct PyPayloadSegHeader {
    magic: u16 = 0x5047;         // "PG"
    version: u8 = 1;             // increment to reject incompatible writers
    flags: u8;                   // bit0=is_fragmented, bit1=is_last_fragment
    message_id: u64;             // monotonically increasing per dataplane
    total_len: u32;              // full logical payload length
    fragment_index: u16;         // zero-based index
    fragment_count: u16;         // total number of fragments
    fragment_payload_len: u32;   // number of user bytes that follow this header
}
```

The header size plus IPv4 (20 B) and TCP (20 B) headers leave `mtu - 64` bytes for user data per fragment. For the
default MTU of 1 400 B this yields 1 336 B per chunk.

## Sender behavior

The Python bindings call `transmit_python_payload` whenever `send_to_node`, `send_to_ip`, or `send_batch_to_node` is
invoked. The helper:

1. Checks `LocalConfig::python_fragmentation_enabled`.
2. Enforces `python_fragmentation_max_message_bytes`; oversize payloads result in a Python exception before any packet
   leaves the process.
3. Splits the `FrozenBuffer` into `ceil(len / chunk_budget)` slices, allocates a message id from a process-wide
   `AtomicU64`, and encodes the header for each fragment.
4. Builds an IPv4/TCP packet per fragment and hands it to `ProcessorHandle::process_packet`.

When fragmentation is disabled the helper sends a single packet and assumes the payload already respects the MTU.

## Receiver behavior

`PythonInterfaceHandle` inspects each packet delivered to Python receivers:

- For payload-only receivers it removes IPv4/TCP headers, validates the `PyPayloadSeg` header, and forwards the payload
  if the message contains a single fragment.
- When a payload spans multiple fragments the handle buffers them by `(flow_id, message_id)` inside a bounded
  `FragmentAssembler`. Once every fragment arrives (or the last fragment flag is set and byte counts match) the payload
  is coalesced and enqueued as a `PythonDelivery::Payload`.
- If the receiver was registered in raw mode, packets are delivered untouched.

The assembler enforces:

- `python_fragmentation_reassembly_window_bytes`: cap on in-flight fragment bytes per flow.
- `python_fragmentation_fragment_timeout_ms`: deadline for the oldest fragment in a message.
- Strict header validation so malformed payloads are dropped before they can starve the queues.

`PayloadDelivery` objects expose `.message_id`, `.total_len`, `.fragment_count`, and `.payload_format` so Python code can
trace individual fragment groups.

## Configuration knobs

Set these fields in the node config (or via CLI flags) before enabling the feature:

| Field | Default | Purpose |
| --- | --- | --- |
| `python_fragmentation_enabled` | `false` | Feature flag. When `false`, the sender emits a single packet per `FrozenBuffer`. |
| `python_fragmentation_max_message_bytes` | `65536` | Upper bound enforced by the Python sender. Keep it at or below `mtu - 64` unless you have a larger MTU. |
| `python_fragmentation_reassembly_window_bytes` | `262144` | Per-flow memory budget for in-flight fragments. |
| `python_fragmentation_fragment_timeout_ms` | `1000` | Timeout for incomplete fragment groups. |
| `python_fragmentation_trace_flow_events` | `true` | Enables controller events + periodic metrics exports when fragments are dropped. |

Remember to restart the dataplane after editing the config or passing new CLI overrides.

## Telemetry and failure modes

When tracing is enabled the dataplane emits `DataplaneToController::PythonFragmentEvents` with a `kind` value of:

- `invalid_header` – header magic/version mismatch, impossible indexes, or payload len inconsistencies.
- `assembler_drop` – the assembler was disabled or encountered an unexpected state.
- `window_overflow` – accepting the fragment would exceed `reassembly_window_bytes` for its flow.
- `timeout` – fragment group failed to complete before `fragment_timeout_ms`.

Each event includes the `node_id`, `flow_id`, optional `message_id`, and a human-readable `detail`.

In addition to the events the dataplane periodically publishes `DataplaneToController::PythonFragmentMetrics` snapshots
containing:

- `python_fragments_received_total`
- `python_fragments_dropped_invalid_header_total`
- `python_reassembly_timeout_total`
- `python_reassembly_window_overflow_total`

These counters can be scraped or logged for long-term alerting even when flow-level tracing is disabled.

## Validation workflow

Use `docs/testing/scripts/python_fragmentation_smoke.py` (documented in
[`docs/docs/testing/python_fragmentation_smoke.md`](../testing/python_fragmentation_smoke.md)) to exercise the end-to-end
pipeline:

1. Start a dataplane with fragmentation enabled.
2. Run the script with a multi-megabyte payload to force fragmentation.
3. Confirm the receiver reports the reconstructed byte length and that no controller events are emitted during a healthy
   run.

Unit tests under `python-api/src/lib.rs` cover the fragment builder, while `dataplane/src/node/python/interface.rs`
contains reassembly tests.
