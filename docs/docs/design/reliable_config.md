# Reliable Multicast Configuration

## Overview

- Defines default behavior for the reliable multicast engines when the `reliable` feature is enabled in the dataplane.

## Location

- Rust struct: `dataplane/src/node/config.rs` → `ReliableConfig`.

## Fields

- `default_chunk_size: u32` — default payload chunk size in bytes.

- `control_weight: u32` — WRR weight for control flows to avoid starvation.

- `data_bucket: Option<TokenBucketSpec>` — pacing for data flows.

- Session coordination happens via explicit session IDs. Senders still allocate via `Dataplane.send_file(..., session_id=...)` (or allow the runtime to pick one), but receivers can now omit the `session_id`. When `Dataplane.receive_file` is invoked without an ID, the dataplane waits for the first inbound manifest, adopts the sender's session ID automatically, and only then spawns the reliable receiver. Advanced orchestrators may still pre-register IDs with `Dataplane.reliable_register_session_id(...)` when they need to short-circuit the wait.

- `ready_grace_ms: u64` — grace window before the sender starts streaming when not all receivers have reported `Ready` (default 1500 ms).

## Example

```toml
[reliable]
default_chunk_size = 4096
control_weight = 10
# Optional pacing
# [reliable.data_bucket]
# rate = 50_000_000  # bytes/sec
# burst = 200_000    # bytes
```

## Notes

- Reliable delivery is guaranteed via TCP with back pressure.
