# Reliable Session Configuration

## Overview

- Defines default behavior for the reliable session engines when the `reliable` feature is enabled in the dataplane.

## Location

- Rust struct: `dataplane/src/node/config.rs` → `ReliableConfig`.

## Fields

- `default_chunk_size: u32` — default payload chunk size in bytes.

- `data_bucket: Option<TokenBucketSpec>` — pacing for data flows.

- Session coordination uses deterministic session IDs derived from flow metadata (controller-assigned flows) or `(group_id, source_node_id)` for Python multicast helpers. Receivers must be registered before send; if no receiver is active for a session, inbound frames are dropped.

- `ready_grace_ms: u64` — grace window before the sender starts streaming when not all receivers have reported `Ready` (default 1500 ms).

## Example

```toml
[reliable]
default_chunk_size = 4096
# Optional pacing
# [reliable.data_bucket]
# rate = 50_000_000  # bytes/sec
# burst = 200_000    # bytes
```

## Notes

- Reliable delivery is guaranteed via TCP with back pressure.
