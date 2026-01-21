# Lossless Session Configuration

This page covers lossless session settings. For a complete list of all configuration options, see the [Configuration Reference](config-reference.md).

## Overview

Defines default behavior for the lossless session engines used by the dataplane.

## Location

- Rust struct: `dataplane/src/node/config.rs` → `LosslessConfig`.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_chunk_size` | `usize` | `8500` | Default payload chunk size in bytes. |
| `data_bucket` | `Option<TokenBucketSpec>` | `None` | Optional token bucket for data pacing. |
| `ready_grace_ms` | `u64` | `1500` | Grace window (ms) before the sender starts streaming when not all receivers have reported `Ready`. |

### TokenBucketSpec Fields

| Field | Type | Description |
|-------|------|-------------|
| `rate` | `usize` | Pacing rate in bytes per second. |
| `bucket_size` | `usize` | Token bucket size in bytes. |

## Session Coordination

Session coordination uses deterministic session IDs derived from flow metadata (controller-assigned flows) or `(group_id, source_node_id)` for Python multicast helpers. Receivers must be registered before send; if no receiver is active for a session, inbound frames are dropped.

## Example

```toml
[lossless_runtime_config]
default_chunk_size = 8500
ready_grace_ms = 1500

# Optional pacing
# [lossless_runtime_config.data_bucket]
# rate = 50_000_000    # bytes/sec
# bucket_size = 200_000  # bytes
```

## Notes

- Lossless sessions implement an application-level framing and acknowledgement protocol (`messages/src/lossless_session.rs`) on top of the normal dataplane forwarding path.
- The `default_chunk_size` should be set considering MTU/MSS constraints and memory usage. The sender uses the configured value; it does not automatically clamp chunk sizes to `mtu`.
