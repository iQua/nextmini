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

## Multi-Tree FEC Contract (v3 Collaborative)

This contract defines sender-side tree selection for collaborative multi-tree FEC scheduling and supersedes the older hash-based assignment model.

### Tree-ID Set Invariants

- Multi-tree senders use an explicit `fec_tree_ids` allowlist.
- `fec_tree_ids` is canonicalized as sorted ascending and unique.
- Sender emission is limited to IDs in `fec_tree_ids`.
- Multi-tree mode requires at least two IDs in `fec_tree_ids`.
- Empty `fec_tree_ids` is invalid and must be rejected.
- Single-tree FEC is valid only when explicitly configured as single-tree; in that case, exactly one tree ID may be used.

### Deterministic Tie-Breakers

- Collaborative sender scheduling uses deterministic selection order over canonical `fec_tree_ids`.
- When multiple trees are writable at a scheduling decision, choose the first candidate in deterministic round-robin order (wrapping over the sorted ID list).

### Hash Assignment Is Removed

- The older per-symbol strategy (`hash(...) % num_trees`) is not part of v3 behavior.
- Tree assignment is a dispatch-time decision driven by current backpressure and the configured `fec_tree_ids` set.

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

- Lossless delivery is guaranteed via TCP with back pressure.
- The `default_chunk_size` should be set considering MTU constraints; the default of 8500 bytes works well with jumbo frames.
