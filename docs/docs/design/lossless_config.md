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
| `fec_tree_lane_depth` | `usize` | `32` | Per-tree sender lane depth for collaborative FEC dispatch. |
| `fec_dispatch_burst` | `usize` | `1` | Max FEC symbols dispatched per sender scheduling cycle. |
| `fec_max_tree_lanes` | `usize` | `64` | Max allowed `fec_tree_ids` length at runtime preflight. |
| `fec_collaborative_multitree_enabled` | `bool` | `true` | On/off gate for collaborative multi-tree FEC mode. |
| `fec_enabled` | `bool` | `false` | Global enable switch for FEC sessions (opt-in by default). |
| `fec_require_capability` | `bool` | `true` | Reject FEC sessions unless receiver capability negotiation is present. |
| `fec_symbols_per_block_min` | `u16` | `1` | Lower bound enforced at runtime for FEC symbols-per-block. |
| `fec_symbols_per_block_max` | `u16` | `1024` | Upper bound enforced at runtime for FEC symbols-per-block. |
| `fec_symbol_size_min` | `u16` | `1` | Lower bound enforced at runtime for FEC symbol size. |
| `fec_symbol_size_max` | `u16` | `16384` | Upper bound enforced at runtime for FEC symbol size. |

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

- The older per-symbol hash strategy is not part of v3 behavior.
- Tree assignment is a dispatch-time decision driven by current backpressure and the configured `fec_tree_ids` set.

## Observability + Rollout Guardrails (v3)

- Collaborative dispatch-time assignment is the only supported multi-tree FEC behavior.
- `fec_collaborative_multitree_enabled` is an explicit rollout on/off gate only; it does not select between multiple strategies.
- Runtime preflight enforces sequential ingress for collaborative multi-tree sessions.
- Sender session-start logs include:
  - configured `fec_tree_ids`
  - `fec_tree_lane_depth`
  - `fec_dispatch_burst`
  - processor ingress policy/support status
- Sender logs now expose per-tree counters for `queued`, `sent`, `blocked`, `drained`, and `wakeups`.
- All-lanes-blocked waits and wakeups include per-tree counter snapshots to speed up backpressure diagnosis.

## Example

```toml
[lossless_runtime_config]
default_chunk_size = 8500
ready_grace_ms = 1500
fec_tree_lane_depth = 32
fec_dispatch_burst = 1
fec_max_tree_lanes = 64
fec_collaborative_multitree_enabled = true
fec_enabled = false
fec_require_capability = true
fec_symbols_per_block_min = 1
fec_symbols_per_block_max = 1024
fec_symbol_size_min = 1
fec_symbol_size_max = 16384

# Optional pacing
# [lossless_runtime_config.data_bucket]
# rate = 50_000_000    # bytes/sec
# bucket_size = 200_000  # bytes
```

## Notes

- Lossless delivery is guaranteed via TCP with back pressure.
- The `default_chunk_size` should be set considering MTU constraints; the default of 8500 bytes works well with jumbo frames.
