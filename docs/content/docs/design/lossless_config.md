---
title: "Lossless Session Configuration"
description: ""
---


This page covers lossless session settings. For a complete list of all configuration options, see the [Configuration](/docs/config) section.

## Overview

Defines default behavior for the lossless session engines used by the dataplane.

Python `send_data(...)` / `receive_data(...)` / `receive_data_async(...)` calls are FEC-oblivious:
FEC tuning belongs to `[lossless_runtime_config]` and is not passed as Python `fec_*` kwargs.

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
| `fec_default_symbols_per_block` | `u16` | `32` | Runtime-derived default `FecManifest.symbols_per_block` before preflight. |
| `fec_symbol_size_policy` | `FecSymbolSizePolicy` | `"chunk_size"` | Default symbol-size policy: `chunk_size` follows session chunk size, `fixed` uses `fec_default_symbol_size`. |
| `fec_default_symbol_size` | `u16` | `8500` | Fixed default `FecManifest.symbol_size` when `fec_symbol_size_policy="fixed"`. |
| `fec_tree_ids_source` | `FecTreeIdsSource` | `"config"` | Source for internal sender tree-id allowlist (`config` or `installed_routes`). |
| `fec_default_tree_ids` | `Vec<u16>` | `[0]` | Config fallback tree-id allowlist used when `fec_tree_ids_source="config"`. |
| `fec_symbols_per_block_min` | `u16` | `1` | Lower bound enforced at runtime for FEC symbols-per-block. |
| `fec_symbols_per_block_max` | `u16` | `1024` | Upper bound enforced at runtime for FEC symbols-per-block. |
| `fec_symbol_size_min` | `u16` | `1` | Lower bound enforced at runtime for FEC symbol size. |
| `fec_symbol_size_max` | `u16` | `16384` | Upper bound enforced at runtime for FEC symbol size. |

### FEC Runtime-Derived Defaults

- Runtime derives default `symbols_per_block` from `fec_default_symbols_per_block`, canonicalized into `fec_symbols_per_block_[min,max]`.
- Runtime derives default `symbol_size` from `fec_symbol_size_policy`:
  - `chunk_size`: use the session `chunk_size`.
  - `fixed`: use `fec_default_symbol_size`.
- Runtime resolves tree IDs from `fec_tree_ids_source` / `fec_default_tree_ids`, then canonicalizes to sorted + unique before sender startup checks.
- Python callers do not provide per-session FEC overrides; runtime config is the source of truth.

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

- Multi-tree senders use a runtime-resolved `fec_tree_ids` allowlist.
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

### Packet-Path Integration (Dataplane Processor)

- Lossless FEC packets are classified in the dataplane processor by reading tree id from packet payload (`packet.lossless_fec_tree_id()`), not from out-of-band metadata.
- The processor uses tree-aware route resolution:
  - `route::RoutingTable::get_next_hops_by_flow_and_tree(flow_id, tree_id, reporter)` for per-packet forwarding.
  - if no tree id is present, it follows the legacy flow-only lookup path.
- `Sequential` ingress behavior:
  - one `mpsc` queue per processor worker plus deterministic lane hashing.
  - FEC packets route to an ingress lane via hash of `(flow_id, tree_id)` so concurrent trees can be distributed across lanes.
- `Concurrent` ingress behavior:
  - one shared `flume` queue for all workers; FEC packets still work, but tree-level lane partitioning is not available.
  - the sender-side runtime therefore enforces `Feature::Sequential` for collaborative multi-tree sessions.
- At sender session preflight time (`fec_collaborative_multitree_enabled` true), the runtime enforces:
  - `ingress_feature == Feature::Sequential`
  - `ingress_channel_backpressure == true`
  - otherwise session startup is rejected with a preflight error (`Runtime check failed` style diagnostic in logs).

### Sender Preflight Matrix

`LosslessRuntime::spawn_sender` derives sender policy in `derive_sender_policy` and rejects startup for any hard error:

- Non-FEC sessions:
  - `fec_enabled=false` yields `SenderPolicy { manifest=None, tree_ids=[] }` and no preflight block.
- FEC sessions:
  - `fec_enabled=true` + `fec_require_capability=false` -> `CapabilityRequirementDisabled`.
  - `fec_tree_ids_source=installed_routes` -> `InstalledRoutesTreeIdsUnsupported` (not implemented yet).
  - `fec_tree_lane_depth==0` -> `InvalidTreeLaneDepth`.
  - `fec_dispatch_burst==0` -> `InvalidDispatchBurst`.
  - `fec_max_tree_lanes==0` -> `InvalidMaxTreeLanes`.
  - `fec_default_tree_ids` empty after canonicalization -> `MissingTreeIds`.
  - Duplicate/unsorted tree IDs -> `TreeIdsMustBeSortedUnique`.
  - Configured count exceeds `fec_max_tree_lanes` -> `TooManyTreeIds`.
  - More than one tree selected while `fec_collaborative_multitree_enabled=false` -> `CollaborativeMultiTreeDisabled`.
  - More than one tree while ingress is not `Feature::Sequential` -> `MultiTreeRequiresSequentialIngress`.
  - More than one tree while `ingress_channel_backpressure=false` -> `MultiTreeRequiresIngressBackpressure`.
  - Derived manifest outside bounds:
    - symbols-per-block out of `[fec_symbols_per_block_min, fec_symbols_per_block_max]`
    - symbol-size out of `[fec_symbol_size_min, fec_symbol_size_max]`
    - `chunk_size` exceeding derived `symbol_size`
    - fixed symbol-size policy failure to derive from `chunk_size`.

Any preflight rejection is logged as `Lossless runtime: rejected sender session during deterministic preflight` and returned to the caller as `PreflightError`.

### Sender Runtime State Machine

The sender event loop (`sender::run`) transitions through three control gates before emitting any data:

1. **Topology gate** (`topology_gate_open`): false until either `set_topology_ready(true)` has been observed or the runtime is configured as already ready.
2. **Manifest gate**: sends MANIFEST once topology is open.
3. **Ready gate** (`ready_gate_open`): opens when all peers are ready and FEC preflight checks are satisfied (if FEC mode).

Non-FEC senders rely on cumulative ACK progress; FEC senders switch retirement to FEC status tracking and skip ACK consumption.

- MANIFEST is retried every `MANIFEST_RETRY_INTERVAL_MS` (250 ms) while not ready.
- In FEC mode, if all peers are not ready after `ready_grace_ms`, sender proceeds with:
  - warning when missing READY only (legacy mode),
  - abort when missing capability or compatibility conditions.

### FEC Symbol Dispatch Pipeline

On first FEC symbol emission, the sender initializes `FecTreeDispatch`:

- one dispatch lane per `tree_id` in canonical `fec_tree_ids`,
- one bounded MPSC queue per lane with depth `fec_tree_lane_depth`,
- round-robin starting point (`next_rr_idx`) and shared notify handle for wakeups.

`drive_fec_scheduler` loops in bursts:

- enqueue one new FEC block when window/capacity allows,
- emit up to `fec_dispatch_burst` symbols per cycle,
- for each symbol, call `FecTreeDispatch::try_enqueue`.

`try_enqueue` behavior:

- `Queued(tree_id)`: counters increment (`queued`, `sent`, outstanding symbol count).
- `AllBlocked`: returns the symbol to front of local scheduler queue and sets `all_lanes_blocked=true`.
- `Closed`: closes session as preflight-failed with an error.

When all lanes are blocked, the main loop waits with `Notify` wakeup or control-frame receive for `ALL_FEC_LANES_BLOCKED_WAIT_MS` (250 ms). Wakeups are emitted whenever lane receive slots free.

`FecTreeDispatch` tracks per-tree counters `queued/sent/blocked/drained/wakeups` and exposes snapshots in logs.

### Receiver-Side and Control-Plane Semantics

`sender::handle_control` handles control frames differently by mode:

- `Ready`: records ready nodes for the ready gate.
- `FecCapabilities`: validates against requested manifest via `control::ensure_fec_compatible`.
- `FecStatus`: updates per-receiver FEC completion watermark in `SenderState` and retirement limits.
- `Ack`: ignored in FEC mode, used only for non-FEC cumulative completion.

`receiver::run` emits:

- `Ready` immediately on startup,
- `Ack` (batched every 16 contiguous chunks or completion edges),
- `FecCapabilities` (requested only if runtime advertises FEC),
- `FecManifest` / `Manifest` during handshakes as needed.

Receiver side drops FEC symbols that arrive before manifest or with invalid payload lengths, and drops all unknown frame formats after warning.

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
fec_default_symbols_per_block = 32
fec_symbol_size_policy = "chunk_size"
fec_default_symbol_size = 8500
fec_tree_ids_source = "config"
fec_default_tree_ids = [0]
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
