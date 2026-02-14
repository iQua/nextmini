---
title: "Dataplane Configuration"
description: "Reference for dataplane startup inputs, node and network settings, runtime state, and lossless runtime configuration."
---

The dataplane configuration file (typically `config.toml` or `node.toml`) defines node-specific settings.

## Startup CLI Inputs

| Input | Default | Description |
|-------|---------|-------------|
| `CONTROLLER_ADDR` (positional) | unset | Optional positional override for `controller_addr`. When provided, it takes precedence over values from the config file and CLI flags. |
| `--config-path` | `config.toml` | Path to the dataplane config file loaded at startup (`config_path`). |

## Controller Connection

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `controller_addr` | `String` | `""` | `--controller-addr` | WebSocket address of the controller (for example, `ws://192.168.1.1:3000`). A plain `host:port` value is normalized to `ws://host:port`. |
| `controller_connect_timeout_ms` | `u64` | `5000` | (not exposed) | Timeout in milliseconds for the initial WebSocket connection to the controller. Config-file only. |

## Node Identity

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `node_id` | `usize` | `0` | `--node-id` | Unique node identifier (auto-assigned if 0). |
| `n_nodes` | `usize` | `1` | `--n-nodes` | Total number of dataplane nodes. |
| `node_id_offset` | `usize` | `0` | `--node-id-offset` | Offset added to computed node IDs (for namespace mode). |

## Network Interface Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `private_network_name` | `String` | `""` | `--private-network-name` | Shared private network identifier. |
| `private_network_interface` | `String` | `"eth0"` | `--private-network-interface` | Network interface for private network. |
| `private_network_addr` | `String` | `""` | `--private-network-addr` | Explicit IPv4 address for private network interface (auto-detected when empty). |
| `private_network_port` | `String` | `"8080"` | `--private-network-port` | Port for private network communication. |
| `public_network_interface` | `String` | `"eth0"` | `--public-network-interface` | Network interface for public network. |
| `public_network_addr` | `String` | `""` | `--public-network-addr` | Explicit IPv4 address for public network interface (auto-detected when empty). |
| `public_network_port` | `String` | `"8080"` | `--public-network-port` | Port for public network communication. |
| `tun_interface_name` | `String` | `"utun"` | `--tun-interface-name` | Name of the TUN interface. |
| `mtu` | `i32` | `1400` | `--mtu` | MTU of the TUN interface (max 6400). |
| `enable_local_interface` | `bool` | `true` | `--enable-local-interface` | Enable kernel TUN interface. Set to `false` when using Python API. |

## Processing Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `num_tun_queues` | `usize` | `1` | `--num-tun-queues` | Number of TUN queues. |
| `num_packet_processors` | `usize` | `0` | `--num-packet-processors` | Number of packet processors (0 = use CPU count). |
| `channel_capacity` | `usize` | `1000` | `--channel-capacity` | Capacity for channels between actors. |
| `queue_capacity` | `usize` | `1000` | `--queue-capacity` | Capacity of scheduler queues. |
| `channel_backpressure` | `bool` | `false` | `--channel-backpressure` | Apply backpressure instead of dropping when channels are full. |

## Protocol Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `protocol` | `Protocol` | `tcp` | `--protocol` | Transport protocol: `tcp`, `udp`, or `quic`. |
| `quic_congestion_control` | `CongestionControl` | `bbr` | `--quic-congestion-control` | QUIC congestion control: `bbr` or `cubic`. |

## Scheduling Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `scheduler_type` | `SchedulingDiscipline` | `fifo` | `--scheduler-type` | Scheduler discipline: `fifo` or `wrr`. |
| `scheduler_drop_strategy` | `DropStrategy` | `taildrop` | `--scheduler-drop-strategy` | Drop strategy: `taildrop` or `red` (Random Early Detection). |

## Processing Mode

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `feature` | `Feature` | `sequential` | `--feature` | Processing mode: `sequential` (in-order) or `concurrent` (parallel, may reorder). |
| `operating_mode` | `OperatingMode` | `normal` | (from controller) | Operating mode: `normal` or `max`. |

## Runtime-populated Dataplane State

These fields are maintained at runtime and should not be treated as user tuning knobs.

| Field | Runtime source |
|-------|----------------|
| `local_address` | Computed from node identity and controller-provided base network settings. |
| `virtual_base_addr` | Received from controller startup configuration (`base_addr`). |
| `user_space_address` | Computed from node identity and user-space base network. |
| `external_base_addr` | Received from controller startup configuration (`external_base_addr`). |
| `local_netmask` | Received from controller startup configuration (`net_mask`). |
| `operating_mode` | Updated from controller node specifications. |
| `flow` | Updated from controller flow messages (`AddFlows`). |
| `max_server_port` | Received from controller startup configuration. |

## Implementation-Level Behavior

- A lane is one ingress queue owned by one processor worker in sequential mode. Sequential mode only spreads work across lanes at ingress; packets assigned to one lane remain ordered relative to that lane.
- `Sequential` mode (`Feature::Sequential`) creates one `mpsc` ingress channel per packet processor (`num_packet_processors` total). `ProcessorHandle::Sequential::new` builds `packet_senders` as a vector of per-worker channels and spawns one processor task per receiver.
- The per-packet ingress lane is selected in `SequentialProcHandle::select_processor_ingress_lane`:
  - regular packets: `packet.flow_id.hash(num_lanes)`.
  - lossless FEC packets (detected via `packet.lossless_fec_tree_id()`): `JumpHasher::slot((flow_id, tree_id), num_lanes)`.
- `Concurrent` mode (`Feature::Concurrent`) creates one shared bounded `flume` queue and spawns multiple processor workers that all consume from the same queue, so packets can be dequeued and processed by different workers and observed out-of-order unless downstream flow reordering is applied.
- Route lookup and all mutable processor state still use per-actor broadcast updates; there is no per-lane cache sharing.
- `channel_backpressure` determines queueing policy before routing and dispatch:
  - `true`: await producer queue space (`send`/`send_async`) so ingress blocks until capacity is available.
  - `false`: `try_send`; if full, packet is dropped after warning.
- In both modes, packet forwarding to connector vs local processors still follows `operating_mode`:
  - local destination or `OperatingMode::Normal` -> processor path.
  - remote destination and `OperatingMode::Max` -> connector path.
- All processor updates (routes, node changes, reporters, lossless handle, and related control state) are broadcast so each processor worker receives the same control updates.

## Processor Route Resolution

- The hot path calls into the routing table with tree context: `get_next_hops_by_flow_and_tree(flow_id, fec_tree_id, reporter)` from `RoutingTable`.
- For legacy non-FEC packets, `fec_tree_id` is `None`, so behavior matches the old `get_next_hops_by_flow` path.
- For multicast trees, FEC packets route to the `(src_node_id, tree_id)` space when present; if no route exists, the processor logs a warning and drops the packet.

## TCP Reordering Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `enforce_tcp_order` | `bool` | `true` | `--enforce-tcp-order` | Reorder TCP packets by sequence number before delivery. |
| `delay_tolerance` | `u64` | `500` | `--delay-tolerance` | Max microseconds to hold a flow waiting for missing TCP segment. |
| `backlog_tolerance` | `u64` | `0` | `--backlog-tolerance` | Max queued TCP packets before forcing delivery (0 = disabled). |

## User-Space Ports

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `user_space_client_port` | `u16` | `45535` | `--user-space-client-port` | User-space client port. |
| `user_space_server_port` | `u16` | `8888` | `--user-space-server-port` | User-space server port. |

## Metrics and Reconnection

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `metrics_collection_interval` | `u64` | `5` | `--metrics-collection-interval` | Metrics collection interval in seconds. |
| `restart_on_disconnect` | `bool` | `false` | `--restart-on-disconnect` | Restart node when connection to controller is lost. |

## Auto-Configuration (Namespace Mode)

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `auto_enable_ip_forward` | `bool` | `false` | `--auto-enable-ip-forward` | Automatically enable IPv4 forwarding. |
| `auto_add_forward_rules` | `bool` | `false` | `--auto-add-forward-rules` | Add FORWARD rules between namespace bridge and outbound interface. |
| `auto_add_nat` | `bool` | `false` | `--auto-add-nat` | Add MASQUERADE rule for namespace subnet. |

## Namespace Mode Configuration

These settings control multi-node deployment on a single machine using Linux namespaces.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bridge_name` | `String` | `"isobr0"` | Linux bridge name for namespace isolation. |
| `bridge_ip` | `String` | `"172.16.8.1"` | IPv4 address for the bridge. |
| `subnet` | `u8` | `16` | Subnet mask length (CIDR). |
| `interval_between_spawn` | `u64` | `50` | Milliseconds between node creation. |
| `carrier_max_wait_ms` | `u64` | `20000` | Max wait time for veth interface carrier. |
| `carrier_poll_interval_ms` | `u64` | `100` | Poll interval for carrier checks. |
| `child_start_delay_ms` | `u64` | `150` | Delay after spawning child before handshake. |
| `handshake_timeout_ms` | `u64` | `25000` | Timeout for network setup handshake. |

## Lossless Session Configuration

See [Lossless Session Configuration](/docs/design/lossless_config) for details.

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
# rate = 50_000_000
# bucket_size = 200_000
```

`fec_collaborative_multitree_enabled` is a rollout gate only: when enabled, multi-tree FEC uses collaborative dispatch-time assignment exclusively.
`fec_symbol_size_policy` accepts `chunk_size` or `fixed`; `fec_tree_ids_source` accepts `config` or `installed_routes`.
Python lossless helpers (`send_data`, `receive_data`, `receive_data_async`) do not accept `fec_*` kwargs; configure FEC behavior through `[lossless_runtime_config]`.
Legacy kwargs (`fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`, `fec_tree_ids`) fail at Python bind time with `TypeError` (unexpected keyword argument).
