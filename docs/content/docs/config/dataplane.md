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
| `channel_backpressure` | `bool` | `false` | `--channel-backpressure` | Apply backpressure instead of dropping when channels are full. This is required for collaborative multi-tree FEC sessions with more than one configured tree id; sender preflight rejects that mode otherwise. |

## Protocol Configuration

Dataplane transport settings are documented in [Transport Configuration](/docs/config/transport), including CLI usage and examples for `protocol` and `quic_congestion_control`.

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

See [Lossless Session Configuration](/docs/config/lossless) for the full configuration details, sample `[lossless_runtime_config]` block, and runtime Python interop notes.
