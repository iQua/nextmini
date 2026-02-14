# Configuration Reference

This document provides a comprehensive reference for all configuration options in Nextmini.

---

## Controller Configuration

The controller configuration file (typically `controller-config.toml`) defines the network topology, routing, flows, and database settings.

### Server Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | `u16` | `3000` | Port for the controller WebSocket server. |
| `max_server_port` | `u16` | `8081` | Port for connection-on-demand TCP server. |

### Network Address Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_addr` | `Ipv4Addr` | `10.0.0.0` | Base IPv4 address for the TUN network. |
| `net_mask` | `Ipv4Addr` | `255.255.0.0` | Network mask (accommodates up to 65,535 nodes). |
| `user_space_base_addr` | `Ipv4Addr` | `192.168.0.0` | Base address for user-space smoltcp network. |
| `external_base_addr` | `Ipv4Addr` | `172.16.8.3` | Base address for external network traffic. |

### Multicast Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `multicast_pool_base` | `Ipv4Addr` | `239.255.0.0` | Base address for multicast group IP allocation. |
| `multicast_pool_mask` | `Ipv4Addr` | `255.255.0.0` | Netmask for multicast pool (/16 = 65,535 groups). |

### Protocol Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protocol` | `Protocol` | `tcp` | Transport protocol: `tcp`, `udp`, or `quic`. |
| `flow_transport` | `FlowTransport` | `tcp` | Transport for controller-managed flows: `tcp` or `lossless_unicast`. |
| `scheduler_type` | `SchedulingDiscipline` | `fifo` | Scheduler discipline: `fifo` or `wrr` (weighted round-robin). |

### Topology Configuration

```toml
[topology]
type = "full_mesh"  # full_mesh | ring | fat_tree | torus
```

#### Full Mesh

```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }
```

#### Ring

```toml
[topology]
type = "ring"
ring_config = { n_nodes = 4 }
```

#### Fat Tree

```toml
[topology]
type = "fat_tree"
fat_tree_config = { k = 4 }  # k must be even
```

The fat tree topology creates `k^3/4` server nodes and `5k^2/4` switch nodes.

#### Torus

```toml
[topology]
type = "torus"
torus_config = { dim = 2, n = 4 }  # 2D torus with 4 nodes per dimension = 16 nodes
```

Supports 1D, 2D, and 3D torus topologies. Total nodes = `n^dim`.

#### Custom Edges

```toml
[topology]
edges = [[1, 2], [2, 3], [3, 4], [4, 1]]
n_nodes = 4  # Required when using custom edges
```

### Routing Configuration

```toml
[routing]
protocol = "shortest_path"
```

Currently only `shortest_path` is supported.

### Custom Routes

Routes can be defined as simple paths or DAGs:

```toml
# Simple path: 1 → 2 → 3 → 4
[[routes]]
route = [1, 2, 3, 4]

# DAG with multiple paths
[[routes]]
route = [[1, 2], [2, 4], [1, 3], [3, 4]]
```

### Link Rate Configuration

```toml
[[link_rates]]
src_node_id = 1
dst_node_id = 2
rate = 100_000_000       # bytes per second
bucket_size = 312_000_000  # token bucket size in bytes
```

### Flow Configuration

```toml
[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000, flow_weight = 1, transport = "tcp" }
```

#### FlowSpec Fields

| Field | Type | Description |
|-------|------|-------------|
| `flow_len` | `FlowLen` | `{ Bytes = n }` or `{ Duration = seconds }` |
| `flow_rate` | `Option<usize>` | Rate in bytes per second (required for Duration flows with lossless_unicast). |
| `flow_weight` | `Option<usize>` | Weight for weighted round-robin scheduling. |
| `transport` | `FlowTransport` | `tcp` or `lossless_unicast`. |

### Node Specifications

Per-node configuration overrides:

```toml
[[nodes]]
node_id = 1
operating_mode = "max"  # normal (default) | max
```

### Database Configuration

```toml
[db]
user = "pgusr"
password = "pgpwrd"
host = "172.16.8.2"
database = "nextmini"
port = "5432"
```

---

## Dataplane Configuration

The dataplane configuration file (typically `config.toml` or `node.toml`) defines node-specific settings.

### Controller Connection

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `controller_addr` | `String` | `""` | `--controller-addr` | WebSocket address of the controller (e.g., `ws://192.168.1.1:3000`). A plain `host:port` value is normalized to `ws://host:port`. |

### Node Identity

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `node_id` | `usize` | `0` | `--node-id` | Unique node identifier (auto-assigned if 0). |
| `n_nodes` | `usize` | `1` | `--n-nodes` | Total number of dataplane nodes. |
| `node_id_offset` | `usize` | `0` | `--node-id-offset` | Offset added to computed node IDs (for namespace mode). |

### Network Interface Configuration

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

### Processing Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `num_tun_queues` | `usize` | `1` | `--num-tun-queues` | Number of TUN queues. |
| `num_packet_processors` | `usize` | `0` | `--num-packet-processors` | Number of packet processors (0 = use CPU count). |
| `channel_capacity` | `usize` | `1000` | `--channel-capacity` | Capacity for channels between actors. |
| `queue_capacity` | `usize` | `1000` | `--queue-capacity` | Capacity of scheduler queues. |
| `channel_backpressure` | `bool` | `false` | `--channel-backpressure` | Apply backpressure instead of dropping when channels are full. |

### Protocol Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `protocol` | `Protocol` | `tcp` | `--protocol` | Transport protocol: `tcp`, `udp`, or `quic`. |
| `quic_congestion_control` | `CongestionControl` | `bbr` | `--quic-congestion-control` | QUIC congestion control: `bbr` or `cubic`. |

### Scheduling Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `scheduler_type` | `SchedulingDiscipline` | `fifo` | `--scheduler-type` | Scheduler discipline: `fifo` or `wrr`. |
| `scheduler_drop_strategy` | `DropStrategy` | `taildrop` | `--scheduler-drop-strategy` | Drop strategy: `taildrop` or `red` (Random Early Detection). |

### Processing Mode

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `feature` | `Feature` | `sequential` | `--feature` | Processing mode: `sequential` (in-order) or `concurrent` (parallel, may reorder). |
| `operating_mode` | `OperatingMode` | `normal` | (from controller) | Operating mode: `normal` or `max`. |

#### Implementation-Level Behavior

- A **lane** is one ingress queue owned by one processor worker in sequential mode. Sequential mode only spreads work across lanes at ingress; packets assigned to one lane remain ordered relative to that lane.

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
  - local destination or `OperatingMode::Normal` → processor path
  - remote destination and `OperatingMode::Max` → connector path
- All processor updates (routes, node changes, reporters, lossless handle, etc.) are still broadcast via `broadcast_sender` so each processor worker receives the same control state.

#### Processor Route Resolution

- The hot path calls into the routing table with tree context:
  - current code uses `get_next_hops_by_flow_and_tree(flow_id, fec_tree_id, reporter)` from `RoutingTable`.
  - for legacy non-FEC packets, `fec_tree_id` is `None`, so it behaves as the old `get_next_hops_by_flow` path.
- For multicast trees, FEC packets route to the `(src_node_id, tree_id)` space when present; if no route exists, the processor logs a warning and drops the packet (`Dropping packet because multicast tree route is unknown`).

### TCP Reordering Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `enforce_tcp_order` | `bool` | `true` | `--enforce-tcp-order` | Reorder TCP packets by sequence number before delivery. |
| `delay_tolerance` | `u64` | `500` | `--delay-tolerance` | Max microseconds to hold a flow waiting for missing TCP segment. |
| `backlog_tolerance` | `u64` | `0` | `--backlog-tolerance` | Max queued TCP packets before forcing delivery (0 = disabled). |

### User-Space Ports

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `user_space_client_port` | `u16` | `45535` | `--user-space-client-port` | User-space client port. |
| `user_space_server_port` | `u16` | `8888` | `--user-space-server-port` | User-space server port. |

### Metrics and Reconnection

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `metrics_collection_interval` | `u64` | `5` | `--metrics-collection-interval` | Metrics collection interval in seconds. |
| `restart_on_disconnect` | `bool` | `false` | `--restart-on-disconnect` | Restart node when connection to controller is lost. |

### Auto-Configuration (for namespace mode)

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `auto_enable_ip_forward` | `bool` | `false` | `--auto-enable-ip-forward` | Automatically enable IPv4 forwarding. |
| `auto_add_forward_rules` | `bool` | `false` | `--auto-add-forward-rules` | Add FORWARD rules between namespace bridge and outbound interface. |
| `auto_add_nat` | `bool` | `false` | `--auto-add-nat` | Add MASQUERADE rule for namespace subnet. |

### Namespace Mode Configuration

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

### Lossless Session Configuration

See [Lossless Session Configuration](lossless_config.md) for details.

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

`fec_collaborative_multitree_enabled` is a rollout gate only: when enabled, multi-tree FEC uses collaborative dispatch-time assignment exclusively (no hash/legacy strategy mode).
`fec_symbol_size_policy` accepts `chunk_size` or `fixed`; `fec_tree_ids_source` accepts `config` or `installed_routes`.
Python lossless helpers (`send_data`, `receive_data`, `receive_data_async`) do not accept `fec_*` kwargs; configure FEC behavior through `[lossless_runtime_config]`.
Legacy kwargs (`fec_enabled`, `fec_symbols_per_block`, `fec_symbol_size`, `fec_tree_ids`) now fail at Python bind time with `TypeError` (unexpected keyword argument).

---

## Enums Reference

### Protocol

Transport protocol for inter-node communication.

| Value | Description |
|-------|-------------|
| `tcp` | TCP transport (default). |
| `udp` | UDP transport. |
| `quic` | QUIC transport with configurable congestion control. |

### FlowTransport

Transport for controller-managed flows.

| Value | Description |
|-------|-------------|
| `tcp` | TCP-based flow transport (default). |
| `lossless_unicast` | Lossless unicast with chunking and pacing. |

### SchedulingDiscipline

Scheduler discipline for packet queues.

| Value | Description |
|-------|-------------|
| `fifo` | First-in, first-out (default). |
| `wrr` | Weighted round-robin. |

### DropStrategy

Drop strategy when queues are full.

| Value | Description |
|-------|-------------|
| `taildrop` | Drop newest packets when queue is full (default). |
| `red` | Random Early Detection - probabilistic dropping before queue fills. |

### Feature

Packet processing mode.

| Value | Description |
|-------|-------------|
| `sequential` | Single processor per flow, guarantees packet order (default). |
| `concurrent` | Multiple processors, higher throughput but may reorder packets. |

### OperatingMode

Node operating mode.

| Value | Description |
|-------|-------------|
| `normal` | Standard operation (default). |
| `max` | Maximum throughput mode with connection-on-demand. |

### CongestionControl

QUIC congestion control algorithm.

| Value | Description |
|-------|-------------|
| `bbr` | BBR congestion control (default). |
| `cubic` | CUBIC congestion control. |

### RouteForwardingMode

How routes forward traffic.

| Value | Description |
|-------|-------------|
| `unicast` | Point-to-point forwarding (default). |
| `multicast` | Fan-out to multiple next hops. |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NEXTMINI_CONFIG` | Optional script convention for Python examples/tools: path passed to `nextmini_py.Dataplane(...)`. Not consumed directly by Rust binaries. |
| `NEXTMINI_DST_NODE` | Optional script convention for Python examples/tools: destination node ID used by user-defined telemetry hooks. |
| `RUST_LOG` | Logging level (`info`, `debug`, `trace`). |
| `PYO3_PYTHON` | Path to Python 3.13 interpreter for `nextmini_py` Rust tests. |
| `DATABASE_URL` | PostgreSQL connection string used by helper scripts such as `utils/start-database.sh` (controller runtime uses `[db]` config fields). |

---

## Example Configurations

### Minimal Controller Configuration

```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }

[db]
host = "postgres"
```

### Minimal Dataplane Configuration

```toml
controller_addr = "ws://192.168.1.1:3000"
private_network_interface = "eth0"
```

### Full Controller Configuration

```toml
port = 3000
protocol = "tcp"
scheduler_type = "fifo"
flow_transport = "tcp"

[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }

[routing]
protocol = "shortest_path"

[[link_rates]]
src_node_id = 1
dst_node_id = 2
rate = 100_000_000
bucket_size = 312_000_000

[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000 }

[[nodes]]
node_id = 1
operating_mode = "max"

[db]
user = "pgusr"
password = "pgpwrd"
host = "postgres"
database = "nextmini"
port = "5432"
```

### Full Dataplane Configuration

```toml
controller_addr = "ws://192.168.1.1:3000"
private_network_interface = "eth0"
private_network_name = "cluster-net"

node_id = 1
mtu = 1400

protocol = "tcp"
scheduler_type = "fifo"
scheduler_drop_strategy = "taildrop"
feature = "sequential"

num_tun_queues = 1
num_packet_processors = 0
channel_capacity = 1000
queue_capacity = 1000

enforce_tcp_order = true
delay_tolerance = 500
backlog_tolerance = 0

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
```
