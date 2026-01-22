# Configuration Reference

This document provides a comprehensive reference for all configuration options in Nextmini.

---

## Controller Configuration

The controller configuration file (typically `controller-config.toml`) defines the network topology, routing, flows, and database settings.

### Server Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | `u16` | `3000` | Port for the controller WebSocket server. |
| `max_server_port` | `u16` | `8081` | Port for the TCP MAX server (dataplane-to-dataplane MAX connections and optional SOCKS5 proxy ingress). See [Proxy flows](proxy-flows.md). |

### Network Address Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_addr` | `Ipv4Addr` | `10.0.0.0` | Base IPv4 address for the TUN network. |
| `net_mask` | `Ipv4Addr` | `255.255.0.0` | Network mask (accommodates up to 65,535 nodes). |
| `user_space_base_addr` | `Ipv4Addr` | `192.168.0.0` | Base address for the user-space network (SmolTCP/Python/lossless). See [User-space flows](user-space-flows.md). |
| `external_base_addr` | `Ipv4Addr` | `172.16.8.3` | Base address for external endpoints (used by SOCKS5/MAX proxy flows). See [Proxy flows](proxy-flows.md). |

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
| `controller_addr` | `String` | `""` | `--controller-addr` | WebSocket address of the controller (e.g., `ws://192.168.1.1:3000`). |
| `controller_connect_timeout_ms` | `u64` | `5000` | *(config only)* | Timeout (ms) for establishing the initial WebSocket connection to the controller. |

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
| `private_network_port` | `String` | `"8080"` | `--private-network-port` | Port for private network communication. |
| `private_network_addr` | `String` | `""` | `--private-network-addr` | Optional override for the private interface address to advertise to the controller. |
| `public_network_interface` | `String` | `"eth0"` | `--public-network-interface` | Network interface for public network. |
| `public_network_port` | `String` | `"8080"` | `--public-network-port` | Port for public network communication. |
| `public_network_addr` | `String` | `""` | `--public-network-addr` | Optional override for the public interface address to advertise to the controller. |
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
| `channel_backpressure` | `bool` | `false` | `--channel-backpressure` | If `true`, internal bounded channels block instead of dropping when full. |

#### Backpressure and overload behavior

The dataplane pipeline is built from multiple asynchronous tasks (network readers, processors, schedulers, connector, user‑space engines). These tasks communicate via **bounded queues**:

- `channel_capacity` bounds internal channels (ingress → processor, processor → connector/scheduler, etc.).
- `queue_capacity` bounds each scheduler’s packet queue (per next hop) in units of **packets**.

When the system is overloaded (producers generate packets faster than downstream tasks can forward them), Nextmini has two strategies:

1. **Drop on overload** (default): `channel_backpressure = false`
   - Internal channels use non-blocking sends (`try_send`). When full, packets are dropped and you will see logs like “channel full; dropping packet”.
   - Scheduler queues use `scheduler_drop_strategy`:
     - `taildrop`: drop newest packets when full
     - `red`: probabilistically drop before full to avoid standing queues

2. **Backpressure on overload**: `channel_backpressure = true`
   - Internal channels use blocking/asynchronous sends (`send` / `blocking_send`) so pressure propagates back to the producer.
   - Scheduler queues also apply backpressure at `queue_capacity` (the scheduler blocks instead of dropping).
   - In this mode, `scheduler_drop_strategy` is effectively bypassed because the queue blocks before the drop policy is consulted.

**Trade-offs**

- Dropping (`channel_backpressure = false`) keeps latency and task scheduling stable under bursty load, but it can reduce throughput (TCP retransmits) and can surprise Python/user‑space consumers if you expected “no internal drops”.
- Backpressure (`channel_backpressure = true`) avoids most internal drops, but it can increase tail latency and can stall producers if the consumer is slow (for example, if a Python receiver is not draining).

**Practical guidance**

- If you see frequent “channel full; dropping packet” logs and want correctness‑style behavior, enable backpressure and increase capacities.
- If you want best-effort throughput under load (and you can tolerate loss/retransmits), keep backpressure off and tune `channel_capacity`, `queue_capacity`, and `scheduler_drop_strategy`.

### Protocol Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `protocol` | `Protocol` | `tcp` | `--protocol` | Transport protocol: `tcp`, `udp`, or `quic` (controller-managed deployments overwrite this on startup). |
| `quic_congestion_control` | `CongestionControl` | `bbr` | `--quic-congestion-control` | QUIC congestion control: `bbr` or `cubic`. |

### Scheduling Configuration

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `scheduler_type` | `SchedulingDiscipline` | `fifo` | `--scheduler-type` | Scheduler discipline: `fifo` or `wrr` (controller-managed deployments overwrite this on startup). |
| `scheduler_drop_strategy` | `DropStrategy` | `taildrop` | `--scheduler-drop-strategy` | Drop strategy: `taildrop` or `red` (Random Early Detection). |

### Processing Mode

| Field | Type | Default | CLI Flag | Description |
|-------|------|---------|----------|-------------|
| `feature` | `Feature` | `sequential` | `--feature` | Processing mode: `sequential` (in-order) or `concurrent` (parallel, may reorder). |
| `operating_mode` | `OperatingMode` | `normal` | (from controller) | Operating mode: `normal` or `max`. |

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

# Optional pacing
# [lossless_runtime_config.data_bucket]
# rate = 50_000_000
# bucket_size = 200_000
```

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
| `max` | Maximum throughput mode with connection-on-demand. See [Proxy flows](proxy-flows.md). |

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
| `NEXTMINI_CONFIG` | Path to dataplane configuration file. |
| `NEXTMINI_DST_NODE` | Destination node ID for telemetry. |
| `RUST_LOG` | Logging level (`info`, `debug`, `trace`). |
| `PYO3_PYTHON` | Path to Python 3.13 interpreter (for running tests). |
| `CONTROLLER_RESET_DB` | If set to `0`/`false`/`no`, disables the controller’s default DB reset on startup (runs migrations only). |
| `DATABASE_URL` | Used by `utils/start-database.sh` (from `.env`) to start a local Postgres container. Not read by the controller. |

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
```
