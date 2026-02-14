---
title: "Controller Configuration"
description: "Reference for controller settings, topology, routing, flows, and database fields."
---

The controller binary reads `./config.toml` at startup (`get_config("config.toml")`).
Examples often keep a source file named `controller-config.toml` and mount or copy it to `./config.toml` at runtime.
This file defines network topology, routing, flows, and database settings.

## Server Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | `u16` | `3000` | Port for the controller WebSocket server. |
| `max_server_port` | `u16` | `8081` | Port for connection-on-demand TCP server. |

## Network Address Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_addr` | `Ipv4Addr` | `10.0.0.0` | Base IPv4 address for the TUN network. |
| `net_mask` | `Ipv4Addr` | `255.255.0.0` | Network mask (accommodates up to 65,535 nodes). |
| `user_space_base_addr` | `Ipv4Addr` | `192.168.0.0` | Base address for user-space smoltcp network. |
| `external_base_addr` | `Ipv4Addr` | `172.16.8.3` | Base address for external network traffic. |

## Multicast Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `multicast_pool_base` | `Ipv4Addr` | `239.255.0.0` | Base address for multicast group IP allocation. |
| `multicast_pool_mask` | `Ipv4Addr` | `255.255.0.0` | Netmask for multicast pool (/16 = 65,535 groups). |

## Protocol Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protocol` | `Protocol` | `tcp` | Transport protocol: `tcp`, `udp`, or `quic`. |
| `flow_transport` | `FlowTransport` | `tcp` | Transport for controller-managed flows: `tcp` or `lossless_unicast`. |
| `scheduler_type` | `SchedulingDiscipline` | `fifo` | Scheduler discipline: `fifo` or `wrr` (weighted round-robin). |

## Topology Configuration

```toml
[topology]
type = "full_mesh"  # full_mesh | ring | fat_tree | torus
```

`type` is the canonical key. The legacy alias `topology` is still accepted for backward compatibility.

### Full Mesh

```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }
```

### Ring

```toml
[topology]
type = "ring"
ring_config = { n_nodes = 4 }
```

### Fat Tree

```toml
[topology]
type = "fat_tree"
fat_tree_config = { k = 4 }  # k must be even
```

The fat tree topology creates `k^3/4` server nodes and `5k^2/4` switch nodes.

### Torus

```toml
[topology]
type = "torus"
torus_config = { dim = 2, n = 4 }  # 2D torus with 4 nodes per dimension = 16 nodes
```

Supports 1D, 2D, and 3D torus topologies. Total nodes = `n^dim`.

### Custom Edges

```toml
[topology]
edges = [[1, 2], [2, 3], [3, 4], [4, 1]]
n_nodes = 4  # Required when using custom edges
```

## Routing Configuration

```toml
[routing]
protocol = "shortest_path"
```

Currently only `shortest_path` is supported. The `[routing]` section is optional; when omitted, no topology-derived routes are generated and only explicit `[[routes]]` entries are used.

## Custom Routes

Routes can be defined as simple paths or DAGs:

```toml
# Simple path: 1 -> 2 -> 3 -> 4
[[routes]]
route = [1, 2, 3, 4]

# DAG with multiple paths
[[routes]]
route = [[1, 2], [2, 4], [1, 3], [3, 4]]
```

## Link Rate Configuration

```toml
[[link_rates]]
src_node_id = 1
dst_node_id = 2
rate = 100_000_000         # bytes per second
bucket_size = 312_000_000  # token bucket size in bytes
```

## Flow Configuration

```toml
[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000, flow_weight = 1, transport = "tcp" }
```

### FlowSpec Fields

| Field | Type | Description |
|-------|------|-------------|
| `flow_len` | `FlowLen` | `{ Bytes = n }` or `{ Duration = seconds }` |
| `flow_rate` | `Option<usize>` | Rate in bytes per second (required for Duration flows with lossless_unicast). |
| `flow_weight` | `Option<usize>` | Weight for weighted round-robin scheduling. |
| `transport` | `FlowTransport` | `tcp` or `lossless_unicast`. |

## Node Specifications

Per-node configuration overrides:

```toml
[[nodes]]
node_id = 1
operating_mode = "max"  # normal (default) | max
```

## Database Configuration

```toml
[db]
user = "pgusr"
password = "pgpwrd"
host = "172.16.8.2"
database = "nextmini"
port = "5432"
```
