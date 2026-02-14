---
title: "Example Configurations"
description: "Collects practical TOML snippets for common Nextmini controller and dataplane setups."
---

## Minimal Controller Configuration

```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }

[db]
host = "postgres"
```

## Minimal Dataplane Configuration

```toml
controller_addr = "ws://192.168.1.1:3000"
private_network_interface = "eth0"
```

## Full Controller Configuration

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

## Full Dataplane Configuration

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
