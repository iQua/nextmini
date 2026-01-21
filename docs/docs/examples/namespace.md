# Namespace scaling (single host)

`examples/namespace` spawns many dataplane nodes as Linux network namespaces on a single host. It is useful for measuring:

- Controller join/startup time at high node counts
- Memory footprint per node
- veth/namespace setup overhead

This is a **namespace-mode scaling harness**, not a TUN overlay demo. The default dataplane config sets `enable_local_interface = false`, so nodes do not create per-namespace TUN interfaces.

## Contents

- Quick start (tmux)
- Manual run (two terminals)
- Observing results
- Cleanup
- Notes

## Running the Single Host Example

### Step 1: Run the Launcher Script

Run the launcher script to apply sysctl tuning and start a tmux session with the controller on the left and the dataplane on the right:

```bash
./examples/namespace/run.sh --n-nodes 800
```

The `--n-nodes` flag updates both the controller config and dataplane automatically.

The script raises ARP neighbor table thresholds, connection backlog limits, and netlink socket buffers to avoid "Exchange full (os error 54)" errors during rapid veth creation and to reduce controller connect timeouts under bursty joins.

Pass `--sysctl-only` to apply tuning without starting `tmux`.

### Manual run (two terminals)

Apply sysctl tuning:

```bash
./examples/namespace/run.sh --sysctl-only
```

Terminal A (controller + DB):

```bash
cd ~/nextmini/examples/namespace
docker compose -f docker-compose.yml up --build
```

Postgres and the controller run inside a dedicated Docker bridge subnet (`170.16.8.0/24`) defined in `examples/namespace/docker-compose.yml`. The controller config uses the Postgres container IP (`170.16.8.2`) accordingly.

Terminal B (dataplane):

```bash
cd ~/nextmini
cargo build -p nextmini --release
sudo -E RUST_LOG=info ./target/release/nextmini --config-path examples/namespace/config.toml --n-nodes 800
```

Make sure `examples/namespace/controller-config.toml` has the expected node count, and set the dataplane node count either via `examples/namespace/config.toml` or `--n-nodes`.

The sysctl parameters:

- `gc_thresh1`: Soft minimum — minimum ARP entries to maintain
- `gc_thresh2`: Soft maximum — triggers aggressive garbage collection
- `gc_thresh3`: Hard maximum — absolute limit on ARP entries

The script also sets `ulimit -u 20000` and `ulimit -n 200000` for large node counts.

The controller container also needs a high `nofile` limit to accept thousands of WebSocket connections. `examples/namespace/docker-compose.yml` sets this for the controller. If you see `controller exited with code 0` while nodes are still connecting, it is usually because `accept()` failed due to a file descriptor limit.

### Step 2: Observing the results

You should see output similar to the following in the controller terminal:

```bash
controller  |  INFO controller::new_node: All 800 nodes are now connected. Sending node addresses, link rates and flows to all nodes.
controller  |  INFO controller::new_node: Skipping AddNodeAddress broadcast (no Max-mode nodes configured).
controller  |  INFO controller::new_node: All dataplane nodes have connected.
controller  |  INFO controller::new_node: All dataplane nodes have finished wiring their topologies. Broadcasting topology-ready signal.
controller  |  INFO controller::new_node: Broadcasting topology-ready signal to 800 dataplane nodes.
```

Note: the controller log "All ... nodes are now connected" refers to nodes connecting to the controller and completing `StartUp`. If your controller config includes a topology (e.g., `type = "ring"`), dataplane nodes will continue wiring node-to-node connections after this point. For the connection-only scaling baseline, remove `type`/`*_config`/`edges` from `[topology]` and set only `n_nodes = N` in `examples/namespace/controller-config.toml`.

Monitor memory usage in a new terminal:

```bash
free -h
```

### Step 3: Cleaning up

To clean up the environment, run:

```bash
./examples/namespace/cleanup.sh
```

This script stops the tmux session (if present), brings down the controller containers, and removes created `veth*`/`isobr*` devices.

## Development and Testing Notes

### Host sizing

For large `n_nodes`, run on a Linux host (or VM) with enough RAM and a high file descriptor limit. If you see failures while creating veth pairs or accepting controller connections, re-run the launcher with `--sysctl-only` and confirm your `ulimit`/sysctl settings were applied.

### Testing the startup time and memory consumption

To test the total amount of time for starting up all the nodes and the memory consumption afterwards:

- Configure the desired `n_nodes` value with ring preset topology

- Remove the `[routing]` section from configuration files (no persistent TCP connections and no routes needed)

The following lines could be used in `examples/namespace/controller-config.toml` for testing purposes:

```toml
protocol = "tcp"

[topology]
type = "ring"
ring_config = { n_nodes = 600 }

[db]
user = "pgusr"
password = "pgpwrd"
host = "170.16.8.2"
database = "nextmini"
port = "5432"
```

### Monitoring the database

To see how many nodes have registered with the controller:

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS nodes_connected FROM nodes;"
```

### The logic of assigning IP addresses

Namespace nodes are assigned sequential node IDs and per-namespace IPs:

- Node IDs: `node_id = idx + node_id_offset + 1` (set by the parent in `dataplane/src/node/namespace/manager.rs`).
- IPs: with the default `bridge_ip = 172.16.8.1`, the first node gets `ns_ip = 172.16.8.2` (network `.0` and gateway `.1` are reserved).
- For large runs, nodes are sharded across multiple `isobr*` bridges to avoid per-bridge port limits; each shard uses a `/22` subnet.

Note: the namespace bridge subnet (`172.16.8.0/…`) is unrelated to the Docker bridge subnet (`170.16.8.0/24`) used by the controller/Postgres containers.

Example (default settings):

- `idx = 0 → ns_ip = 172.16.8.2 → node_id = 1` (host veth: `veth0a`, namespace veth: `veth0b`)
