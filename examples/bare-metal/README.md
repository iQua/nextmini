# Multi-Host Deployment Guide

Deploy Nextmini Controller and Dataplane Nodes across multiple physical machines.

## Quick Start

### Scenario
- Controller: `206.12.89.244` (Physical Machine 1)
- Node 1: Physical Machine 2
- Node 2: Physical Machine 3

All nodes use port `8080` (no conflict).

---

### Step 1: Build (Once)

```bash
cd nextmini
cargo run -p cert-gen
cargo build --release -p controller
cargo build --release -p nextmini
```

### Step 2: Deploy Controller (206.12.89.244)

```bash
cd ~/nextmini/examples/routes-multi-host
chmod +x *.py *.sh
uv run deploy_controller.py
```

### Step 3: Deploy Nodes (with SSH setup)

**Important:** Check your network interface first:
```bash
ip addr show | grep "state UP"
# Example output: ens3, eth0, etc.
```

**Node Machine 1:**
```bash
cd ~/nextmini/examples/routes-multi-host
chmod +x *.py
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --interface ens3
```

The script will show SSH setup command at the end. **Copy and run it** to enable ring all-reduce testing.

**Node Machine 2:**
```bash
cd ~/nextmini/examples/routes-multi-host
chmod +x *.py
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 2 --interface ens3
```

Again, **copy and run the SSH setup command** shown at the end.

**Note:** Replace `ens3` with your actual interface name if different.

### Step 4: Verify

```bash
# Controller log
tail -f ~/nextmini/examples/routes-multi-host/controller-deploy/controller.log

# Node logs (on each node machine)
tail -f ~/nextmini/examples/routes-multi-host/node1-deploy/node1.log
tail -f ~/nextmini/examples/routes-multi-host/node2-deploy/node2.log
```

Look for:
```
INFO controller: Node 1 successfully inserted into node_ws. Total nodes now: 1.
INFO controller: Node 2 successfully inserted into node_ws. Total nodes now: 2.
INFO controller::new_node: All 2 nodes are now connected. Sending node addresses, link rates and flows to all nodes.
INFO controller::new_node: All dataplane nodes have connected. It takes XX.XX seconds since the first node arrived.
```

### Step 5: Test with Ring All-Reduce (Optional)

```bash
# Build ringallreduce (once)
cd ~/nextmini
cargo build --release -p ringallreduce-routes

# Run test
cd ~/nextmini/examples/routes-multi-host
./run_ring.sh
```

Or test with iperf3:

**On Node 2 machine:**
```bash
iperf3 -s
```

**On Node 1 machine:**
```bash
# Test to Node 2's TUN IP
iperf3 -c 10.0.0.1

# Example output:
# [ ID] Interval           Transfer     Bitrate
# [  5]   0.00-10.00  sec  XXX MBytes  XXX Mbits/sec
```

The traffic will go through Nextmini's TUN interface and appear in controller logs as flows.

### Step 6: Cleanup

```bash
cd ~/nextmini/examples/routes-multi-host
uv run cleanup.py
```

---

## Prerequisites

### All Machines
- Rust toolchain: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Build tools: `build-essential`, `pkg-config`, `libssl-dev`
- Python 3.13 and uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Network connectivity between all machines

### Controller Machine (206.12.89.244)
- Docker for PostgreSQL
- Port 3000 open for incoming connections

### Dataplane Machines
- Root/sudo access (for TUN device)
- Port 8080 open for node-to-node communication

---

## Optional Parameters

### Change Network Interface

Default is `ens3`. If your interface is different:

```bash
# Find your interface
ip addr show | grep "state UP"

# Deploy with different interface (e.g., eth0)
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --interface eth0
```

### Custom Controller Port

```bash
# Deploy controller on custom port
uv run deploy_controller.py --port 3001

# Deploy nodes with custom controller port
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --controller-port 3001
```

### Custom Node Port

```bash
# Only if you need a different port
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --port 9000
```

### View Help

```bash
uv run deploy_controller.py --help
uv run deploy_node.py --help
uv run cleanup.py --help
```

---

## Troubleshooting

### Issue: Database connection failed

**On controller machine:**
```bash
docker ps | grep nextmini-database
docker logs nextmini-database
```

### Issue: TUN device permission denied

**Solution:** Scripts use sudo automatically. Ensure sudo access on node machines.

### Issue: Build permission denied (target directory)

**Symptoms:**
```
error: failed to write `/home/ubuntu/nextmini/target/release/.fingerprint/...`
Caused by: Permission denied (os error 13)
```

**Cause:** Previous builds with sudo created root-owned files in target directory.

**Solution:**
```bash
cd /home/ubuntu/nextmini
sudo chown -R ubuntu:ubuntu target/
cargo run -p cert-gen
cargo build --release -p controller
cargo build --release -p nextmini
```

### Issue: Ring all-reduce "Cannot assign requested address"

**Symptoms:**
```
Error: rank 0 failed to bind 10.0.0.1:9000
Caused by: Cannot assign requested address (os error 99)
```

**Cause:** `ring.txt` IP order doesn't match actual node TUN IPs.

**Solution:**

Check actual TUN IPs on each node:
```bash
# On Node 1
ip addr show | grep -A 2 utun
# Example: inet 10.0.0.2/16

# On Node 2  
ip addr show | grep -A 2 utun
# Example: inet 10.0.0.1/16
```

Update `ring.txt` to match:
```bash
# If Node 1 has 10.0.0.2 and Node 2 has 10.0.0.1:
10.0.0.2:9000  # rank 0 (Node 1)
10.0.0.1:9000  # rank 1 (Node 2)
```

Order in `ring.txt` must match `ssh_hosts.txt` order.

### Issue: Ring all-reduce "Address already in use"

**Symptoms:**
```
Error: rank 0 failed to bind 10.0.0.2:9000
Caused by: Address already in use (os error 98)
```

**Solution:**
```bash
# Cleanup and retry
cd ~/nextmini/examples/routes-multi-host
uv run cleanup_ring.py
./run_ring.sh
```

Or manually on each node:
```bash
pkill -9 ringallreduce
```

### Issue: Port already in use (Dataplane)

**Find and kill process:**
```bash
sudo lsof -ti:8080 | xargs sudo kill -9
```

---

## Configuration Files

### Controller Config (`controller-config.toml`)

```toml
protocol = "tcp"

[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 2 }

[[routes]]
route = [[1, 2]]

[[routes]]
route = [[2, 1]]

[db]
user = "pgusr"
password = "pgpwrd"
host = "127.0.0.1"
port = "5432"
database = "nextmini"
```

### Node Config (Auto-generated)

```toml
private_network_interface = "eth0"
private_network_name = "net1"
num_tun_queues = 1
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"

controller_addr = "ws://206.12.89.244:3000"
node_id = 1
public_network_port = "8080"
```

---

## Deployment Architecture

```
Physical Machine 1 (206.12.89.244)
├── PostgreSQL (Docker, port 5432)
└── Controller (port 3000)
        │
        ├─ Node 1 (Machine 2, port 8080)
        └─ Node 2 (Machine 3, port 8080)
```

**Key Point:** Each node runs on a separate machine, so all nodes can use the same port 8080.

---

## Advanced Topics

### Customizing Routes

Edit `controller-config.toml` before deploying:

```toml
# Example: Add more routes
[[routes]]
route = [[1, 2]]

[[routes]]
route = [[2, 1]]
```

Then redeploy controller:
```bash
uv run cleanup.py
uv run deploy_controller.py
```

### Changing Number of Nodes

Edit `controller-config.toml`:
```toml
[topology]
full_mesh_config = { n_nodes = 3 }  # Change to 3 nodes
```

Deploy additional nodes with higher node IDs.

---

## Notes

- Each node on a **separate machine** can use the **same port** (8080)
- Controller IP must be the **actual network IP**, not localhost
- TLS certificates are self-signed (generated by cert-gen)
- PostgreSQL runs in Docker on controller machine
- Logs are in `*-deploy/*.log` files
- Database container name: `nextmini-database`
