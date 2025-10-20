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
chmod +x *.py
uv run deploy_controller.py
```

### Step 3: Deploy Nodes

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

**Node Machine 2:**
```bash
cd ~/nextmini/examples/routes-multi-host
chmod +x *.py
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 2 --interface ens3
```

**Note:** Replace `ens3` with your actual interface name if different.

### Step 4: Verify

```bash
# Controller log
tail -f ~/nextmini/examples/routes-multi-host/controller-deploy/controller.log

# Node logs (on each node machine)
tail -f ~/nextmini/examples/routes-multi-host/node1-deploy/node1.log
tail -f ~/nextmini/examples/routes-multi-host/node2-deploy/node2.log
```

### Step 5: Cleanup

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

### Issue: Node can't connect to controller

**Check firewall on controller machine:**
```bash
sudo ufw allow 3000/tcp
sudo ufw status
```

**Test connectivity from node:**
```bash
telnet 206.12.89.244 3000
```

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

### Issue: Port already in use

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
