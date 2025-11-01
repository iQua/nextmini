# Bare Metal Deployment

Deploy Nextmini Controller and Dataplane Nodes as native binaries across multiple physical machines.

This guide demonstrates **native binary deployment** on bare metal or virtual machines, using Python scripts for automated setup. Unlike Docker-based examples, the **Controller and Dataplane nodes run directly on the host OS** (not in containers), with TUN interfaces for networking. PostgreSQL runs in a Docker container for database management.

## Quick Start

### Step 1: Build

```bash
cd nextmini
cargo run -p cert-gen
cargo build --release -p controller
cargo build --release -p nextmini
```

### Step 2: Start Database (One-time)

```bash
./start-database.sh
```

### Step 3: Deploy Controller

```bash
cd examples/bare-metal/controller
./deploy.sh
```

### Step 4: Setup SSH (One-time)

```bash
# Install keychain
sudo apt-get install keychain

# Load SSH key
eval $(keychain --eval ~/nextmini/examples/bare-metal/dataplane/ssh/id_rsa)
```

Or add to ~/.bashrc for permanent setup:
```
echo 'eval $(keychain --eval --quiet ~/nextmini/examples/bare-metal/dataplane/ssh/id_rsa)' >> ~/.bashrc
source ~/.bashrc
```

Enter passphrase once, it will persist across terminal sessions.

### Step 5: Configure

Edit `controller/config.toml`:
```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 2 }
```

Edit `dataplane/hosts.txt`:
```
1|root@157.180.84.40|157.180.84.40
2|ubuntu@206.12.91.229|206.12.91.229
```

Edit `dataplane/node.toml`:
```toml
controller_addr = "ws://206.12.89.244:3000"
```

### Step 6: Deploy Nodes

```bash
cd examples/bare-metal/dataplane
./deploy.sh
```

### Step 7: Manage Services

**Stop nodes:**
```bash
cd examples/bare-metal/dataplane
./stop.sh
```

**Stop controller:**
```bash
cd examples/bare-metal/controller
./stop.sh
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
cd ~/nextmini/examples/bare-metal
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
private_network_interface = "ens3"
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
