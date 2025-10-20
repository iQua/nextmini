# Multi-Host Deployment Guide

Deploy Nextmini Controller and Dataplane Nodes across multiple physical machines.

## Deployment Scenario

```
Physical Machine 1 (206.12.89.244)
├── PostgreSQL (Docker)
└── Controller (port 3000)

Physical Machine 2
└── Node 1 (port 8080)

Physical Machine 3  
└── Node 2 (port 8080)

Physical Machine N
└── Node N (port 8080)
```

**Key Point:** Each node runs on a separate machine, so all nodes can use the **same port 8080**.

---

## Prerequisites

### All Machines
- Rust toolchain: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Build tools: `build-essential`, `pkg-config`, `libssl-dev`
- Python 3.8+ and uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Network connectivity between all machines

### Controller Machine (206.12.89.244)
- Docker for PostgreSQL
- Port 3000 open for incoming connections

### Dataplane Machines
- Root/sudo access (for TUN device)
- Port 8080 open for node-to-node communication

---

## Step 1: Build Binaries (Once)

On any machine with Rust:

```bash
cd nextmini

# Generate TLS certificates
cargo run -p cert-gen

# Build binaries
cargo build --release -p controller
cargo build --release -p nextmini
```

Artifacts created:
- `target/release/controller`
- `target/release/nextmini`
- `server_cert.pem`
- `server_key.pem`

---

## Step 2: Deploy Controller

On controller machine (`206.12.89.244`):

```bash
# Copy example folder
cd ~/nextmini
cp -r examples/routes-multi-host ~/deploy

# Make scripts executable
cd ~/deploy
chmod +x *.py *.sh

# Deploy controller
uv run deploy_controller.py
```

**Output shows:** Controller listening on `0.0.0.0:3000`

---

## Step 3: Deploy Dataplane Nodes

On each dataplane machine:

```bash
# Copy example folder
cd ~/nextmini
cp -r examples/routes-multi-host ~/deploy

# Make scripts executable
cd ~/deploy
chmod +x *.py *.sh

# Deploy node (each machine uses same command with different node-id)
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id <N>
```

**Examples:**

**Machine 2 (Node 1):**
```bash
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1
```

**Machine 3 (Node 2):**
```bash
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 2
```

**Machine 4 (Node 3):**
```bash
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 3
```

All nodes use port `8080` by default (no conflict because they're on different machines).

---

## Verification

### Check Controller (on 206.12.89.244)

```bash
tail -f ~/deploy/controller-deploy/controller.log
```

Look for:
```
INFO controller: Node 1 successfully inserted into node_ws. Total nodes now: 1.
INFO controller: Node 2 successfully inserted into node_ws. Total nodes now: 2.
```

### Check Nodes (on each node machine)

```bash
tail -f ~/deploy/node<N>-deploy/node<N>.log
```

Look for:
```
INFO nextmini::node::controller::interface: WebSocket handshake has been successfully completed.
INFO nextmini::node::conductor: Starting Nextmini node <N> on :8080...
```

### Test Connectivity

From any node machine:
```bash
# Test controller reachability
telnet 206.12.89.244 3000

# Check TUN interface
ip addr show | grep utun
```

---

## Cleanup

On each machine:
```bash
cd ~/deploy
uv run cleanup.py
```

Or manually:
```bash
# Stop processes
sudo pkill nextmini
sudo pkill controller

# On controller: stop database
docker stop nextmini-database
docker rm nextmini-database
```

---

## Optional Parameters

### Custom Network Interface

If your interface is not `eth0`:

```bash
# Find interface
ip addr show | grep "state UP"

# Deploy with custom interface
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --interface ens3
```

### Custom Controller Port

```bash
# Deploy controller on custom port
uv run deploy_controller.py --port 3001

# Deploy nodes with custom controller port
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --controller-port 3001
```

### Custom Node Port (Usually Not Needed)

Only if you need a different port for specific reasons:

```bash
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --port 9000
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
full_mesh_config = { n_nodes = 4 }

[[routes]]
route = [[1, 2], [2, 4], [1, 3], [3, 4]]

[[routes]]
route = [[4, 1]]

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

## Scripts Reference

### Python Scripts (Recommended)

- `deploy_controller.py` - Deploy controller with database
- `deploy_node.py` - Deploy single dataplane node
- `cleanup.py` - Stop all processes and cleanup

### Shell Scripts (Alternative)

- `deploy-controller.sh` - Deploy controller
- `deploy-node.sh <CONTROLLER_IP> <NODE_ID>` - Deploy node
- `cleanup.sh` - Cleanup

---

## Help Commands

```bash
uv run deploy_controller.py --help
uv run deploy_node.py --help
uv run cleanup.py --help
```

---

## File Structure

```
routes-multi-host/
├── README.md                      # This file
├── QUICKSTART.md                  # Quick reference
├── USAGE_EXAMPLES.md              # Copy-paste commands
├── pyproject.toml                 # Python config
├── controller-config.toml         # Controller config
├── node-config-template.toml      # Node template
├── deploy_controller.py           # Python: deploy controller
├── deploy_node.py                 # Python: deploy node
├── cleanup.py                     # Python: cleanup
├── deploy-controller.sh           # Shell: deploy controller
├── deploy-node.sh                 # Shell: deploy node
└── cleanup.sh                     # Shell: cleanup
```

---

## Notes

- Each node on a **separate machine** can use the **same port** (8080)
- Controller IP must be the **actual network IP**, not localhost
- TLS certificates are self-signed (generated by cert-gen)
- PostgreSQL runs in Docker on controller machine
- Logs are in `*-deploy/*.log` files
