# Nextmini Fly.io Deployment Guide

Deploy Nextmini controller and dataplane nodes to Fly.io.

## Prerequisites

1. **Install flyctl**
   ```bash
   # macOS/Linux
   curl -L https://fly.io/install.sh | sh
   
   # Or via package manager
   brew install flyctl  # macOS
   ```

2. **Login to Fly.io**
   ```bash
   flyctl auth login
   ```

3. **Verify authentication**
   ```bash
   flyctl auth whoami
   ```

## Quick Start (Automated)

```bash
cd examples/flyio
chmod +x deploy.sh
./deploy.sh
```

To ssh into the nodes:

```bash
flyctl ssh console -a nextmini-node-1
```

Follow the prompts to deploy database, controller, and dataplane nodes.

## Manual Deployment

### 1. Deploy PostgreSQL Database

```bash
# Create database (512MB RAM required)
flyctl postgres create \
  --name nextmini-db \
  --region iad \
  --initial-cluster-size 1 \
  --vm-size shared-cpu-2x \
  --volume-size 1

# Wait for database to be ready
sleep 10

# Create user and database (matching local config)
echo -e "CREATE USER pgusr WITH PASSWORD 'pgpwrd' SUPERUSER;\nCREATE DATABASE nextmini OWNER pgusr;\n\\q" | \
  flyctl postgres connect -a nextmini-db

# Disable SSL (required for compatibility)
echo -e "ALTER SYSTEM SET ssl = off;\nSELECT pg_reload_conf();\n\\q" | \
  flyctl postgres connect -a nextmini-db

# Restart database to apply SSL setting
DB_MACHINE_ID=$(flyctl machine list -a nextmini-db -q | head -1 | tr -d '[:space:]')
flyctl machine restart $DB_MACHINE_ID -a nextmini-db
```

### 2. Deploy Controller

```bash
# Create app
flyctl apps create nextmini-controller

# Update database host in config
cd examples/flyio
sed -i 's/host = ".*\.internal"/host = "nextmini-db.internal"/' controller-config.toml

# Deploy from repository root
cd ../..
flyctl deploy \
  --config examples/flyio/fly.controller.toml \
  --dockerfile examples/flyio/Dockerfile.controller \
  --build-arg CARGO_PROFILE=release \
  --ha=false

# Verify deployment
flyctl status -a nextmini-controller
flyctl logs -a nextmini-controller
```

### 3. Deploy Dataplane Nodes

For each node (adjust `node_id` accordingly):

```bash
# Node 1
flyctl apps create nextmini-node-1

# Create config with node_id=1
cp examples/flyio/node-config.toml /tmp/node-config-1.toml
sed -i 's/node_id = [0-9]*/node_id = 1/' /tmp/node-config-1.toml

# Create fly.toml
cp examples/flyio/fly.dataplane.toml /tmp/fly.node-1.toml
sed -i 's/app = "nextmini-node-1"/app = "nextmini-node-1"/' /tmp/fly.node-1.toml
sed -i 's|local_path = "examples/flyio/node-config.toml"|local_path = "/tmp/node-config-1.toml"|' /tmp/fly.node-1.toml

# Deploy
flyctl deploy \
  --config /tmp/fly.node-1.toml \
  --dockerfile examples/flyio/Dockerfile.dataplane \
  --build-arg CARGO_PROFILE=release \
  --ha=false

# Cleanup
rm /tmp/node-config-1.toml /tmp/fly.node-1.toml

# Verify connection
flyctl logs -a nextmini-node-1 | grep -i "websocket\|connected"
```

Repeat for node 2 with `node_id = 2`.

## Configuration Files

### `controller-config.toml`
- Database connection: `host = "nextmini-db.internal"`
- Credentials: `pgusr` / `pgpwrd` / `nextmini` (matches local setup)

### `node-config.toml`
- Controller address: `ws://nextmini-controller.internal:3000`
- **Important**: Each node must have unique `node_id`

### `fly.controller.toml`
- Internal service on port 3000
- No external ports (internal communication only)
- File mount: `controller-config.toml` → `/app/config.toml`

### `fly.dataplane.toml`
- Internal service on port 8080
- File mount: `node-config.toml` → `/app/config.toml`

## Key Technical Details

### IPv6 Requirement
Fly.io's internal network uses IPv6. The controller **must** listen on `[::]:{port}` (not `0.0.0.0`) to accept connections from dataplane nodes.

**Fixed in**: `controller/src/main.rs:41`
```rust
let listener = TcpListener::bind(format!("[::]:{}", config.port))
```

### PostgreSQL Configuration
- **VM Size**: `shared-cpu-2x` (512MB RAM minimum, 256MB insufficient)
- **SSL**: Disabled via `ALTER SYSTEM SET ssl = off` (Rust sqlx compatibility)
- **User**: `pgusr` / `pgpwrd` (matches local `start-database.sh`)

### High Availability
Use `--ha=false` to deploy single instances. Multiple instances require additional configuration for node coordination.

## Verification

### Check Controller
```bash
# Status
flyctl status -a nextmini-controller

# Logs
flyctl logs -a nextmini-controller | grep -i "listening\|node\|connected"

# Expected output:
# INFO controller: The controller is now listening on port 3000 (IPv4 and IPv6).
# INFO controller: Node 1 successfully inserted into node_ws.
```

### Check Nodes
```bash
# Status
flyctl status -a nextmini-node-1

# Logs
flyctl logs -a nextmini-node-1 | grep -i "websocket\|controller"

# Expected output:
# INFO nextmini::node::controller::interface: WebSocket handshake has been successfully completed.
# INFO nextmini::node::conductor: Starting Nextmini node 1 on 172.19.4.3:8080...
```

### Check Database
```bash
flyctl postgres connect -a nextmini-db

# Inside psql:
\c nextmini
\dt
SELECT * FROM nodes;
```

## Troubleshooting

### Connection Refused (os error 111)
**Symptom**: Node logs show `Failed to connect to the controller: IO error: Connection refused`

**Causes**:
1. Controller listening on IPv4 only (use `[::]`)
2. Controller not yet started (wait 5-10s after deploy)
3. Wrong controller address in node config

So change:

```rust
    let listener = TcpListener::bind(format!("[::]:{}", config.port))
        .await
        .expect("Failed to bind to port.");
    info!("The controller is now listening on port {} (IPv4 and IPv6).", config.port);
```

**Fix**:
```bash
# Restart node to reconnect
flyctl machine restart $(flyctl machine list -a nextmini-node-1 -q | head -1) -a nextmini-node-1
```

### Database Connection Failed
**Symptom**: Controller logs show `Failed to connect to database: PoolTimedOut`

**Causes**:
1. SSL enabled (disable with `ALTER SYSTEM SET ssl = off`)
2. Database not ready (wait after creation)
3. Wrong credentials in `controller-config.toml`

**Fix**:
```bash
# Check database health
flyctl status -a nextmini-db

# Verify SSL is off
echo "SHOW ssl; \q" | flyctl postgres connect -a nextmini-db
```

## Management Commands

### View Logs
```bash
# Follow logs
flyctl logs -a nextmini-controller
flyctl logs -a nextmini-node-1

# Filter logs
flyctl logs -a nextmini-controller | grep ERROR
```

### SSH Access
```bash
# Access controller
flyctl ssh console -a nextmini-controller

# Access node
flyctl ssh console -a nextmini-node-1
```

### Restart
```bash
# Restart app (all machines)
flyctl apps restart nextmini-controller

# Restart specific machine
MACHINE_ID=$(flyctl machine list -a nextmini-controller -q | head -1)
flyctl machine restart $MACHINE_ID -a nextmini-controller
```

### Destroy
```bash
# Destroy single app
flyctl apps destroy nextmini-node-1 -y

# Destroy all (use with caution)
flyctl apps destroy nextmini-node-1 -y
flyctl apps destroy nextmini-node-2 -y
flyctl apps destroy nextmini-controller -y
flyctl apps destroy nextmini-db -y
```
