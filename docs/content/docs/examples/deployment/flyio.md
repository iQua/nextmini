---
title: "Fly.io Deployment"
description: "Runbook for deploying Nextmini controller and dataplane nodes on Fly.io."
---

This runbook condenses the original deployment notes and scripts into a reproducible sequence.

## Prerequisites

- `flyctl` installed and authenticated.
- A checked-out copy of this repository.
- A target Fly region (examples below use `iad`).

```bash
flyctl auth login
flyctl auth whoami
```

## Operational Constraints

- Deploy in this order: database, then controller, then dataplane nodes.
- Fly private networking is IPv6-first; controller should bind on `[::]:3000`.
- PostgreSQL must run on at least `shared-cpu-2x` (512MB). `shared-cpu-1x` is typically insufficient.
- Disable PostgreSQL SSL for this configuration (`ALTER SYSTEM SET ssl = off`).
- Each dataplane app needs a unique `node_id` in its mounted `node-config.toml`.
- `examples/flyio/node-config.toml` assumes `private_network_interface = "eth0"`.

## Deployment Workflow

### Option A: Automated script

```bash
cd examples/flyio
chmod +x deploy.sh
./deploy.sh
```

The script performs the full sequence and creates per-node temporary configs.

### Option B: Manual sequence

Set names and sizing:

```bash
export DB_APP_NAME=nextmini-db
export CONTROLLER_APP_NAME=nextmini-controller
export REGION=iad
export NUM_NODES=2
```

1. Deploy PostgreSQL:

```bash
flyctl postgres create \
  --name "$DB_APP_NAME" \
  --region "$REGION" \
  --initial-cluster-size 1 \
  --vm-size shared-cpu-2x \
  --volume-size 1

# Create user/db expected by controller-config.toml
echo -e "CREATE USER pgusr WITH PASSWORD 'pgpwrd' SUPERUSER;\nCREATE DATABASE nextmini OWNER pgusr;\n\\q" | \
  flyctl postgres connect -a "$DB_APP_NAME"

# Required for current sqlx setup in this deployment
echo -e "ALTER SYSTEM SET ssl = off;\nSELECT pg_reload_conf();\n\\q" | \
  flyctl postgres connect -a "$DB_APP_NAME"

DB_MACHINE_ID=$(flyctl machine list -a "$DB_APP_NAME" -q | head -1 | tr -d '[:space:]')
flyctl machine restart "$DB_MACHINE_ID" -a "$DB_APP_NAME"
```

2. Deploy controller:

```bash
flyctl apps create "$CONTROLLER_APP_NAME" || true
sed -i.bak "s/host = \".*\\.internal\"/host = \"$DB_APP_NAME.internal\"/" examples/flyio/controller-config.toml

flyctl deploy \
  --config examples/flyio/fly.controller.toml \
  --dockerfile examples/flyio/Dockerfile.controller \
  --build-arg CARGO_PROFILE=release \
  --ha=false
```

3. Deploy dataplane nodes:

```bash
for i in $(seq 1 "$NUM_NODES"); do
  NODE_APP_NAME="nextmini-node-$i"
  TMP_NODE_CONFIG="/tmp/node-config-$i.toml"
  TMP_FLY_CONFIG="/tmp/fly.node-$i.toml"

  flyctl apps create "$NODE_APP_NAME" || true

  cp examples/flyio/node-config.toml "$TMP_NODE_CONFIG"
  sed -i.bak "s/node_id = [0-9]*/node_id = $i/" "$TMP_NODE_CONFIG"

  cp examples/flyio/fly.dataplane.toml "$TMP_FLY_CONFIG"
  sed -i.bak "s/app = \"nextmini-node-1\"/app = \"$NODE_APP_NAME\"/" "$TMP_FLY_CONFIG"
  sed -i.bak "s|local_path = \"examples/flyio/node-config.toml\"|local_path = \"$TMP_NODE_CONFIG\"|" "$TMP_FLY_CONFIG"
  sed -i.bak '/^\[build\]/,/^$/d' "$TMP_FLY_CONFIG"

  flyctl deploy \
    --config "$TMP_FLY_CONFIG" \
    --dockerfile examples/flyio/Dockerfile.dataplane \
    --build-arg CARGO_PROFILE=release \
    --ha=false

  rm -f "$TMP_NODE_CONFIG" "$TMP_NODE_CONFIG.bak" "$TMP_FLY_CONFIG" "$TMP_FLY_CONFIG.bak"
done

rm -f examples/flyio/controller-config.toml.bak
```

## Verification

```bash
flyctl status -a "$CONTROLLER_APP_NAME"
flyctl logs -a "$CONTROLLER_APP_NAME" | grep -Ei "listening|node|connected"

flyctl status -a nextmini-node-1
flyctl logs -a nextmini-node-1 | grep -Ei "websocket|controller|connected"
```

Optional database check:

```bash
flyctl postgres connect -a "$DB_APP_NAME"
# then in psql:
# \c nextmini
# SELECT * FROM nodes;
```

## Cleanup Workflow

### Option A: Cleanup script

```bash
cd examples/flyio
chmod +x cleanup.sh
./cleanup.sh
```

`cleanup.sh` deletes all apps matching `nextmini-*` after confirmation.

### Option B: Manual teardown (reverse order)

```bash
for i in $(seq 1 "$NUM_NODES"); do
  flyctl apps destroy "nextmini-node-$i" -y
done

flyctl apps destroy "$CONTROLLER_APP_NAME" -y
flyctl apps destroy "$DB_APP_NAME" -y
```

Optional post-checks:

```bash
flyctl apps list | grep nextmini- || true
flyctl volumes list
```
