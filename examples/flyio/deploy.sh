#!/bin/bash
set -e

# Nextmini Fly.io Deployment Script
# This script automates the deployment of Nextmini to Fly.io

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "==================================="
echo "Nextmini Fly.io Deployment"
echo "==================================="
echo ""

# Check if flyctl is installed
if ! command -v flyctl &> /dev/null; then
    echo "❌ Error: flyctl is not installed"
    echo "Install: https://fly.io/docs/hands-on/install-flyctl/"
    exit 1
fi

# Check if logged in
if ! flyctl auth whoami &> /dev/null; then
    echo "❌ Error: Not logged in to Fly.io"
    echo "Run: flyctl auth login"
    exit 1
fi

echo "✅ flyctl installed and authenticated"
echo ""

# Configuration
read -p "Database app name [nextmini-db]: " DB_APP_NAME
DB_APP_NAME=${DB_APP_NAME:-nextmini-db}

read -p "Controller app name [nextmini-controller]: " CONTROLLER_APP_NAME
CONTROLLER_APP_NAME=${CONTROLLER_APP_NAME:-nextmini-controller}

read -p "Region [iad]: " REGION
REGION=${REGION:-iad}

read -p "Number of dataplane nodes [2]: " NUM_NODES
NUM_NODES=${NUM_NODES:-2}

echo ""
echo "Configuration:"
echo "  Database:   $DB_APP_NAME"
echo "  Controller: $CONTROLLER_APP_NAME"
echo "  Region:     $REGION"
echo "  Nodes:      $NUM_NODES"
echo ""
read -p "Continue? (y/n): " -n 1 -r
echo
[[ ! $REPLY =~ ^[Yy]$ ]] && exit 0

# ============================================================
# Step 1: PostgreSQL Database
# ============================================================
echo ""
echo "Step 1: PostgreSQL Database"
echo "----------------------------"

if flyctl apps list | grep -q "^$DB_APP_NAME"; then
    echo "⚠️  Database $DB_APP_NAME already exists"
    read -p "Delete and recreate? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        flyctl apps destroy "$DB_APP_NAME" -y
    else
        echo "Using existing database"
        DB_SKIP=1
    fi
fi

if [ -z "$DB_SKIP" ]; then
    echo "Creating PostgreSQL database..."
    flyctl postgres create \
        --name "$DB_APP_NAME" \
        --region "$REGION" \
        --initial-cluster-size 1 \
        --vm-size shared-cpu-2x \
        --volume-size 1
    
    echo "⏳ Waiting for database to be ready..."
    sleep 10
    
    echo "Creating user 'pgusr' and database 'nextmini'..."
    echo -e "CREATE USER pgusr WITH PASSWORD 'pgpwrd' SUPERUSER;\nCREATE DATABASE nextmini OWNER pgusr;\n\\q" | \
        flyctl postgres connect -a "$DB_APP_NAME"
    
    echo "Disabling SSL..."
    echo -e "ALTER SYSTEM SET ssl = off;\nSELECT pg_reload_conf();\n\\q" | \
        flyctl postgres connect -a "$DB_APP_NAME"
    
    echo "Restarting database..."
    DB_MACHINE_ID=$(flyctl machine list -a "$DB_APP_NAME" -q | head -1 | tr -d '[:space:]')
    flyctl machine restart "$DB_MACHINE_ID" -a "$DB_APP_NAME"
    
    echo "✅ Database ready"
fi

# ============================================================
# Step 2: Controller
# ============================================================
echo ""
echo "Step 2: Controller"
echo "------------------"

if flyctl apps list | grep -q "^$CONTROLLER_APP_NAME"; then
    echo "⚠️  Controller $CONTROLLER_APP_NAME already exists"
    read -p "Delete and recreate? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        flyctl apps destroy "$CONTROLLER_APP_NAME" -y
        flyctl apps create "$CONTROLLER_APP_NAME"
    fi
else
    flyctl apps create "$CONTROLLER_APP_NAME"
fi

# Update controller config with database hostname
cd "$SCRIPT_DIR"
sed -i.bak "s/host = \".*\.internal\"/host = \"$DB_APP_NAME.internal\"/" controller-config.toml

echo "Deploying controller..."
cd "$REPO_ROOT"
flyctl deploy \
    --config "$SCRIPT_DIR/fly.controller.toml" \
    --dockerfile "$SCRIPT_DIR/Dockerfile.controller" \
    --build-arg CARGO_PROFILE=release \
    --ha=false

echo "✅ Controller deployed"

# ============================================================
# Step 3: Dataplane Nodes
# ============================================================
echo ""
echo "Step 3: Dataplane Nodes"
echo "-----------------------"

for ((i=1; i<=NUM_NODES; i++)); do
    NODE_APP_NAME="nextmini-node-$i"
    echo ""
    echo "Deploying node $i ($NODE_APP_NAME)..."
    
    if flyctl apps list | grep -q "^$NODE_APP_NAME"; then
        echo "App exists, updating..."
    else
        flyctl apps create "$NODE_APP_NAME"
    fi
    
    # Create temporary config with unique node_id
    TMP_NODE_CONFIG="/tmp/node-config-$i.toml"
    cp "$SCRIPT_DIR/node-config.toml" "$TMP_NODE_CONFIG"
    sed -i "s/node_id = [0-9]*/node_id = $i/" "$TMP_NODE_CONFIG"
    
    # Create temporary fly.toml
    TMP_FLY_CONFIG="/tmp/fly.node-$i.toml"
    cp "$SCRIPT_DIR/fly.dataplane.toml" "$TMP_FLY_CONFIG"
    sed -i "s/app = \"nextmini-node-1\"/app = \"$NODE_APP_NAME\"/" "$TMP_FLY_CONFIG"
    sed -i "s|local_path = \"examples/flyio/node-config.toml\"|local_path = \"$TMP_NODE_CONFIG\"|" "$TMP_FLY_CONFIG"
    # Remove [build] section to use command-line --dockerfile
    sed -i '/^\[build\]/,/^$/d' "$TMP_FLY_CONFIG"
    
    # Deploy from repo root
    cd "$REPO_ROOT"
    flyctl deploy \
        --config "$TMP_FLY_CONFIG" \
        --dockerfile "$SCRIPT_DIR/Dockerfile.dataplane" \
        --build-arg CARGO_PROFILE=release \
        --ha=false
    
    rm -f "$TMP_NODE_CONFIG" "$TMP_FLY_CONFIG"
    echo "✅ Node $i deployed"
done

# ============================================================
# Summary
# ============================================================
echo ""
echo "==================================="
echo "Deployment Complete!"
echo "==================================="
echo ""
echo "Database:   $DB_APP_NAME.internal"
echo "Controller: $CONTROLLER_APP_NAME.internal:3000"
echo "Nodes:      $NUM_NODES"
for ((i=1; i<=NUM_NODES; i++)); do
    echo "  - nextmini-node-$i"
done
echo ""
echo "Check logs:"
echo "  flyctl logs -a $CONTROLLER_APP_NAME"
echo "  flyctl logs -a nextmini-node-1"
echo ""
echo "Verify connection:"
echo "  flyctl ssh console -a $CONTROLLER_APP_NAME"
echo ""
