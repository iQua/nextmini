#!/bin/bash
set -e

# Nextmini Fly.io Deployment Script
# This script automates the deployment of Nextmini to Fly.io

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Nextmini Fly.io Deployment"
echo "================================"
echo ""

# Check if flyctl is installed
if ! command -v flyctl &> /dev/null; then
    echo "❌ Error: flyctl is not installed"
    echo "Install it from: https://fly.io/docs/hands-on/install-flyctl/"
    exit 1
fi

# Check if logged in
if ! flyctl auth whoami &> /dev/null; then
    echo "❌ Error: Not logged in to Fly.io"
    echo "Run: flyctl auth login"
    exit 1
fi

echo "flyctl is installed and you are logged in"
echo ""

# Step 1: Create PostgreSQL database
echo "Step 1: Create PostgreSQL Database"
echo "--------------------------------------"
read -p "Create new PostgreSQL database? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "Enter database app name [nextmini-db]: " DB_APP_NAME
    DB_APP_NAME=${DB_APP_NAME:-nextmini-db}
    
    read -p "Enter region [sjc]: " DB_REGION
    DB_REGION=${DB_REGION:-sjc}
    
    echo "Creating PostgreSQL database..."
    flyctl postgres create \
        --name "$DB_APP_NAME" \
        --region "$DB_REGION" \
        --initial-cluster-size 1 \
        --vm-size shared-cpu-1x \
        --volume-size 1
    
    echo ""
    echo "Database created: $DB_APP_NAME"
    echo ""
    echo "Setting up custom user and database..."
    sleep 10  # Wait for database to be ready
    
    # Create custom user and database to match local setup
    flyctl postgres connect -a "$DB_APP_NAME" <<EOF
CREATE USER pgusr WITH PASSWORD 'pgpwrd';
CREATE DATABASE nextmini OWNER pgusr;
GRANT ALL PRIVILEGES ON DATABASE nextmini TO pgusr;
\q
EOF
    
    echo "User 'pgusr' and database 'nextmini' created"
else
    read -p "Enter existing database app name: " DB_APP_NAME
fi
echo ""

# Step 2: Deploy Controller
echo "Step 2: Deploy Controller"
echo "-----------------------------"
read -p "Enter controller app name [nextmini-controller]: " CONTROLLER_APP_NAME
CONTROLLER_APP_NAME=${CONTROLLER_APP_NAME:-nextmini-controller}

read -p "Enter region [sjc]: " CONTROLLER_REGION
CONTROLLER_REGION=${CONTROLLER_REGION:-sjc}

read -p "Deploy controller? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$REPO_ROOT"
    
    # Create or update the app
    if flyctl apps list | grep -q "$CONTROLLER_APP_NAME"; then
        echo "App $CONTROLLER_APP_NAME already exists"
        read -p "Delete and recreate? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Deleting old app..."
            flyctl apps destroy "$CONTROLLER_APP_NAME" -y
            echo "Creating new app: $CONTROLLER_APP_NAME"
            flyctl apps create "$CONTROLLER_APP_NAME" --org personal
        else
            echo "Using existing app (this may fail if app has no machines)"
        fi
    else
        echo "Creating new app: $CONTROLLER_APP_NAME"
        flyctl apps create "$CONTROLLER_APP_NAME" --org personal
    fi
    
    # Attach PostgreSQL database
    echo "Attaching database to controller..."
    flyctl postgres attach "$DB_APP_NAME" -a "$CONTROLLER_APP_NAME" || true
    
    # Update controller-config.toml with correct database host
    echo "Updating database host in config..."
    cd "$SCRIPT_DIR"
    sed -i.bak "s/host = \".*\.internal\"/host = \"$DB_APP_NAME.internal\"/" controller-config.toml
    
    # Set secrets
    echo "Setting environment variables..."
    
    # Deploy (from repo root with flyio directory for config files)
    echo "Deploying controller..."
    cd "$REPO_ROOT"
    flyctl deploy \
        --config "$SCRIPT_DIR/fly.controller.toml" \
        --dockerfile "$SCRIPT_DIR/Dockerfile.controller" \
        --build-arg BUILDKIT_CONTEXT_KEEP_GIT_DIR=1 \
        --app "$CONTROLLER_APP_NAME" \
        --ha=false
    
    echo "Controller deployed: $CONTROLLER_APP_NAME"
    echo "Access at: https://$CONTROLLER_APP_NAME.fly.dev"
else
    echo "Skipping controller deployment"
fi
echo ""

# Step 3: Deploy Dataplane Nodes
echo "Step 3: Deploy Dataplane Nodes"
echo "-----------------------------------"
read -p "How many dataplane nodes to deploy? [2]: " NUM_NODES
NUM_NODES=${NUM_NODES:-2}

read -p "Deploy dataplane nodes? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    for ((i=1; i<=NUM_NODES; i++)); do
        NODE_APP_NAME="nextmini-node-$i"
        
        echo ""
        echo "Deploying node $i..."
        echo "--------------------"
        
        cd "$REPO_ROOT"
        
        # Create or update the app
        if flyctl apps list | grep -q "$NODE_APP_NAME"; then
            echo "App $NODE_APP_NAME already exists, updating..."
        else
            echo "Creating new app: $NODE_APP_NAME"
            flyctl apps create "$NODE_APP_NAME" --org personal
        fi
        
        # Set secrets for this node
        echo "Setting node configuration..."
        flyctl secrets set \
            NODE_ID="$i" \
            CONTROLLER_ADDR="ws://$CONTROLLER_APP_NAME.internal:3000" \
            -a "$NODE_APP_NAME"
        
        # Deploy
        echo "Deploying node $i..."
        
        # Create a temporary fly.toml for this node
        cp "$SCRIPT_DIR/fly.dataplane.toml" /tmp/fly.node-$i.toml
        sed -i "s/nextmini-node-1/$NODE_APP_NAME/g" /tmp/fly.node-$i.toml
        # Remove entire [build] section to rely on command line --dockerfile
        sed -i '/^\[build\]/,/^$/d' /tmp/fly.node-$i.toml
        
        cd "$REPO_ROOT"
        flyctl deploy \
            --config /tmp/fly.node-$i.toml \
            --dockerfile "$SCRIPT_DIR/Dockerfile.dataplane" \
            --build-arg BUILDKIT_CONTEXT_KEEP_GIT_DIR=1 \
            --app "$NODE_APP_NAME" \
            --ha=false
        
        rm /tmp/fly.node-$i.toml
        
        echo "Node $i deployed: $NODE_APP_NAME"
    done
else
    echo "Skipping dataplane node deployment"
fi
echo ""

# Summary
echo "Deployment Complete!"
echo "======================="
echo ""
echo "Controller: $CONTROLLER_APP_NAME"
echo "  URL: https://$CONTROLLER_APP_NAME.fly.dev"
echo "  Internal: ws://$CONTROLLER_APP_NAME.internal:3000"
echo ""
echo "Database: $DB_APP_NAME"
echo ""
echo "Dataplane Nodes: $NUM_NODES"
for ((i=1; i<=NUM_NODES; i++)); do
    echo "  - nextmini-node-$i"
done
echo ""
echo "Next Steps:"
echo "  1. Check controller logs: flyctl logs -a $CONTROLLER_APP_NAME"
echo "  2. Check node logs: flyctl logs -a nextmini-node-1"
echo "  3. Scale nodes: flyctl scale count 2 -a nextmini-node-1"
echo "  4. SSH into controller: flyctl ssh console -a $CONTROLLER_APP_NAME"
echo ""
echo "Documentation: examples/flyio/README.md"

