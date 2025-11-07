#!/usr/bin/env bash
set -euo pipefail

# Deploy multicast test to remote nodes

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

# Configuration
CONTROLLER_ADDR=${CONTROLLER_ADDR:-"ws://206.12.89.244:3000"}
REMOTE_DIR=${REMOTE_DIR:-"/root/multicast-test"}
GROUP_ID=${GROUP_ID:-520}

if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "ERROR: hosts.txt not found at $HOSTS_FILE"
    exit 1
fi

echo "=== Multicast Test Deployment ==="
echo "Repository root: $REPO_ROOT"
echo "Remote directory: $REMOTE_DIR"
echo "Controller: $CONTROLLER_ADDR"
echo "Group ID: $GROUP_ID"
echo ""

# Parse hosts.txt (format: NODE_ID | HOST | LOCAL_IP | ROLE | PORT)
declare -A HOSTS IPS ROLES PORTS
while IFS='|' read -r NODE_ID HOST_SPEC LOCAL_IP ROLE PORT _; do
    # Skip empty lines and comments
    [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
    
    HOSTS[$NODE_ID]="$HOST_SPEC"
    IPS[$NODE_ID]="$LOCAL_IP"
    ROLES[$NODE_ID]="${ROLE,,}"
    PORTS[$NODE_ID]="${PORT:-8080}"
done < "$HOSTS_FILE"

echo "Found ${#HOSTS[@]} nodes"
echo ""

# Build nextmini_py wheel
echo "=== Building nextmini_py wheel ==="
cd "$REPO_ROOT"

if ! command -v maturin &> /dev/null; then
    echo "ERROR: maturin not found. Install: pip install maturin"
    exit 1
fi

maturin build --release -m python-api/Cargo.toml

WHEEL=$(find "$REPO_ROOT/target/wheels" -name "nextmini_py-*.whl" -type f | sort | tail -n1)
if [[ -z "$WHEEL" ]]; then
    echo "ERROR: No wheel found in target/wheels/"
    exit 1
fi

echo "✅ Wheel built: $WHEEL"
WHEEL_NAME=$(basename "$WHEEL")

# Create deployment bundle
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

cp "$REPO_ROOT/examples/multicast-test/sender.py" "$WORK_DIR/"
cp "$REPO_ROOT/examples/multicast-test/receiver.py" "$WORK_DIR/"
cp "$WHEEL" "$WORK_DIR/$WHEEL_NAME"

# Create bundle
BUNDLE="$WORK_DIR/multicast-test-bundle.tgz"
tar -czf "$BUNDLE" -C "$WORK_DIR" sender.py receiver.py "$WHEEL_NAME"

echo ""
echo "=== Deploying to nodes ==="

for NODE_ID in $(echo "${!HOSTS[@]}" | tr ' ' '\n' | sort -n); do
    HOST_SPEC="${HOSTS[$NODE_ID]}"
    LOCAL_IP="${IPS[$NODE_ID]}"
    LOCAL_PORT="${PORTS[$NODE_ID]}"
    ROLE="${ROLES[$NODE_ID]}"
    
    echo ""
    echo "--- Deploying to node $NODE_ID ($HOST_SPEC, IP: $LOCAL_IP, role: $ROLE) ---"
    
    # Upload bundle
    scp -q "$BUNDLE" "$HOST_SPEC:~/multicast-test-bundle.tgz"
    
    # Extract and setup directory
    ssh -q "$HOST_SPEC" "mkdir -p $REMOTE_DIR && tar xzf ~/multicast-test-bundle.tgz -C $REMOTE_DIR && rm -f ~/multicast-test-bundle.tgz"
    
    # Generate node-config.toml dynamically on remote node
    ssh -q "$HOST_SPEC" "cat > $REMOTE_DIR/node-config.toml" <<CONFIG
controller_addr = "$CONTROLLER_ADDR"
node_id = $NODE_ID
num_tun_queues = 1
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"
private_network_addr = "$LOCAL_IP"
private_network_port = "$LOCAL_PORT"
public_network_addr = "$LOCAL_IP"
public_network_port = "$LOCAL_PORT"
enable_local_interface = false
CONFIG
    
    # Setup Python environment
    echo "Setting up Python environment on node $NODE_ID..."
    ssh "$HOST_SPEC" "bash -s" <<REMOTE_SETUP
        set -euo pipefail
        cd $REMOTE_DIR
        
        # Create venv
        python3 -m venv venv
        source venv/bin/activate
        
        # Install dependencies
        pip install -q --upgrade pip
        pip install -q $WHEEL_NAME
        
        echo "✅ Environment ready"
REMOTE_SETUP
    
    echo "✅ Node $NODE_ID deployed"
done

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Directory structure on each node:"
echo "  $REMOTE_DIR/"
echo "    ├── config.py"
echo "    ├── sender.py"
echo "    ├── receiver.py"
echo "    ├── node-config.toml (auto-generated)"
echo "    ├── venv/"
echo "    └── $WHEEL_NAME"
echo ""
echo "See QUICKSTART.md for next steps"

