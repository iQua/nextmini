#!/usr/bin/env bash
set -euo pipefail

# Bare-metal deployment for ml-tensor-multicast (multicast branch)
# Similar to pyo3 branch's deploy.sh but uses nextmini_py.Dataplane API

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

CONTROLLER_ADDR=${CONTROLLER_ADDR:-"ws://206.12.89.244:3000"}
REMOTE_DIR=${REMOTE_DIR:-"/mnt/ml-tensor-multicast"}  # Use /mnt for large disk space
ITERATIONS=${ITERATIONS:-5}
TENSOR_ROWS=${TENSOR_ROWS:-32}  # Reduced to fit within nextmini's 6400 byte MTU limit
TENSOR_COLS=${TENSOR_COLS:-32}  # 32x32 float32 ≈ 4KB + overhead < 6KB

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found at $HOSTS_FILE" >&2
  exit 1
fi

declare -A HOSTS IPS ROLES PORTS

while IFS='|' read -r NODE_ID HOST_SPEC LOCAL_IP ROLE PORT _; do
  [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
  HOSTS[$NODE_ID]="$HOST_SPEC"
  IPS[$NODE_ID]="$LOCAL_IP"
  ROLES[$NODE_ID]="${ROLE,,}"
  PORTS[$NODE_ID]="${PORT:-8080}"
done < "$HOSTS_FILE"

TRAINER_ID=""
WORKER_ID=""
for id in "${!ROLES[@]}"; do
  [[ "${ROLES[$id]}" == "trainer" ]] && TRAINER_ID="$id"
  [[ "${ROLES[$id]}" == "worker" ]] && WORKER_ID="$id"
done

if [[ -z "$TRAINER_ID" || -z "$WORKER_ID" ]]; then
  echo "hosts.txt must define one trainer and one worker" >&2
  exit 1
fi

# Build nextmini_py wheel on local machine (will be uploaded)
echo "Building nextmini_py extension module..."
cd "$REPO_ROOT"

# Check if maturin is available
if ! command -v maturin &> /dev/null; then
  echo "ERROR: maturin not found. Install: pip install maturin"
  exit 1
fi

maturin build --release -m python-api/Cargo.toml

WHEEL=$(find "$REPO_ROOT/target/wheels" -name "nextmini_py-*.whl" -type f | sort | tail -n1)
if [[ -z "$WHEEL" ]]; then
  echo "ERROR: No wheel found in target/wheels/" >&2
  exit 1
fi

echo "✅ Wheel built: $WHEEL"

# Create deployment bundle
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

cp "$REPO_ROOT/examples/ml-tensor-multicast/trainer.py" "$WORK_DIR/"
cp "$REPO_ROOT/examples/ml-tensor-multicast/worker.py" "$WORK_DIR/"
cp "$REPO_ROOT/examples/ml-tensor-multicast/torch_serializer.py" "$WORK_DIR/"
WHEEL_NAME=$(basename "$WHEEL")
cp "$WHEEL" "$WORK_DIR/$WHEEL_NAME"

BUNDLE="$WORK_DIR/ml-tensor-bundle.tgz"
tar -czf "$BUNDLE" -C "$WORK_DIR" trainer.py worker.py torch_serializer.py "$WHEEL_NAME"

# Deploy to each node
for NODE_ID in "${!HOSTS[@]}"; do
  HOST_SPEC="${HOSTS[$NODE_ID]}"
  LOCAL_IP="${IPS[$NODE_ID]}"
  LOCAL_PORT="${PORTS[$NODE_ID]}"

  echo "Uploading bundle to node $NODE_ID ($HOST_SPEC)"
  scp -q "$BUNDLE" "$HOST_SPEC:~/ml-tensor-bundle.tgz"
  ssh -q "$HOST_SPEC" "sudo mkdir -p $REMOTE_DIR && sudo chown \$USER:\$USER $REMOTE_DIR && tar xzf ~/ml-tensor-bundle.tgz -C $REMOTE_DIR && rm -f ~/ml-tensor-bundle.tgz"
  
  ssh -q "$HOST_SPEC" "cat > $REMOTE_DIR/node-config.toml" <<CONFIG
controller_addr = "$CONTROLLER_ADDR"
num_tun_queues = 1
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"
private_network_addr = "$LOCAL_IP"
private_network_port = "$LOCAL_PORT"
public_network_addr = "$LOCAL_IP"
public_network_port = "$LOCAL_PORT"
node_id = $NODE_ID
enable_local_interface = false
CONFIG

  # Setup uv environment on remote node
  echo "Setting up uv environment on node $NODE_ID..."
  ssh "$HOST_SPEC" "bash -s" <<REMOTE_SETUP
    set -euo pipefail
    
    # Install uv if not present
    if ! command -v uv &> /dev/null && [[ ! -f \$HOME/.local/bin/uv ]]; then
      echo "Installing uv..."
      curl -LsSf https://astral.sh/uv/install.sh | sh
    fi
    
    # Ensure uv is in PATH
    export PATH="\$HOME/.local/bin:\$PATH"
    
    # Use /mnt for uv cache to avoid filling root partition
    export UV_CACHE_DIR="/mnt/.uv-cache"
    sudo mkdir -p "\$UV_CACHE_DIR"
    sudo chown \$USER:\$USER "\$UV_CACHE_DIR"
    
    # Create/update venv in ml-tensor-multicast directory
    cd $REMOTE_DIR
    uv venv --python python3 .venv
    
    # Install dependencies
    echo "Installing torch, numpy, toml with uv..."
    uv pip install torch numpy toml
    
    # Install nextmini_py wheel (using wildcard to match the actual filename)
    echo "Installing nextmini_py wheel..."
    uv pip install nextmini_py-*.whl --force-reinstall
REMOTE_SETUP
done

TRAINER_HOST="${HOSTS[$TRAINER_ID]}"
WORKER_HOST="${HOSTS[$WORKER_ID]}"

# Start worker first
ssh -q "$WORKER_HOST" "cd $REMOTE_DIR && bash -c 'nohup .venv/bin/python -u worker.py \
  --config node-config.toml \
  --trainer-node-id $TRAINER_ID \
  --src-port 5000 \
  --dst-port 4000 \
  --max-iterations $ITERATIONS \
  > worker.log 2>&1 & echo \$! > worker.pid' && exit 0"

sleep 2

# Start trainer
ssh -q "$TRAINER_HOST" "cd $REMOTE_DIR && bash -c 'nohup .venv/bin/python -u trainer.py \
  --config node-config.toml \
  --worker-node-id $WORKER_ID \
  --src-port 4000 \
  --dst-port 5000 \
  --tensor-size $TENSOR_ROWS $TENSOR_COLS \
  --iterations $ITERATIONS \
  > trainer.log 2>&1 & echo \$! > trainer.pid' && exit 0"

echo "Trainer logs:  ssh ${HOSTS[$TRAINER_ID]} 'tail -f $REMOTE_DIR/trainer.log'"
echo "Worker logs:   ssh ${HOSTS[$WORKER_ID]} 'tail -f $REMOTE_DIR/worker.log'"

