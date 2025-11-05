#!/usr/bin/env bash
set -euo pipefail

# Bare-metal deployment for ml-tensor-multicast (multicast branch)
# Similar to pyo3 branch's deploy.sh but uses nextmini_py.Dataplane API

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

CONTROLLER_ADDR=${CONTROLLER_ADDR:-"ws://206.12.89.244:3000"}
REMOTE_DIR=${REMOTE_DIR:-"~/ml-tensor-multicast"}
ITERATIONS=${ITERATIONS:-5}
TENSOR_ROWS=${TENSOR_ROWS:-128}
TENSOR_COLS=${TENSOR_COLS:-128}

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
cp "$WHEEL" "$WORK_DIR/nextmini_py.whl"

BUNDLE="$WORK_DIR/ml-tensor-bundle.tgz"
tar -czf "$BUNDLE" -C "$WORK_DIR" trainer.py worker.py torch_serializer.py nextmini_py.whl

# Deploy to each node
for NODE_ID in "${!HOSTS[@]}"; do
  HOST_SPEC="${HOSTS[$NODE_ID]}"
  LOCAL_IP="${IPS[$NODE_ID]}"
  LOCAL_PORT="${PORTS[$NODE_ID]}"

  echo "Uploading bundle to node $NODE_ID ($HOST_SPEC)"
  scp -q "$BUNDLE" "$HOST_SPEC:~/ml-tensor-bundle.tgz"
  ssh -q "$HOST_SPEC" "mkdir -p $REMOTE_DIR && tar xzf ~/ml-tensor-bundle.tgz -C $REMOTE_DIR && rm -f ~/ml-tensor-bundle.tgz"
  
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

  # Install dependencies on remote node
  ssh -q "$HOST_SPEC" "python3 -m pip install --user --quiet torch numpy toml >/dev/null 2>&1 || true"
  ssh -q "$HOST_SPEC" "python3 -m pip install --user --quiet $REMOTE_DIR/nextmini_py.whl --force-reinstall"
done

TRAINER_HOST="${HOSTS[$TRAINER_ID]}"
WORKER_HOST="${HOSTS[$WORKER_ID]}"

# Start worker first
ssh -q "$WORKER_HOST" "cd $REMOTE_DIR && nohup python3 worker.py \
  --config node-config.toml \
  --trainer-node-id $TRAINER_ID \
  --src-port 5000 \
  --dst-port 4000 \
  --max-iterations $ITERATIONS \
  > worker.log 2>&1 & echo \$! > worker.pid"

sleep 2

# Start trainer
ssh -q "$TRAINER_HOST" "cd $REMOTE_DIR && nohup python3 trainer.py \
  --config node-config.toml \
  --worker-node-id $WORKER_ID \
  --src-port 4000 \
  --dst-port 5000 \
  --tensor-size $TENSOR_ROWS $TENSOR_COLS \
  --iterations $ITERATIONS \
  > trainer.log 2>&1 & echo \$! > trainer.pid"

echo "Trainer logs:  ssh ${HOSTS[$TRAINER_ID]} 'tail -f $REMOTE_DIR/trainer.log'"
echo "Worker logs:   ssh ${HOSTS[$WORKER_ID]} 'tail -f $REMOTE_DIR/worker.log'"

