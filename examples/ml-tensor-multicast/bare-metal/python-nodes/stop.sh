#!/usr/bin/env bash
set -euo pipefail

# Clean up ALL python and nextmini processes on remote nodes

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found at $HOSTS_FILE" >&2
  exit 1
fi

declare -A HOSTS
while IFS='|' read -r NODE_ID HOST_SPEC _ _ _; do
  [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
  HOSTS[$NODE_ID]="$HOST_SPEC"
done < "$HOSTS_FILE"

for NODE_ID in "${!HOSTS[@]}"; do
  HOST_SPEC="${HOSTS[$NODE_ID]}"
  echo "==> Cleaning up all processes on node $NODE_ID ($HOST_SPEC)"
  
  ssh "$HOST_SPEC" bash <<'CLEANUP'
    # Kill all Python processes related to ml-tensor-multicast
    echo "  Stopping all Python trainer/worker processes..."
    pkill -f "python.*trainer.py" 2>/dev/null || true
    pkill -f "python.*worker.py" 2>/dev/null || true
    pkill -f ".venv/bin/python" 2>/dev/null || true
    
    # Kill all nextmini dataplane processes
    echo "  Stopping all nextmini dataplane processes..."
    sudo pkill -f "nextmini.*--node-id" 2>/dev/null || true
    sudo pkill -f "./nextmini" 2>/dev/null || true
    
    # Clean up PID files
    rm -f ~/ml-tensor-multicast/*.pid 2>/dev/null || true
    rm -f /mnt/ml-tensor-multicast/*.pid 2>/dev/null || true
    rm -f ~/nextmini/nextmini.pid 2>/dev/null || true
    
    echo "  Done."
CLEANUP
done

echo ""
echo "All processes cleaned up. You can now redeploy."
