#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"
REMOTE_DIR=${REMOTE_DIR:-"~/ml-tensor-multicast"}

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found" >&2
  exit 1
fi

declare -A HOSTS

while IFS='|' read -r NODE_ID HOST_SPEC _; do
  [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
  HOSTS[$NODE_ID]="$HOST_SPEC"
done < "$HOSTS_FILE"

for NODE_ID in "${!HOSTS[@]}"; do
  HOST_SPEC="${HOSTS[$NODE_ID]}"
  echo "Stopping processes on node $NODE_ID ($HOST_SPEC)..."
  
  ssh -q "$HOST_SPEC" "
    if [ -f $REMOTE_DIR/trainer.pid ]; then
      kill \$(cat $REMOTE_DIR/trainer.pid) 2>/dev/null || true
      rm -f $REMOTE_DIR/trainer.pid
    fi
    if [ -f $REMOTE_DIR/worker.pid ]; then
      kill \$(cat $REMOTE_DIR/worker.pid) 2>/dev/null || true
      rm -f $REMOTE_DIR/worker.pid
    fi
  " || echo "Failed to stop node $NODE_ID"
done

echo "✅ All nodes stopped"

