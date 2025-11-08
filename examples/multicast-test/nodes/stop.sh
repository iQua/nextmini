#!/usr/bin/env bash
set -euo pipefail

# Clean up ALL python and nextmini processes on remote nodes

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

if [ ! -f "$HOSTS_FILE" ]; then
    echo "ERROR: hosts.txt not found"
    exit 1
fi

echo "=== Cleaning up Multicast Test on All Nodes ==="

while IFS='|' read -r NODE_ID HOST_SPEC LOCAL_IP ROLE PORT _; do
    # Skip empty lines and comments
    [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
    
    echo ""
    echo "--- Cleaning up node $NODE_ID ($HOST_SPEC) ---"
    
    ssh "$HOST_SPEC" bash <<'CLEANUP'
        # Kill Python processes
        echo "  Stopping Python processes..."
        pkill -f "python3.*receiver.py" 2>/dev/null || true
        pkill -f "python3.*sender.py" 2>/dev/null || true
        pkill -f "venv/bin/python" 2>/dev/null || true
        
        # Kill nextmini dataplane processes
        echo "  Stopping nextmini dataplane processes..."
        sudo pkill -f "nextmini.*--node-id" 2>/dev/null || true
        sudo pkill -f "./nextmini" 2>/dev/null || true
        sudo pkill nextmini 2>/dev/null || true
        
        # Clean up PID files and logs (optional)
        rm -f ~/multicast-test/*.pid 2>/dev/null || true
        
        echo "  Done."
CLEANUP
    
    echo "✅ Node $NODE_ID cleaned up"
done < "$HOSTS_FILE"

echo ""
echo "=== All processes stopped and cleaned up ==="
echo "You can now redeploy."
