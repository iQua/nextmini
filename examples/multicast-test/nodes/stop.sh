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
        # Kill Python processes (use broader patterns to catch all)
        echo "  Stopping Python processes..."
        pkill -9 -f "receiver.py" 2>/dev/null || true
        pkill -9 -f "sender.py" 2>/dev/null || true
        pkill -9 -f "python.*receiver" 2>/dev/null || true
        pkill -9 -f "python.*sender" 2>/dev/null || true
        
        # Kill nextmini dataplane processes
        echo "  Stopping nextmini dataplane processes..."
        sudo pkill -9 -f "nextmini.*--node-id" 2>/dev/null || true
        sudo pkill -9 -f "./nextmini" 2>/dev/null || true
        sudo pkill -9 nextmini 2>/dev/null || true
        
        # Clean up PID files
        rm -f ~/multicast-test/*.pid 2>/dev/null || true
        rm -f \$HOME/multicast-test/*.pid 2>/dev/null || true
        
        echo "  Done."
CLEANUP
    
    echo "✅ Node $NODE_ID cleaned up"
done < "$HOSTS_FILE"

echo ""
echo "=== All processes stopped and cleaned up ==="
echo "You can now redeploy."
