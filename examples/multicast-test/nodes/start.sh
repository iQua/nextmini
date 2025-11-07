#!/usr/bin/env bash
set -euo pipefail

# Start multicast test on remote nodes

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

# Configuration
REMOTE_DIR=${REMOTE_DIR:-"\$HOME/multicast-test"}
GROUP_ID=${GROUP_ID:-520}
ITERATIONS=${ITERATIONS:-10}
INTERVAL=${INTERVAL:-2.0}

if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "ERROR: hosts.txt not found at $HOSTS_FILE"
    exit 1
fi

echo "=== Starting Multicast Test ==="
echo "Remote directory: $REMOTE_DIR"
echo "Group ID: $GROUP_ID"
echo "Iterations: $ITERATIONS"
echo "Interval: $INTERVAL seconds"
echo ""

# Parse hosts.txt (format: NODE_ID | HOST | LOCAL_IP | ROLE | PORT)
declare -A HOSTS IPS ROLES PORTS
SENDER_ID=""
declare -a RECEIVER_IDS

while IFS='|' read -r NODE_ID HOST_SPEC LOCAL_IP ROLE PORT _; do
    # Skip empty lines and comments
    [[ -z "$NODE_ID" || "$NODE_ID" =~ ^# ]] && continue
    
    HOSTS[$NODE_ID]="$HOST_SPEC"
    IPS[$NODE_ID]="$LOCAL_IP"
    ROLES[$NODE_ID]="${ROLE,,}"
    PORTS[$NODE_ID]="${PORT:-8080}"
    
    if [[ "${ROLES[$NODE_ID]}" == "sender" ]]; then
        SENDER_ID="$NODE_ID"
    elif [[ "${ROLES[$NODE_ID]}" == "receiver" ]]; then
        RECEIVER_IDS+=("$NODE_ID")
    fi
done < "$HOSTS_FILE"

if [[ -z "$SENDER_ID" ]]; then
    echo "ERROR: No sender found in hosts.txt"
    exit 1
fi

if [[ ${#RECEIVER_IDS[@]} -eq 0 ]]; then
    echo "ERROR: No receivers found in hosts.txt"
    exit 1
fi

echo "Found sender: Node $SENDER_ID"
echo "Found ${#RECEIVER_IDS[@]} receiver(s): ${RECEIVER_IDS[*]}"
echo ""

# Start ALL nodes simultaneously (sender will wait inside for everyone to connect)
echo "Starting all nodes..."

# Start sender
SENDER_HOST="${HOSTS[$SENDER_ID]}"
echo "--- Starting sender on node $SENDER_ID ($SENDER_HOST) ---"

ssh "$SENDER_HOST" "bash -s" <<REMOTE_START
    set -euo pipefail
    cd $REMOTE_DIR
    
    # Activate venv
    source .venv/bin/activate
    
    # Start sender in background
    nohup python -u sender.py \
        --config node-config.toml \
        --iterations $ITERATIONS \
        --interval $INTERVAL \
        > sender.log 2>&1 &
    
    echo \$! > sender.pid
    echo "✅ Sender started (PID: \$(cat sender.pid))"
REMOTE_START

echo "✅ Node $SENDER_ID sender started"

# Start receivers immediately after sender
for NODE_ID in "${RECEIVER_IDS[@]}"; do
    HOST_SPEC="${HOSTS[$NODE_ID]}"
    
    echo "--- Starting receiver on node $NODE_ID ($HOST_SPEC) ---"
    
    ssh "$HOST_SPEC" "bash -s" <<REMOTE_START
        set -euo pipefail
        cd $REMOTE_DIR
        
        # Activate venv
        source .venv/bin/activate
        
        # Start receiver in background
        nohup python -u receiver.py \
            --config node-config.toml \
            --sender-node-id $SENDER_ID \
            --iterations $ITERATIONS \
            > receiver.log 2>&1 &
        
        echo \$! > receiver.pid
        echo "✅ Receiver started (PID: \$(cat receiver.pid))"
REMOTE_START
    
    echo "✅ Node $NODE_ID receiver started"
done

echo ""
echo "=== Multicast Test Started ==="
echo ""
echo "All nodes started simultaneously."
echo "Sender will wait 10s for all nodes to connect, then 5s more for receivers to join."
echo ""
echo "Monitor logs:"
echo "  Sender:    ssh ${HOSTS[$SENDER_ID]} 'tail -f $REMOTE_DIR/sender.log'"
for NODE_ID in "${RECEIVER_IDS[@]}"; do
    echo "  Receiver $NODE_ID: ssh ${HOSTS[$NODE_ID]} 'tail -f $REMOTE_DIR/receiver.log'"
done
echo ""
echo "Stop all:"
echo "  ./stop.sh"
echo ""
