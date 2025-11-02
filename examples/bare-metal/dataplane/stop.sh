#!/bin/bash
INTERACTIVE=${1:-}  # -i for interactive mode
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping all nodes..."
mapfile -t NODE_LINES < <(grep -E '^[0-9]+' "$SCRIPT_DIR/hosts.txt")

for node in "${NODE_LINES[@]}"; do
    IFS='|' read -r _ HOST _ <<< "$node"
    if [ "$INTERACTIVE" = "-i" ]; then
        ssh -t -o StrictHostKeyChecking=no "$HOST" "sudo pkill -f 'nextmini --config'"
    else
        ssh -o StrictHostKeyChecking=no "$HOST" "sudo pkill -f 'nextmini --config'" &
    fi
done

[ "$INTERACTIVE" != "-i" ] && wait

echo "Done."
