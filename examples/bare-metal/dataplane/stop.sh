#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping all nodes..."
while IFS='|' read -r _ HOST _; do
    ssh -o StrictHostKeyChecking=no "$HOST" "sudo pkill -f 'nextmini --config'" &
done < <(grep -E '^[0-9]+' "$SCRIPT_DIR/hosts.txt")
wait

echo "Done."
