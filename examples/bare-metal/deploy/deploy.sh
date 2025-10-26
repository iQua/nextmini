#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Prepare bundle
BUNDLE="/tmp/nextmini-bundle.tgz"
tar -czf "$BUNDLE" \
    -C "$REPO_ROOT/target/release" nextmini \
    -C "$REPO_ROOT" server_cert.pem server_key.pem \
    -C "$SCRIPT_DIR" node.toml

# Extract node lines from config
mapfile -t NODE_LINES < <(grep -E '^[0-9]+' "$SCRIPT_DIR/nodes.conf")

# Upload bundle to all hosts (parallel)
for node in "${NODE_LINES[@]}"; do
    IFS='|' read -r _ HOST _ <<< "$node"
    scp -o StrictHostKeyChecking=no "$BUNDLE" "$HOST:~/nextmini-bundle.tgz" &
done
wait

# Start nodes (parallel ssh commands)
echo "Starting nodes in parallel..."
for node in "${NODE_LINES[@]}"; do
    IFS='|' read -r ID USER_HOST_PORT PUBLIC_IP <<< "$node"

    ssh -n -f -o StrictHostKeyChecking=no "$USER_HOST_PORT" "mkdir -p ~/nextmini && tar xzf ~/nextmini-bundle.tgz -C ~/nextmini && cd ~/nextmini && sudo -E nohup ./nextmini --config-path node.toml --public-network-addr $PUBLIC_IP --private-network-addr $PUBLIC_IP --node-id $ID > node.log 2>&1 < /dev/null & echo \$! > nextmini.pid; exit 0"
done

wait

echo ""
echo "All nodes deployed."
