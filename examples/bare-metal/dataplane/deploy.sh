#!/bin/bash
set -e

INTERACTIVE=${1:-}  # -i for interactive mode

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Prepare bundle
BUNDLE="/tmp/nextmini-bundle.tgz"
tar -czf "$BUNDLE" \
    -C "$REPO_ROOT/target/release" nextmini \
    -C "$REPO_ROOT" server_cert.pem server_key.pem \
    -C "$SCRIPT_DIR" node.toml

# Extract node lines from config
mapfile -t NODE_LINES < <(grep -E '^[0-9]+' "$SCRIPT_DIR/hosts.txt")

# Upload bundle to all hosts (parallel)
for node in "${NODE_LINES[@]}"; do
    IFS='|' read -r _ HOST _ <<< "$node"
    scp -o StrictHostKeyChecking=no "$BUNDLE" "$HOST:~/nextmini-bundle.tgz" &
done
wait

# Start nodes
echo "Starting nodes..."
for node in "${NODE_LINES[@]}"; do
    IFS='|' read -r ID USER_HOST_PORT PUBLIC_IP <<< "$node"
    
    if [ "$INTERACTIVE" = "-i" ]; then
        # Interactive: use -t for password prompt, serial execution
        ssh -t -o StrictHostKeyChecking=no "$USER_HOST_PORT" "mkdir -p ~/nextmini && tar xzf ~/nextmini-bundle.tgz -C ~/nextmini && cd ~/nextmini && sudo sh -c \"RUST_LOG=info nohup ./nextmini --config-path node.toml --public-network-addr $PUBLIC_IP --private-network-addr $PUBLIC_IP --node-id $ID > node.log 2>&1 < /dev/null & echo \\\$! > nextmini.pid\""
    else
        # Non-interactive: parallel execution
        ssh -n -f -o StrictHostKeyChecking=no "$USER_HOST_PORT" "mkdir -p ~/nextmini && tar xzf ~/nextmini-bundle.tgz -C ~/nextmini && cd ~/nextmini && sudo RUST_LOG=info nohup ./nextmini --config-path node.toml --public-network-addr $PUBLIC_IP --private-network-addr $PUBLIC_IP --node-id $ID > node.log 2>&1 < /dev/null & echo \$! > nextmini.pid; exit 0" &
    fi
done

[ "$INTERACTIVE" != "-i" ] && wait

echo ""
echo "All nodes deployed."
