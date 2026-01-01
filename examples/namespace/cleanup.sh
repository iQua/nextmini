#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    SUDO="sudo"
fi

# Stop tmux session (send Ctrl+C first for graceful shutdown)
if tmux has-session -t nextmini-namespace 2>/dev/null; then
    echo "Sending Ctrl+C to tmux panes..."
    tmux send-keys -t nextmini-namespace:0.0 C-c
    tmux send-keys -t nextmini-namespace:0.1 C-c
    sleep 3
    echo "Killing tmux session: nextmini-namespace"
    tmux kill-session -t nextmini-namespace 2>/dev/null || true
fi

# Stop docker containers
if [[ -f "${script_dir}/docker-compose.yml" ]]; then
    echo "Stopping docker containers"
    docker compose -f "${script_dir}/docker-compose.yml" down 2>/dev/null || true
fi

# Clean up veth interfaces and bridges
$SUDO bash -c '
for dev in $(ip -o link show type veth 2>/dev/null | awk -F": " "{print \$2}" | cut -d"@" -f1 | sort -u | grep -E "^veth[0-9]+[ab]$" || true); do
    echo "Deleting $dev"
    ip link del "$dev" 2>/dev/null || true
done

for br in $(ip -o link show type bridge 2>/dev/null | awk -F": " "{print \$2}" | cut -d"@" -f1 | grep -E "^isobr" | sort -u); do
    echo "Deleting $br"
    ip link set "$br" down 2>/dev/null || true
    ip link del "$br" 2>/dev/null || true
done
'

echo "Cleanup complete."
