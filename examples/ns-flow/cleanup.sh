#!/usr/bin/env bash
set -euo pipefail
shopt -s extglob

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
session_name="${SESSION_NAME:-nextmini-ns-flow}"
config_path="${CONFIG_PATH:-${script_dir}/config.toml}"
bridge_name=""
n_nodes=""

usage() {
  cat <<'EOF'
Usage: cleanup.sh [options]

Options:
  --session NAME    tmux session name to kill (default: nextmini-ns-flow).
  --config PATH     Dataplane config to read n_nodes/bridge_name from (default: examples/ns-flow/config.toml).
  --n-nodes N       Override n_nodes used for veth cleanup (default: read from config.toml).
  --bridge-name N   Override bridge_name used for bridge cleanup (default: read from config.toml or isobr0).
  -h, --help        Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --session)
      session_name="${2:-}"
      if [[ -z "$session_name" ]]; then
        echo "--session requires a value" >&2
        usage
        exit 1
      fi
      shift 2
      ;;
    --config)
      config_path="${2:-}"
      if [[ -z "$config_path" ]]; then
        echo "--config requires a value" >&2
        usage
        exit 1
      fi
      shift 2
      ;;
    --n-nodes)
      n_nodes="${2:-}"
      if [[ -z "$n_nodes" ]]; then
        echo "--n-nodes requires a value" >&2
        usage
        exit 1
      fi
      shift 2
      ;;
    --bridge-name)
      bridge_name="${2:-}"
      if [[ -z "$bridge_name" ]]; then
        echo "--bridge-name requires a value" >&2
        usage
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  SUDO="sudo"
fi

# Stop tmux session (send Ctrl+C first for graceful shutdown)
if command -v tmux >/dev/null 2>&1; then
  if tmux has-session -t "$session_name" 2>/dev/null; then
    echo "Sending Ctrl+C to tmux panes..."
    while IFS= read -r pane_id; do
      [[ -z "$pane_id" ]] && continue
      tmux send-keys -t "$pane_id" C-c 2>/dev/null || true
    done < <(tmux list-panes -t "$session_name" -F '#{pane_id}' 2>/dev/null || true)
    sleep 3
    echo "Killing tmux session: $session_name"
    tmux kill-session -t "$session_name" 2>/dev/null || true
  fi
fi

compose_bin="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  if command -v docker-compose >/dev/null 2>&1; then
    compose_bin="docker-compose"
  else
    compose_bin=""
  fi
fi

# Stop docker containers
if [[ -n "$compose_bin" && -f "${script_dir}/docker-compose.yml" ]]; then
  echo "Stopping docker containers"
  $compose_bin -f "${script_dir}/docker-compose.yml" down 2>/dev/null || true
fi

# Best-effort; still useful to stop tmux/docker on non-Linux hosts.
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Skipping network cleanup (requires Linux + iproute2)."
  echo "Cleanup complete."
  exit 0
fi

if ! command -v ip >/dev/null 2>&1; then
  echo "Skipping network cleanup (missing iproute2 'ip' command)."
  echo "Cleanup complete."
  exit 0
fi

if [[ -z "$n_nodes" && -f "$config_path" ]]; then
  n_nodes="$(awk -F'=' '/^[[:space:]]*n_nodes[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' "$config_path" 2>/dev/null || true)"
fi
if [[ -z "$bridge_name" && -f "$config_path" ]]; then
  bridge_name="$(awk -F'"' '/^[[:space:]]*bridge_name[[:space:]]*=/{print $2; exit}' "$config_path" 2>/dev/null || true)"
fi
if [[ -z "$bridge_name" ]]; then
  bridge_name="isobr0"
fi

bridge_name_for_shard() {
  local base="$1"
  local shard="$2"

  if [[ "$shard" == "0" ]]; then
    printf '%s' "$base"
    return 0
  fi

  local prefix="${base%%+([0-9])}"
  if [[ -z "$prefix" || "$prefix" == "$base" ]]; then
    printf '%s%s' "$base" "$shard"
  else
    printf '%s%s' "$prefix" "$shard"
  fi
}

# Clean up veth interfaces and bridges
if [[ -n "$n_nodes" ]]; then
  if ! [[ "$n_nodes" =~ ^[0-9]+$ ]]; then
    echo "Invalid n_nodes value '$n_nodes' (expected integer)." >&2
    exit 1
  fi

  echo "Deleting namespace veth devices (veth0a..veth$((n_nodes - 1))a)"
  for ((idx = 0; idx < n_nodes; idx++)); do
    dev="veth${idx}a"
    $SUDO ip link del "$dev" 2>/dev/null || true
  done
else
  echo "n_nodes not found; skipping veth cleanup (use --n-nodes to override)."
fi

# Sharded bridges are /22s (1020 usable node IPs per shard).
MAX_NODES_PER_SHARD=1020
if [[ -n "$n_nodes" ]]; then
  shards=$(((n_nodes + MAX_NODES_PER_SHARD - 1) / MAX_NODES_PER_SHARD))
else
  shards=1
fi

echo "Deleting namespace bridges (base: $bridge_name, shards: $shards)"
for ((shard = 0; shard < shards; shard++)); do
  br="$(bridge_name_for_shard "$bridge_name" "$shard")"
  $SUDO ip link set "$br" down 2>/dev/null || true
  $SUDO ip link del "$br" 2>/dev/null || true
done

echo "Cleanup complete."
