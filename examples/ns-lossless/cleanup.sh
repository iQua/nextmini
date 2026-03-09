#!/usr/bin/env bash
set -euo pipefail
shopt -s extglob

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
artifacts_root="${ARTIFACTS_ROOT:-${script_dir}/artifacts}"
config_path=""
bridge_name=""
n_nodes="5"

usage() {
  cat <<'EOF'
Usage: cleanup.sh [options]

Options:
  --config PATH     Dataplane config to read n_nodes/bridge_name from.
  --n-nodes N       Override n_nodes used for veth cleanup (default: 5).
  --bridge-name N   Override bridge_name used for bridge cleanup (default: isobr0).
  -h, --help        Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

if [[ -n "$config_path" && -f "$config_path" ]]; then
  n_nodes="$(awk -F'=' '/^[[:space:]]*n_nodes[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' "$config_path" 2>/dev/null || printf '%s' "$n_nodes")"
  if [[ -z "$bridge_name" ]]; then
    bridge_name="$(awk -F'"' '/^[[:space:]]*bridge_name[[:space:]]*=/{print $2; exit}' "$config_path" 2>/dev/null || true)"
  fi
fi

if [[ -z "$bridge_name" ]]; then
  bridge_name="isobr0"
fi

if [[ -d "$artifacts_root" ]]; then
  while IFS= read -r pid_file; do
    [[ -z "$pid_file" ]] && continue
    if [[ -s "$pid_file" ]]; then
      kill "$(cat "$pid_file")" >/dev/null 2>&1 || true
      wait "$(cat "$pid_file")" 2>/dev/null || true
    fi
    rm -f "$pid_file"
  done < <(find "$artifacts_root" -type f \( -name 'controller.pid' -o -name 'dataplane.pid' \) | sort)
fi

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

if ! [[ "$n_nodes" =~ ^[0-9]+$ ]]; then
  echo "Invalid n_nodes value '$n_nodes' (expected integer)." >&2
  exit 1
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

echo "Deleting namespace veth devices (veth0a..veth$((n_nodes - 1))a)"
for ((idx = 0; idx < n_nodes; idx++)); do
  dev="veth${idx}a"
  $SUDO ip link del "$dev" 2>/dev/null || true
done

MAX_NODES_PER_SHARD=1020
shards=$(((n_nodes + MAX_NODES_PER_SHARD - 1) / MAX_NODES_PER_SHARD))

echo "Deleting namespace bridges (base: $bridge_name, shards: $shards)"
for ((shard = 0; shard < shards; shard++)); do
  br="$(bridge_name_for_shard "$bridge_name" "$shard")"
  $SUDO ip link set "$br" down 2>/dev/null || true
  $SUDO ip link del "$br" 2>/dev/null || true
done

echo "Cleanup complete."
