#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env.local}"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required variable: $name" >&2
    exit 1
  fi
}

load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing env file: $ENV_FILE" >&2
    echo "Copy $SCRIPT_DIR/.env.local.example to $ENV_FILE and fill in local values." >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

delete_droplet() {
  local name="$1"
  local droplet_ids=()

  mapfile -t droplet_ids < <(
    doctl compute droplet list --format ID,Name --no-header \
      | awk -v name="$name" '$2 == name { print $1 }'
  )

  if (( ${#droplet_ids[@]} == 0 )); then
    echo "Droplet not found, skipping: $name"
    return
  fi

  echo "Deleting $name (${droplet_ids[*]})"
  doctl compute droplet delete "${droplet_ids[@]}" --force
}

node_name() {
  local spec="$1"
  printf '%s\n' "${spec%%:*}"
}

main() {
  require_cmd doctl
  require_cmd awk

  load_env

  require_var DO_NODES

  local spec=""
  local name=""
  for spec in $DO_NODES; do
    name="$(node_name "$spec")"
    delete_droplet "$name"
  done
}

main "$@"
