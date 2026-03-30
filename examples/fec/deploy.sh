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

resolve_ssh_key_id() {
  local key_id=""

  if [[ ! -f "$DO_SSH_PUBLIC_KEY_PATH" ]]; then
    echo "SSH public key file does not exist: $DO_SSH_PUBLIC_KEY_PATH" >&2
    exit 1
  fi

  key_id="$(
    doctl compute ssh-key list --format ID,Name --no-header \
      | awk -v name="$DO_SSH_KEY_NAME" '$2 == name { print $1; exit }'
  )"

  if [[ -n "$key_id" ]]; then
    printf '%s\n' "$key_id"
    return
  fi

  echo "Importing DigitalOcean SSH key '$DO_SSH_KEY_NAME' from $DO_SSH_PUBLIC_KEY_PATH"
  doctl compute ssh-key import "$DO_SSH_KEY_NAME" --public-key-file "$DO_SSH_PUBLIC_KEY_PATH" >/dev/null

  key_id="$(
    doctl compute ssh-key list --format ID,Name --no-header \
      | awk -v name="$DO_SSH_KEY_NAME" '$2 == name { print $1; exit }'
  )"

  if [[ -z "$key_id" ]]; then
    echo "Failed to resolve DigitalOcean SSH key ID for $DO_SSH_KEY_NAME" >&2
    exit 1
  fi

  printf '%s\n' "$key_id"
}

droplet_exists() {
  local name="$1"
  doctl compute droplet list --format Name --no-header \
    | awk -v name="$name" '$1 == name { found = 1 } END { exit found ? 0 : 1 }'
}

create_droplet() {
  local name="$1"
  local region="$2"
  local ssh_key_id="$3"

  if droplet_exists "$name"; then
    echo "Droplet already exists, skipping: $name"
    return
  fi

  echo "Creating $name in $region"
  doctl compute droplet create "$name" \
    --size "$DO_DROPLET_SIZE" \
    --image "$DO_DROPLET_IMAGE" \
    --region "$region" \
    --ssh-keys "$ssh_key_id" \
    --tag-names "$DO_DROPLET_TAGS" \
    --wait
}

print_droplet_summary() {
  local name="$1"
  doctl compute droplet list --format Name,PublicIPv4,Region,Status,Tags --no-header \
    | awk -v name="$name" '$1 == name { print }'
}

node_name() {
  local spec="$1"
  printf '%s\n' "${spec%%:*}"
}

node_region() {
  local spec="$1"
  if [[ "$spec" != *:* ]]; then
    echo "Invalid DO_NODES entry (expected name:region): $spec" >&2
    exit 1
  fi
  printf '%s\n' "${spec#*:}"
}

main() {
  require_cmd doctl
  require_cmd awk

  load_env

  require_var DO_SSH_PUBLIC_KEY_PATH
  require_var DO_SSH_KEY_NAME
  require_var DO_DROPLET_SIZE
  require_var DO_DROPLET_IMAGE
  require_var DO_DROPLET_TAGS
  require_var DO_NODES

  local ssh_key_id=""
  ssh_key_id="$(resolve_ssh_key_id)"

  local spec=""
  local name=""
  local region=""
  for spec in $DO_NODES; do
    name="$(node_name "$spec")"
    region="$(node_region "$spec")"
    create_droplet "$name" "$region" "$ssh_key_id"
  done

  echo
  echo "Droplet summary:"
  for spec in $DO_NODES; do
    name="$(node_name "$spec")"
    print_droplet_summary "$name"
  done
}

main "$@"
