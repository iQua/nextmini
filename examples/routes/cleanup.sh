#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

SESSION_NAME="${SESSION_NAME:-routes-iperf}"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT_DIR")}"
NETWORK_NAME="${PROJECT_NAME}_network"
LOCK_DIR="$ROOT_DIR/.run_routes_tmux.${SESSION_NAME}.lock"
LOG_FILE="$ROOT_DIR/.run_routes_tmux.${SESSION_NAME}.log"

usage() {
  cat <<EOF
Usage:
  ./cleanup.sh

Env:
  SESSION_NAME           (default: routes-iperf)
  COMPOSE_PROJECT_NAME   (default: basename of this directory)

What it does:
  - Kills tmux session (\$SESSION_NAME) if present
  - Kills any background runner process recorded in the lock dir
  - docker compose down + removes node*/controller/postgres containers
  - Removes the compose network (\$NETWORK_NAME)
  - Deletes temp files (.controller-config.toml.bak.*, .controller-config.toml.tmp.*, runner log, lock dir)
EOF
}

kill_pid_tree() {
  local pid="$1"
  if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi

  if command -v pkill >/dev/null 2>&1; then
    pkill -P "$pid" >/dev/null 2>&1 || true
  elif command -v pgrep >/dev/null 2>&1; then
    while read -r child; do
      kill "$child" >/dev/null 2>&1 || true
    done < <(pgrep -P "$pid" 2>/dev/null || true)
  fi

  kill "$pid" >/dev/null 2>&1 || true
  sleep 0.5
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

echo "Cleaning routes environment..."
echo "- session: $SESSION_NAME"
echo "- project: $PROJECT_NAME"
echo "- network: $NETWORK_NAME"

if command -v tmux >/dev/null 2>&1; then
  if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    tmux kill-session -t "$SESSION_NAME" >/dev/null 2>&1 || true
  fi
fi

if [[ -f "$LOCK_DIR/pid" ]]; then
  pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]]; then
    kill_pid_tree "$pid"
  fi
fi
rm -rf "$LOCK_DIR" >/dev/null 2>&1 || true

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    docker compose down --remove-orphans >/dev/null 2>&1 || true
  fi

  for name in controller postgres $(docker ps -a --format "{{.Names}}" 2>/dev/null | grep -E "^node[0-9]+$" || true); do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done

  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
fi

shopt -s nullglob
rm -f "$ROOT_DIR"/.controller-config.toml.bak.* "$ROOT_DIR"/.controller-config.toml.tmp.* "$LOG_FILE" >/dev/null 2>&1 || true
shopt -u nullglob

echo "Done."
