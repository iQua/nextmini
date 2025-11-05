#!/usr/bin/env bash
set -euo pipefail

# Controller deployment script for ml-tensor-multicast

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)

CONFIG_FILE="$SCRIPT_DIR/config.toml"
LOG_FILE="$SCRIPT_DIR/controller.log"
CONTROLLER_BIN="$REPO_ROOT/target/release/controller"

if [[ ! -x "$CONTROLLER_BIN" ]]; then
  echo "controller binary not found at $CONTROLLER_BIN"
  echo "Run: cargo build --release -p controller"
  exit 1
fi

echo "Starting PostgreSQL (if needed)"
(cd "$REPO_ROOT" && ./start-database.sh)

echo "Launching controller"
mkdir -p "$SCRIPT_DIR"
RUST_LOG=${RUST_LOG:-info} nohup "$CONTROLLER_BIN" \
  --config "$CONFIG_FILE" \
  > "$LOG_FILE" 2>&1 &
echo $! > "$SCRIPT_DIR/controller.pid"
echo "Controller PID: $(cat "$SCRIPT_DIR/controller.pid")"
echo "Logs: $LOG_FILE"

