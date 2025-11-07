#!/usr/bin/env bash
set -euo pipefail

# Deploy controller for multicast test

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

CONFIG_FILE="$SCRIPT_DIR/controller-config.toml"
LOG_FILE="$SCRIPT_DIR/controller.log"
PID_FILE="$SCRIPT_DIR/controller.pid"
CONTROLLER_BIN="$REPO_ROOT/target/release/controller"

echo "=== Starting Multicast Test Controller ==="

# Build controller if not exists
if [[ ! -x "$CONTROLLER_BIN" ]]; then
    echo "Controller binary not found. Building..."
    cd "$REPO_ROOT"
    cargo build --release -p controller
fi

# Start PostgreSQL
echo "Starting PostgreSQL (if needed)..."
(cd "$REPO_ROOT" && ./start-database.sh)

# Wait for PostgreSQL to be ready
sleep 2

# Start controller
echo "Launching controller..."
RUST_LOG=${RUST_LOG:-info} nohup "$CONTROLLER_BIN" \
    --config-path "$CONFIG_FILE" \
    > "$LOG_FILE" 2>&1 &

CONTROLLER_PID=$!
echo $CONTROLLER_PID > "$PID_FILE"

echo ""
echo "✅ Controller started"
echo "   PID: $CONTROLLER_PID"
echo "   Config: $CONFIG_FILE"
echo "   Logs: $LOG_FILE"
echo ""
echo "Monitor logs:"
echo "  tail -f $LOG_FILE"
echo ""
echo "Stop controller:"
echo "  kill \$(cat $PID_FILE)"

