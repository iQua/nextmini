#!/usr/bin/env bash
set -euo pipefail

# Stop controller and clean up

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PID_FILE="$SCRIPT_DIR/controller.pid"

echo "=== Stopping Controller ==="

# Try to stop using PID file first
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    echo "Stopping controller (PID: $PID)..."
    kill $PID 2>/dev/null || echo "Process already stopped"
    rm -f "$PID_FILE"
else
    echo "No PID file found."
fi

# Ensure all controller processes are stopped
echo "Ensuring all controller processes are stopped..."
pkill -f "controller.*controller-config.toml" 2>/dev/null || true
pkill -f "./controller" 2>/dev/null || true

echo "Controller stopped and cleaned up"

