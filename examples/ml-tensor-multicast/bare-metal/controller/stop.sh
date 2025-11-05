#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PID_FILE="$SCRIPT_DIR/controller.pid"

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    echo "Stopping controller (PID: $PID)"
    kill "$PID" 2>/dev/null || echo "Process already stopped"
    rm -f "$PID_FILE"
else
    echo "No PID file found"
fi

