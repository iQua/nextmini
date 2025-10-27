#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$SCRIPT_DIR/controller.pid" ]; then
    PID=$(cat "$SCRIPT_DIR/controller.pid")
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "Controller stopped (PID: $PID)"
    else
        echo "Controller not running"
    fi
    rm -f "$SCRIPT_DIR/controller.pid"
else
    pkill -f "^./controller" && echo "Controller stopped" || echo "Controller not running"
fi

# Clean up log
rm -f "$SCRIPT_DIR/controller.log"
echo "Log cleaned."
