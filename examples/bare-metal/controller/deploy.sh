#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Check if database is running, start if not
if ! docker ps | grep -q nextmini-database; then
    echo "Starting PostgreSQL database..."
    # cd to REPO_ROOT before starting database (so it can find .env)
    (cd "$REPO_ROOT" && ./start-database.sh)
    sleep 5
else
    echo "Database already running."
fi

# Stop existing controller if running
pkill -f "$REPO_ROOT/target/release/controller" || true

# Start controller
echo "Starting controller..."
cd "$SCRIPT_DIR"
RUST_LOG=info nohup "$REPO_ROOT/target/release/controller" \
    > controller.log 2>&1 < /dev/null &
echo $! > controller.pid

sleep 2

# Verify it's running
if ps -p $(cat controller.pid) > /dev/null 2>&1; then
    echo ""
    echo "Controller started successfully!"
    echo "PID: $(cat controller.pid)"
    echo "Log: $SCRIPT_DIR/controller.log"
    echo ""
    echo "View logs: tail -f $SCRIPT_DIR/controller.log"
else
    echo "Error: Controller failed to start"
    tail -20 controller.log
    exit 1
fi
