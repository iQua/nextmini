#!/bin/bash
# Clean all containers, volumes, and processes

set -e

cd /home/ubuntu/nextmini/examples/routing/waterfilling

echo "Cleaning up..."

# Stop docker services
docker compose down -v 2>/dev/null || true

# Kill any controller processes on port 3000
CONTROLLER_PID=$(lsof -ti :3000 2>/dev/null || true)
if [ -n "$CONTROLLER_PID" ]; then
    echo "Killing controller process on port 3000 (PID: $CONTROLLER_PID)"
    kill -9 $CONTROLLER_PID 2>/dev/null || true
fi

# Kill any postgres processes on port 5432
POSTGRES_PID=$(lsof -ti :5432 2>/dev/null || true)
if [ -n "$POSTGRES_PID" ]; then
    echo "Killing postgres process on port 5432 (PID: $POSTGRES_PID)"
    kill -9 $POSTGRES_PID 2>/dev/null || true
fi

echo "✓ All containers, volumes, and ports cleaned"

