#!/bin/sh
# Wait for network readiness
sleep 20

echo "Launching simple HTTP server on port 8080..."
python -m http.server 8080 --bind 0.0.0.0
