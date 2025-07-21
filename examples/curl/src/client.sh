#!/bin/sh
# Wait for network and routes to be ready
sleep 30

SOCKS_PROXY=172.16.8.5:8081
TARGET=http://172.16.8.8:8080

echo "Starting simple curl client via SOCKS5 proxy $SOCKS_PROXY -> $TARGET"

while true; do
  echo "[$(date +%H:%M:%S)] Sending request..."
  curl -s --socks5-hostname "$SOCKS_PROXY" "$TARGET" > /dev/null
  sleep 1
done
