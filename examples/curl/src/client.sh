#!/bin/sh
# Wait for network and routes to be ready
sleep 30

SOCKS_PROXY=172.16.8.5:8081
TARGET=http://172.16.8.8:8080

echo "Starting simple curl client via SOCKS5 proxy $SOCKS_PROXY -> $TARGET"
echo "Press Ctrl+C to stop"
echo ""

counter=1

while true; do
  echo "========================"
  echo "[$(date +%H:%M:%S)] Request #$counter"
  echo ""

  # Use curl's built-in timing and response display
  curl --socks5-hostname "$SOCKS_PROXY" "$TARGET" \
    -w "\n\n--- Performance Metrics ---\n\
HTTP Code: %{http_code}\n\
Total Time: %{time_total}s\n\
DNS Lookup: %{time_namelookup}s\n\
TCP Connect: %{time_connect}s\n\
TLS Handshake: %{time_appconnect}s\n\
Pre-transfer: %{time_pretransfer}s\n\
Redirect: %{time_redirect}s\n\
Start Transfer: %{time_starttransfer}s\n\
Download Speed: %{speed_download} bytes/sec\n\
Upload Speed: %{speed_upload} bytes/sec\n\
Content Length: %{size_download} bytes\n\
Request Size: %{size_request} bytes\n\
--- End Metrics ---\n\n"

  counter=$((counter + 1))
  sleep 2
done
