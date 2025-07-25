#!/bin/sh
# Wait for network and routes to be ready
sleep 30

SOCKS_PROXY=172.16.8.5:8081
TARGET=http://172.16.8.10:8080/large_test.dat

echo "Starting curl throughput test (unlimited duration)..."
echo "Target: $TARGET"
echo "Proxy: $SOCKS_PROXY"
echo ""

# Execute curl command without time limits - download the entire file
# This allows curl to run at maximum speed without artificial constraints
curl --socks5-hostname "$SOCKS_PROXY" "$TARGET" \
  -o /dev/null -s \
  -w "\n--- Performance Metrics (Full Download) ---\n\
HTTP Code: %{http_code}\n\
Total Time: %{time_total}s\n\
Download Speed: %{speed_download} bytes/sec\n\
Content Length Downloaded: %{size_download} bytes\n\
--- End Metrics ---\n\n" | tee /tmp/curl_output.txt

# Calculate Gbps from the download speed
if [ -f /tmp/curl_output.txt ]; then
    speed_bps=$(grep "Download Speed:" /tmp/curl_output.txt | awk '{print $3}' | cut -d'.' -f1)
    if [ "$speed_bps" -gt 0 ] 2>/dev/null; then
        # Convert bytes/sec to Gbps: (bytes * 8) / 1000000000
        speed_gbps_x1000=$((speed_bps * 8 / 1000000))
        speed_gbps_int=$((speed_gbps_x1000 / 1000))
        speed_gbps_dec=$((speed_gbps_x1000 % 1000))
        echo "Download Speed: ${speed_gbps_int}.$(printf "%03d" $speed_gbps_dec) Gbps"
    fi
fi
