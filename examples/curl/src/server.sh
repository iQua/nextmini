#!/bin/sh
# Wait for network readiness
sleep 5

# Create a large (100 GB) sparse file for unlimited throughput measurement.
# This is instant and uses minimal disk space.
FILE_SIZE_GB=50
echo "Generating ${FILE_SIZE_GB}GB sparse test file large_test.dat ..."
truncate -s ${FILE_SIZE_GB}G large_test.dat

echo "Launching simple HTTP server on port 8080..."
python -m http.server 8080 --bind 0.0.0.0
