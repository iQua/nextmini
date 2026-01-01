#!/usr/bin/env bash
set -euo pipefail

# Memory monitoring script for namespace experiments
# Records memory usage before and after nodes connect

output_file="${1:-memory-report.txt}"
controller_container="${2:-controller}"

echo "=== Memory Monitor ===" | tee "$output_file"
echo "Output: $output_file"
echo ""

# Wait for controller to be ready
echo "Waiting for controller (port 3000)..."
while ! nc -z localhost 3000 2>/dev/null; do
    sleep 1
done
echo "Controller is ready."
sleep 2

# Record memory before nodes connect
echo "" | tee -a "$output_file"
echo "=== Memory BEFORE nodes connect ===" | tee -a "$output_file"
echo "Timestamp: $(date -Iseconds)" | tee -a "$output_file"
free -h | tee -a "$output_file"
mem_before=$(free -b | awk '/Mem:/ {print $3}')

# Wait for all nodes to connect by watching controller logs
echo ""
echo "Waiting for all nodes to connect..."
docker logs -f "$controller_container" 2>&1 | while read -r line; do
    if [[ "$line" == *"All dataplane nodes have connected"* ]]; then
        # Extract wiring time from log
        echo "$line" | tee -a "$output_file"
        break
    fi
done

# Small delay to let memory settle
sleep 2

# Record memory after nodes connect
echo "" | tee -a "$output_file"
echo "=== Memory AFTER nodes connect ===" | tee -a "$output_file"
echo "Timestamp: $(date -Iseconds)" | tee -a "$output_file"
free -h | tee -a "$output_file"
mem_after=$(free -b | awk '/Mem:/ {print $3}')

# Calculate difference
mem_diff=$((mem_after - mem_before))
mem_diff_mb=$((mem_diff / 1024 / 1024))

echo "" | tee -a "$output_file"
echo "=== Summary ===" | tee -a "$output_file"
echo "Memory before: $((mem_before / 1024 / 1024)) MB" | tee -a "$output_file"
echo "Memory after:  $((mem_after / 1024 / 1024)) MB" | tee -a "$output_file"
echo "Difference:    ${mem_diff_mb} MB" | tee -a "$output_file"

echo ""
echo "Report saved to: $output_file"
