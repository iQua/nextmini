#!/usr/bin/env bash
set -euo pipefail

# Memory monitoring script for namespace experiments
# Usage: monitor-memory.sh <n_nodes> <signal_file> <report_file>

n_nodes="${1:-0}"
signal_file="${2:-}"
memory_report="${3:-memory-report.txt}"

node_count="$n_nodes"
if [[ "$node_count" == "0" ]]; then
  node_count="unknown"
fi

echo ""
echo "========================================"
echo "Run: n_nodes=${node_count} at $(date -Iseconds)"
echo "========================================"

# Wait for controller to be ready
echo "Waiting for controller (port 3000)..."
while ! nc -z localhost 3000 2>/dev/null; do
  sleep 1
done
echo "Controller is ready."
sleep 1

# Record memory before nodes connect
echo ""
echo "--- Memory BEFORE nodes connect ---"
free -h
mem_before=$(free -b | awk '/Mem:/ {print $3}')

# Signal dataplane to start (if signal file specified)
if [[ -n "$signal_file" ]]; then
  echo ""
  echo "Signaling dataplane to start..."
  touch "$signal_file"
fi

# Wait for all nodes to connect
echo ""
echo "Waiting for all nodes to connect..."
wiring_time=""
while read -r line; do
  if echo "$line" | grep -q 'All dataplane nodes have connected'; then
    echo "$line"
    wiring_time=$(echo "$line" | sed -n 's/.*It takes \([0-9.]*\) seconds.*/\1/p')
    break
  fi
done < <(docker logs -f controller 2>&1)

sleep 1

# Record memory after nodes connect
echo ""
echo "--- Memory AFTER nodes connect ---"
free -h
mem_after=$(free -b | awk '/Mem:/ {print $3}')

# Calculate differences
mem_diff=$((mem_after - mem_before))
mem_before_mb=$((mem_before / 1024 / 1024))
mem_after_mb=$((mem_after / 1024 / 1024))
mem_diff_mb=$((mem_diff / 1024 / 1024))

# Display and save summary
echo ""
echo "--- Summary ---"
{
  echo ""
  echo "========================================"
  echo "Run: n_nodes=${node_count} at $(date -Iseconds)"
  echo "========================================"
  echo "Memory before: ${mem_before_mb} MB"
  echo "Memory after:  ${mem_after_mb} MB"
  echo "Difference:    ${mem_diff_mb} MB"
  echo "Nodes:         ${node_count}"
  if [[ -n "$wiring_time" ]]; then
    echo "Wiring time:   ${wiring_time}s"
  else
    echo "Wiring time:   n/a"
  fi
  if [[ "$n_nodes" -gt 0 ]]; then
    per_node_mb=$((mem_diff_mb / n_nodes))
    echo "Per-node:      ${per_node_mb} MB"
  else
    echo "Per-node:      n/a"
  fi
  echo ""
} | tee -a "$memory_report"

echo "Report appended to: $memory_report"
echo ""
echo "Press Enter to close this pane..."
read -r
