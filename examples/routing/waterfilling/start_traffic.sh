#!/bin/bash
# Start traffic for waterfilling example

set -e

echo "Starting iperf3 servers on node2..."
docker exec -d node2 bash -c "iperf3 -s -p 5201 &"
docker exec -d node2 bash -c "iperf3 -s -p 5202 &"
docker exec -d node2 bash -c "iperf3 -s -p 5203 &"
docker exec -d node2 bash -c "iperf3 -s -p 5204 &"
docker exec -d node2 bash -c "iperf3 -s -p 5205 &"
docker exec -d node2 bash -c "iperf3 -s -p 5206 &"

sleep 2

echo "Starting iperf3 clients on node1..."
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5201 -b 20M -t 0 &"
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5202 -b 20M -t 0 &"
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5203 -b 20M -t 0 &"
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5204 -b 20M -t 0 &"
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5205 -b 20M -t 0 &"
docker exec -d node1 bash -c "iperf3 -c 10.0.0.2 -p 5206 -b 20M -t 0 &"

sleep 2

echo "Traffic started: 6 flows, 20 Mbps each, 120 Mbps total"
echo ""
echo "Check flows:"
echo "  docker exec postgres psql -U pgusr -d nextmini -c 'SELECT COUNT(*) FROM app_flows WHERE is_finished=FALSE'"
echo ""
echo "Run algorithm:"
echo "  uv run run_waterfilling.py"

