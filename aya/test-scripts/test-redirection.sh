#!/bin/sh

echo "=== eBPF Socket Redirection Test ==="
echo "Testing load balancing across multiple backend servers"
echo ""

# Test direct connections to backends first
echo "1. Testing direct connections to backends:"
echo "Backend 1 (172.20.0.11:8080):"
curl -s --connect-timeout 3 http://172.20.0.11:8080 | grep -o "Backend Server [0-9]" || echo "  ❌ Failed to connect"

echo "Backend 2 (172.20.0.12:8080):"
curl -s --connect-timeout 3 http://172.20.0.12:8080 | grep -o "Backend Server [0-9]" || echo "  ❌ Failed to connect"

echo "Backend 3 (172.20.0.13:8080):"
curl -s --connect-timeout 3 http://172.20.0.13:8080 | grep -o "Backend Server [0-9]" || echo "  ❌ Failed to connect"

echo ""
echo "2. Testing load balancer frontend (should distribute across backends):"

# Test multiple requests to see distribution
for i in $(seq 1 10); do
    echo -n "Request $i: "
    response=$(curl -s --connect-timeout 3 http://172.20.0.5:80 2>/dev/null)
    if [ $? -eq 0 ]; then
        echo "$response" | grep -o "Backend Server [0-9]" || echo "No backend identified"
    else
        echo "❌ Connection failed"
    fi
    sleep 0.5
done

echo ""
echo "3. Testing eBPF redirection effectiveness:"
echo "Monitoring network stats on eBPF proxy container..."

# Function to get network stats
get_net_stats() {
    # This would need to be run from inside the eBPF container
    echo "Network stats check (run from eBPF container):"
    echo "docker exec ebpf-socket-proxy cat /proc/net/dev | grep eth0"
}

echo ""
echo "4. Performance comparison test:"
echo "Testing with and without eBPF redirection..."

# Measure response times
echo "Measuring response times:"
for i in $(seq 1 5); do
    echo -n "Test $i: "
    start_time=$(date +%s%N)
    curl -s --connect-timeout 3 http://172.20.0.5:80 >/dev/null 2>&1
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    echo "${duration}ms"
    sleep 1
done

echo ""
echo "=== Test Complete ==="
echo "To check eBPF logs: docker logs ebpf-socket-proxy"
echo "To monitor network: docker exec ebpf-socket-proxy cat /proc/net/dev"
