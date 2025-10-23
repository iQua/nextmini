#!/bin/bash
# Test nextmini network connectivity
#
# Usage:
#   ./test_connectivity.sh
#   # or from docker container:
#   docker exec node3 bash /var/nextmini/scripts/test_connectivity.sh

echo "Testing Nextmini Virtual Network Connectivity"
echo "=============================================="

# Check TUN interface (utun or tun0)
echo -e "\n1. Checking TUN interface..."
if ip addr show utun &>/dev/null; then
    echo "   [OK] TUN interface 'utun' exists"
    ip addr show utun | grep "inet "
else
    echo "   [FAIL] TUN interface not found (checked utun and tun0)"
    exit 1
fi

# Test connectivity to other nodes
echo -e "\n2. Testing connectivity to other nodes..."

NODES=(
    "10.0.0.1:Node1 (Trainer 1)"
    "10.0.0.2:Node2 (Trainer 2)"
    "10.0.0.3:Node3 (Orchestrator)"
    "10.0.0.4:Node4 (Inference)"
)

for node_info in "${NODES[@]}"; do
    IFS=':' read -r ip name <<< "$node_info"
    echo -n "   Testing connection to $name ($ip)... "

    if ping -c 1 -W 2 $ip &>/dev/null; then
        echo "[OK]"
    else
        echo "[FAIL]"
    fi
done

# Test HTTP services (if inference server is running)
echo -e "\n3. Testing service availability..."
echo -n "   Testing Inference Server (http://10.0.0.4:8000)... "

if command -v curl &>/dev/null; then
    if curl -s --connect-timeout 2 http://10.0.0.4:8000/health &>/dev/null || \
       curl -s --connect-timeout 2 http://10.0.0.4:8000/v1/models &>/dev/null; then
        echo "[OK]"
    else
        echo "[NOT RUNNING]"
    fi
else
    echo "[SKIP - curl not installed]"
fi

echo -e "\n=============================================="
echo "Test complete!"
