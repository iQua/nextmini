#!/bin/bash

echo "=== eBPF Socket Redirection Test (Based on blog) ==="

cd "$(dirname "$0")"

echo "Choose test method:"
echo "1. Native build and test"
echo "2. Docker test"
read -p "Enter choice (1 or 2): " choice

case $choice in
    1)
        echo "Building eBPF program natively..."
        cargo build --release

        if [ $? -ne 0 ]; then
            echo "Build failed!"
            exit 1
        fi

        echo ""
        echo "=== Native Test Instructions ==="
        echo "1. Terminal 1: Monitor network stats"
        echo "   watch -n 1 \"cat /proc/net/dev\""
        echo ""
        echo "2. Terminal 2: Run eBPF program (requires sudo)"
        echo "   sudo ./target/release/aya"
        echo ""
        echo "3. Terminal 3: Start test server"
        echo "   python3 -m http.server 8080"
        echo ""
        echo "4. Terminal 4: Test connections"
        echo "   curl localhost:8080"
        echo ""
        read -p "Start monitoring now? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Starting network monitoring..."
            echo "Open another terminal and run: sudo ./target/release/aya"
            watch -n 1 "cat /proc/net/dev"
        fi
        ;;
    2)
        echo "Building Docker image..."
        docker compose build

        if [ $? -ne 0 ]; then
            echo "Docker build failed!"
            exit 1
        fi

        echo ""
        echo "=== Docker Test Instructions ==="
        echo "1. Start the container:"
        echo " docker compose up "
        echo ""
        echo "2. In another terminal, test the setup:"
        echo "   docker exec aya-ebpf-test python3 -m http.server 8080 &"
        echo "   docker exec aya-ebpf-test curl localhost:8080"
        echo ""
        echo "3. Monitor network stats:"
        echo "   watch -n 1 'docker exec aya-ebpf-test cat /host/proc/net/dev'"
        echo ""
        read -p "Start containers now? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker compose up
        fi
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac
