#!/bin/bash

echo "Starting client..."

while true; do
    echo "Sending request through SOCKS5 proxy..."
    
    response=$(curl --socks5 node1:8081 http://server:8080/)
    
    if [ $? -eq 0 ]; then
        echo "Response: $response"
        break
    else
        echo "Request failed"
    fi
    
    echo "Waiting 2 seconds..."
    sleep 2
done