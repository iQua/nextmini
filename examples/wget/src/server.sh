#!/bin/sh
# Wait for network readiness
sleep 15

# create small file to download
echo "Hello from Nextmini!" > /app/file.txt

echo "Launching simple HTTP server on port 8080 with file.txt available..."
python -m http.server 8080 --bind 0.0.0.0
