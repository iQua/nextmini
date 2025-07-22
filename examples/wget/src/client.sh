#!/bin/sh
# Wait for network and routes to be ready
sleep 20

SOCKS_PROXY=172.16.8.5:8081
TARGET=http://172.16.8.8:8080/file.txt

export all_proxy="socks5h://$SOCKS_PROXY"

echo "Starting wget client via SOCKS5 proxy $SOCKS_PROXY -> $TARGET"

mkdir -p /downloads

while true; do
  ts=$(date +%H:%M:%S)
  echo "[$ts] Downloading $TARGET to /downloads/file.txt"
  if wget -q "$TARGET" -O /downloads/file.txt; then
    echo "[$ts] Download completed. File content: $(cat /downloads/file.txt)"
  else
    echo "[$ts] Download failed"
  fi
  sleep 3
done
