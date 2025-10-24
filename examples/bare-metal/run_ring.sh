#!/bin/bash
# Run ring all-reduce with automatic cleanup
# Usage: ./run_ring.sh

set -e

cd "$(dirname "$0")"

echo "Step 1: Cleaning up old processes..."
uv run cleanup_ring.py

echo ""
echo "Step 2: Running ring all-reduce..."
cd ring-emu

uv run launch_ring.py \
  --ring ../ring.txt \
  --bin ~/nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes \
  --ssh-hosts ../ssh_hosts.txt \
  --ssh-key ../ssh/id_rsa \
  --remote-dir ~/ring-test \
  --len 104857 \
  --init rank \
  --reps 10 \
  --verify

