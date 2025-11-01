#!/bin/bash
# Run ring all-reduce on physical network (bypassing Nextmini)
# Usage: ./run_physical.sh (run from bare-metal directory)

set -e

cd "$(dirname "$0")"

echo "Step 1: Cleaning up old processes..."
uv run cleanup_ring.py

echo ""
echo "Step 2: Running ring all-reduce on physical network..."

# Default test (~400KB: 104857 elements * 4 bytes)
# ./launch_physical.py \
#   --hosts 157.180.84.40 174.138.23.4 134.209.191.238 134.199.159.159 \
#   --ssh-hosts root@157.180.84.40 root@174.138.23.4 root@134.209.191.238 root@134.199.159.159 \
#   --len 104857 \
#   --reps 10 \
#   --verify

# 500MB test (131072000 elements * 4 bytes = 500MB)
./launch_physical.py \
  --hosts 157.180.84.40 174.138.23.4 134.209.191.238 134.199.159.159 \
  --ssh-hosts root@157.180.84.40 root@174.138.23.4 root@134.209.191.238 root@134.199.159.159 \
  --len 131072000 \
  --reps 2 \
  --verify

# 1GB test (262144000 elements * 4 bytes = 1GB)
# ./launch_physical.py \
#   --hosts 157.180.84.40 174.138.23.4 134.209.191.238 134.199.159.159 \
#   --ssh-hosts root@157.180.84.40 root@174.138.23.4 root@134.209.191.238 root@134.199.159.159 \
#   --len 262144000 \
#   --reps 10 \
#   --verify

# 3GB test (786432000 elements * 4 bytes = 3GB)
# ./launch_physical.py \
#   --hosts 157.180.84.40 174.138.23.4 134.209.191.238 134.199.159.159 \
#   --ssh-hosts root@157.180.84.40 root@174.138.23.4 root@134.209.191.238 root@134.199.159.159 \
#   --len 786432000 \
#   --reps 10 \
#   --verify

echo ""
echo "Physical network test complete!"
