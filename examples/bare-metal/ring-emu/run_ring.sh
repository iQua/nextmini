#!/bin/bash
# Run ring all-reduce with automatic cleanup
# Usage: ./run_ring.sh (run from ring-emu directory)

set -e

cd "$(dirname "$0")"

echo "Step 1: Cleaning up old processes..."
uv run cleanup_ring.py

echo ""
echo "Step 2: Running ring all-reduce..."

# Default test (100KB * 4 bytes = 400KB)
# uv run launch_ring.py \
#   --ring ring.txt \
#   --bin ~/nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes \
#   --ssh-hosts ssh_hosts.txt \
#   --ssh-key ../dataplane/ssh/id_rsa \
#   --remote-dir ~/ring-test \
#   --len 104857 \
#   --init rank \
#   --reps 10 \
#   --verify

# 500MB test (131072000 elements * 4 bytes = 500MB)
uv run launch_ring.py \
  --ring ring.txt \
  --bin ~/nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes \
  --ssh-hosts ssh_hosts.txt \
  --ssh-key ../dataplane/ssh/id_rsa \
  --remote-dir ~/ring-test \
  --len 131072000 \
  --init rank \
  --reps 10 \
  --verify

# 1GB test (262144000 elements * 4 bytes = 1GB)
# uv run launch_ring.py \
#   --ring ring.txt \
#   --bin ~/nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes \
#   --ssh-hosts ssh_hosts.txt \
#   --ssh-key ../dataplane/ssh/id_rsa \
#   --remote-dir ~/ring-test \
#   --len 262144000 \
#   --init rank \
#   --reps 10 \
#   --verify

# 3GB test (786432000 elements * 4 bytes = 3GB)
# uv run launch_ring.py \
#   --ring ring.txt \
#   --bin ~/nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes \
#   --ssh-hosts ssh_hosts.txt \
#   --ssh-key ../dataplane/ssh/id_rsa \
#   --remote-dir ~/ring-test \
#   --len 786432000 \
#   --init rank \
#   --reps 10 \
#   --verify

