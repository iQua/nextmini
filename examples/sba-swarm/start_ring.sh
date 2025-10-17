#!/bin/bash
set -e

R0_HOST=10.0.0.1           # node1
R1_HOST=10.0.0.2           # node2
NUMEL=65536
ITERS=10
CHUNKS=2
BIN=/var/nextmini/ring-emu
LOG_DIR=/var/nextmini
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes"

# cleanup any previous runs
ssh $SSH_OPTS $R1_HOST "pkill -9 ring-emu || true"
pkill -9 ring-emu || true

# start rank1 in the background on node2
ssh $SSH_OPTS $R1_HOST \
  "sh -c 'exec $BIN --rank 1 --world-size 2 --listen 0.0.0.0:9002 --next $R0_HOST:9001 \
    --numel $NUMEL --chunks $CHUNKS --iters $ITERS --verbose \
    > $LOG_DIR/ring-emu-rank1.log 2>&1 &'"

# wait a moment for port 9002 to listen
sleep 1

# start rank0 in the foreground on node1
exec $BIN --rank 0 --world-size 2 --listen 0.0.0.0:9001 --next $R1_HOST:9002 \
     --numel $NUMEL --chunks $CHUNKS --iters $ITERS --verbose
