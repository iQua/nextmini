#!/bin/bash

# Run ring all-reduce test with MPI
mpirun --allow-run-as-root \
  -np 2 \
  -H 10.0.0.1:1,10.0.0.2:1 \
  -x MASTER_ADDR=node1 \
  -x PATH \
  -bind-to none \
  -map-by :OVERSUBSCRIBE \
  uv run ring_allreduce.py --size 10000000 --verify

