#!/usr/bin/env bash
# Reproduce the Stage 3 reservoir experiment, an extension BEYOND the METTLE paper.
set -euo pipefail

export CARGO_INCREMENTAL=0
export PYO3_PYTHON=/opt/homebrew/bin/python3.13

cargo build --release -p mettle --examples
mkdir -p results/stage3

{
  target/release/examples/reservoir_storage_spike retain 65536 1400 2 100 5 100
  target/release/examples/reservoir_storage_spike recompute 65536 1400 2 100 5 100 | sed '1d'
  target/release/examples/reservoir_storage_spike spill 65536 1400 2 100 5 100 | sed '1d'
  target/release/examples/reservoir_storage_spike retain 65536 1400 4 100 7 100 | sed '1d'
} > results/stage3/reservoir-storage.csv

target/release/examples/reservoir_simulation sweep 512 8192 1400 \
  > results/stage3/reservoir-sweep.csv
target/release/examples/reservoir_simulation case confirmation-best 4096 8192 1400 4 100 7 100 \
  | sed '1d' >> results/stage3/reservoir-sweep.csv
