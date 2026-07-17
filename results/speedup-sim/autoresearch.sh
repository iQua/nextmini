#!/bin/sh
set -eu

# Run from the repository root. This produces model-level evidence, not WAN measurements.
export CARGO_INCREMENTAL=0
export PYO3_PYTHON=/opt/homebrew/bin/python3.13

seeds=${1:-512}
output_dir=${2:-results/speedup-sim}

cargo run --release -p mettle --example pooling_speedup_sim -- \
    sweep "$seeds" "$output_dir"
