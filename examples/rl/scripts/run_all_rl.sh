#!/bin/bash
set -e
INV=examples/rl/multidc/inventory.toml
RL_OUT=examples/rl/multidc/archives/r2_dc_results_rl/rl
SNAP=examples/rl/multidc/archives/r2_dc_results_rl/capacity_snapshots/snap_t1.json

mkdir -p "$RL_OUT"

echo "=== Running Main Algo Comparison ==="
for algo in star two_level basic_bottleneck cf_bottleneck_mwu; do
  echo "Running $algo..."
  python examples/rl/scripts/run_wan_rl_matrix.py \
    --inventory "$INV" \
    --batch-ssh \
    --out-dir "$RL_OUT" \
    --label "t1_${algo}" \
    --args "--capacity-snapshot $SNAP \
            --multicast-tree-algo $algo --hop-limit 3 \
            --allow-worker-relays \
            --model-name sshleifer/tiny-gpt2 \
            --train-steps 6 --batch-size 0 --grpo-group-size 1 \
            --generation-len 8 --max-seq-len 128 \
            --no-use-sharded-weights \
            --multicast-timeout-ms 600000 \
            --cpu-only \
            --no-sync"
done

echo "=== Running Ablation: No Worker Relays ==="
python examples/rl/scripts/run_wan_rl_matrix.py \
  --inventory "$INV" --batch-ssh --out-dir "$RL_OUT" \
  --label "t1_cf_bottleneck_mwu_fwdno" \
  --args "--capacity-snapshot $SNAP \
          --multicast-tree-algo cf_bottleneck_mwu --hop-limit 3 \
          --no-allow-worker-relays \
          --model-name sshleifer/tiny-gpt2 \
          --train-steps 6 --batch-size 0 --grpo-group-size 1 \
          --generation-len 8 --max-seq-len 128 \
          --no-use-sharded-weights --cpu-only --no-sync"

echo "=== Running Ablation: Relay Budgets ==="
for k in 0 1 2 3; do
  echo "Running Budget $k..."
  python examples/rl/scripts/run_wan_rl_matrix.py \
    --inventory "$INV" --batch-ssh --out-dir "$RL_OUT" \
    --label "t1_cf_bottleneck_mwu_relays${k}" \
    --args "--capacity-snapshot $SNAP \
            --multicast-tree-algo cf_bottleneck_mwu --hop-limit 3 \
            --max-relays $k --allow-worker-relays \
            --model-name sshleifer/tiny-gpt2 \
            --train-steps 6 --batch-size 0 --grpo-group-size 1 \
            --generation-len 8 --max-seq-len 128 \
            --no-use-sharded-weights --cpu-only --no-sync"
done
