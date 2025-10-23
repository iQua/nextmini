#!/bin/bash
# Complete script to run Prime-RL reverse text example on nextmini

set -e

echo "=========================================="
echo "Prime-RL Reverse Text on Nextmini"
echo "=========================================="

# Step 1: Start nextmini network
echo -e "\nStep 1: Starting nextmini network with GPU support..."
docker compose -f docker-compose.gpu.yml up -d

echo "Waiting for network to be ready..."
sleep 15

# Step 2: Verify network connectivity
echo -e "\nStep 2: Verifying network connectivity..."
docker exec node3 bash -lc '
  for i in {1..15}; do
    if command -v ip &>/dev/null; then
      if ip addr show utun &>/dev/null; then echo "[OK] TUN interface exists"; exit 0; fi
    elif command -v ifconfig &>/dev/null; then
      if ifconfig utun &>/dev/null; then echo "[OK] TUN interface exists"; exit 0; fi
    else
      if grep -Eq "^ *utun:" /proc/net/dev; then echo "[OK] TUN interface exists"; exit 0; fi
    fi
    echo "Attempt $i: Waiting for utun..."; sleep 2
  done
  echo "[FAIL] TUN interface not found"; exit 1
'

# Step 3: Install prime-rl dependencies in containers
echo -e "\nStep 3: Installing prime-rl dependencies..."

for node in node1 node2 node3 node4; do
  echo "Installing in $node..."
  docker exec $node bash -c '
    cd /var/prime-rl &&
    export PATH="/root/.local/bin:${PATH}" &&
    if ! command -v uv &>/dev/null; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
    fi &&
    uv sync --all-extras || echo "Note: Some dependencies may not install without GPU, continuing..."
  ' || echo "Warning: Installation in $node had issues, continuing..."
done

echo -e "\nStep 4: Checking GPU availability..."
docker exec node4 nvidia-smi || echo "Warning: GPU not detected in node4"
docker exec node1 nvidia-smi || echo "Warning: GPU not detected in node1"
docker exec node2 nvidia-smi || echo "Warning: GPU not detected in node2"

# Step 5: Start inference server
echo -e "\nStep 5: Waiting for Node 4 utun to be ready..."
docker exec node4 bash -lc '
  for i in {1..15}; do
    if command -v ip &>/dev/null; then
      if ip addr show utun &>/dev/null; then echo "[OK] utun ready on node4"; exit 0; fi
    elif command -v ifconfig &>/dev/null; then
      if ifconfig utun &>/dev/null; then echo "[OK] utun ready on node4"; exit 0; fi
    else
      if grep -Eq "^ *utun:" /proc/net/dev; then echo "[OK] utun ready on node4"; exit 0; fi
    fi
    echo "Attempt $i: Waiting for node4 utun..."; sleep 2
  done
  echo "[WARN] utun not ready on node4"; exit 1
' || echo "Warning: node4 utun may not be ready"

# Ensure utun is ready on node1/node2 before distributed training
echo -e "\nWaiting for Node 1 utun to be ready..."
docker exec node1 bash -lc '
  for i in {1..15}; do
    if command -v ip &>/dev/null; then
      if ip addr show utun &>/dev/null; then echo "[OK] utun ready on node1"; exit 0; fi
    elif command -v ifconfig &>/dev/null; then
      if ifconfig utun &>/dev/null; then echo "[OK] utun ready on node1"; exit 0; fi
    else
      if grep -Eq "^ *utun:" /proc/net/dev; then echo "[OK] utun ready on node1"; exit 0; fi
    fi
    echo "Attempt $i: Waiting for node1 utun..."; sleep 2
  done
  echo "[WARN] utun not ready on node1"; exit 1
' || echo "Warning: node1 utun may not be ready"

echo -e "\nWaiting for Node 2 utun to be ready..."
docker exec node2 bash -lc '
  for i in {1..15}; do
    if command -v ip &>/dev/null; then
      if ip addr show utun &>/dev/null; then echo "[OK] utun ready on node2"; exit 0; fi
    elif command -v ifconfig &>/dev/null; then
      if ifconfig utun &>/dev/null; then echo "[OK] utun ready on node2"; exit 0; fi
    else
      if grep -Eq "^ *utun:" /proc/net/dev; then echo "[OK] utun ready on node2"; exit 0; fi
    fi
    echo "Attempt $i: Waiting for node2 utun..."; sleep 2
  done
  echo "[WARN] utun not ready on node2"; exit 1
' || echo "Warning: node2 utun may not be ready"

echo -e "\nStarting inference server on Node 4 (GPU 0)..."
docker exec -d node4 bash -c '
  export PATH="/root/.local/bin:${PATH}"
  export CUDA_VISIBLE_DEVICES=0  # Container sees only 1 GPU as index 0
  cd /var/prime-rl
  uv run inference \
    @ examples/reverse_text/rl/infer.toml \
    --host 10.0.0.4 --port 8000 \
    --model.name Winifredkkkk/Qwen3-0.6B-Reverse-Text-RL \
    > /var/nextmini/inference.log 2>&1
'

echo "Waiting for inference server to start..."
sleep 10

# Step 6: Test inference server
echo -e "\nStep 6: Testing inference server (may take up to 2 minutes for first startup)..."
docker exec node3 bash -c '
  for i in {1..60}; do
    if curl -s --connect-timeout 2 http://10.0.0.4:8000/health &>/dev/null || \
       curl -s --connect-timeout 2 http://10.0.0.4:8000/v1/models &>/dev/null; then
      echo "[OK] Inference server is responding"
      exit 0
    fi
    echo "Attempt $i/60: Waiting for inference server..."
    sleep 2
  done
  echo "[WARN] Inference server may not be ready"
' || echo "Warning: Could not verify inference server"

# Step 7: Start orchestrator
echo -e "\nStep 7: Starting orchestrator on Node 3... (2 train workers)"
docker exec -d node3 bash -c '
  export PATH="/root/.local/bin:${PATH}"
  cd /var/prime-rl
  uv run orchestrator \
    @ examples/reverse_text/rl/orch.toml \
    --client.base-url http://10.0.0.4:8000/v1 \
    --num-train-workers 2 \
    > /var/nextmini/orchestrator.log 2>&1
'

sleep 5

# Step 8: Start distributed trainer (2 nodes, 1 GPU each)
echo -e "\nStep 8: Starting trainer on Node 1 (GPU 1 + GPU 2)..."
echo "  Single-node dual-GPU training with NCCL"
echo ""

# Start trainer on node1 with 2 GPUs
echo "Starting trainer (node1, GPU 1+2)..."
echo "Press Ctrl+C to stop. Monitor flows: docker exec postgres psql -U pgusr -d nextmini -c \"SELECT * FROM app_flows WHERE is_finished=FALSE;\""
echo ""
docker exec node1 bash -c '
  export PATH="/root/.local/bin:${PATH}"
  export CUDA_VISIBLE_DEVICES=1,2  # Use host GPU 1 and 2
  export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
  export OMP_NUM_THREADS=1
  export NCCL_DEBUG=INFO
  export NCCL_IB_DISABLE=1
  export NCCL_SHM_DISABLE=1
  export NCCL_P2P_DISABLE=1
  export NCCL_NET_GDR_LEVEL=0
  export NCCL_COLLNET_ENABLE=0
  cd /var/prime-rl
  uv run torchrun \
    --nproc-per-node=2 \
    --standalone \
    -m prime_rl.trainer.rl.train \
    @ examples/reverse_text/rl/train.toml \
    --model.name Winifredkkkk/Qwen3-0.6B-Reverse-Text-RL
'

echo -e "\n=========================================="
echo "Training completed!"
echo "=========================================="

# Graceful shutdown to close TCP connections properly
echo -e "\nCleaning up: stopping orchestrator and inference..."
docker exec node3 pkill -TERM -f 'uv run orchestrator' || true
sleep 3
docker exec node4 pkill -TERM -f 'uv run inference' || true
sleep 2

echo -e "\nFinal flow statistics:"
docker exec postgres psql -U pgusr -d nextmini -c "
  SELECT
    src_node_id,
    dst_node_id,
    COUNT(*) as total,
    SUM(CASE WHEN is_finished THEN 0 ELSE 1 END) as active
  FROM app_flows
  GROUP BY src_node_id, dst_node_id;
"

echo -e "\nView logs:"
echo "  Inference: docker exec node4 cat /var/nextmini/inference.log"
echo "  Orchestrator: docker exec node3 cat /var/nextmini/orchestrator.log"
echo "  Trainer rank1: docker exec node2 cat /var/nextmini/trainer_rank1.log"
echo "=========================================="
