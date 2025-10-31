# Ring AllReduce on Fly.io over Nextmini

Optimized launcher for running ring-allreduce tests across multiple Fly.io nodes.

## Quick Start

### 1. Start Local Controller

```bash
cd /home/xindan/nextmini/examples/localserver-flyio
uv run start-controller.py

# View logs
docker compose logs -f controller

# View dashboard
cd ~/nextmini/tools/monitor 
uv run dashboard.py
```

### 2. Deploy Nodes to Fly.io

```bash
# Deploy 10 nodes
uv run deploy-flyio.py --public-ip YOUR_PUBLIC_IP --nodes 10
```

### 3. Run Ring AllReduce Test

```bash
cd ring-emu

# Recommended: Auto-cleanup + run (avoids port conflicts)
uv run run_with_cleanup.py \
  --apps nextmini-node-1 nextmini-node-2 nextmini-node-3 \
         nextmini-node-4 nextmini-node-5 nextmini-node-6 \
         nextmini-node-7 nextmini-node-8 nextmini-node-9 \
         nextmini-node-10 \
  --len 1024 --reps 3 --verify
```

## Configuration

### Change Port (if conflicts occur)

```bash
# Update ring.txt
sed -i 's/:9955/:9966/g' ring.txt

# Update remote nodes
for app in nextmini-node-{1..10}; do 
  flyctl ssh console -a $app -C "sed -i 's/:9955/:9966/g' /tmp/ring-test/ring.txt" &
done
```

## Cleanup

```bash
cd /home/xindan/nextmini/examples/localserver-flyio

# Destroy all nodes
for i in {1..10}; do
  flyctl apps destroy nextmini-node-$i --yes
done

# Stop local controller
docker compose down
```


--- 

# Baseline Ring All-Reduce

Baseline ring all-reduce performance testing on Fly.io native network.

## Quick Start

```bash
cd /home/xindan/nextmini/examples/localserver-flyio/ring-emu

# 1. Generate ring.txt (N nodes)
uv run generate_ring.py --num-nodes 10

# 2. Deploy nodes
uv run deploy_baseline.py --nodes 10 --region iad

# 3. Wait for deployment
sleep 30

# 4. Run test (auto-cleanup + parallel execution)
uv run run_baseline.py --num-nodes 10 --len 1048576 --reps 20 --verify
```

### Cleanup
```bash
# Destroy all nodes
uv run destroy_all.py --num-nodes 10
```