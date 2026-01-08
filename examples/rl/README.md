# RL Training on GSM8K with Nextmini

## Run with Docker

```bash
uv venv
source .venv/bin/activate
cd examples/rl
uv pip install -r requirements.txt
docker compose up
```

View logs:

```bash
docker compose logs -f trainer worker_0 worker_1
```

Stop:

```bash
docker compose down
```

## Evaluation

If you are running evaluation on a separate machine (e.g. a Linux GPU server), follow these steps to set up the environment:

1. **Create and activate environment**:

   ```bash
   cd examples/rl
   uv venv --python 3.13
   source .venv/bin/activate
   ```

2. **Install dependencies**:

   ```bash
   # Install requirements (nextmini_py is NOT required for evaluation)
   uv pip install -r requirements.txt
   ```

3. **Run evaluation**:

   ```bash
   uv run evaluate_comparison.py
   ```

## Results with Qwen2.5-0.5B-Instruct—500 Epochs

- **Baseline (Qwen/Qwen2.5-0.5B-Instruct)**: 30.20% (151/500)

- **Trained model (qwen-gsm8k-rl)**: 41.80% (209/500)

## Multicast Planning (LP / CF-Tree)

The trainer computes multicast group routes before broadcasting weights. You can tune the planner via env vars:

- `MULTICAST_TREE_ALGO`: `cf_tree` (default), `cf_bottleneck`, `mflow`, `basic_tree`, `basic_bottleneck`, `star`, `two_level`, `cf_tree_mwu`, or `cf_bottleneck_mwu`
- `MULTICAST_HOP_LIMIT`: hop limit `H` (default `3`)
- `MULTICAST_ETA`: LP-guidance weight `eta` for `cf_tree` (default `0.1`)
- `MULTICAST_MAX_RELAYS`: optional cap on relay nodes selected by LP-importance (unset = no cap)
- `MULTICAST_RELAY_SCORING`: `coverage` (default), `path_flow`, or `incident`
- `MULTICAST_ALLOW_WORKER_RELAYS`: if `true`, allow destination workers to forward (workers can appear as internal nodes; not counted against `MULTICAST_MAX_RELAYS`)
- `MULTICAST_NUM_PATHS`: candidate paths per destination for the LP (default `2`)
- `MULTICAST_PROBE_LINKS`: if `true`, insert DB probe flows and overwrite link capacities before planning (default `false`)
- `MULTICAST_PROBE_BYTES`: bytes per probe flow (default `67108864`)
- `MULTICAST_PROBE_TIMEOUT_SECS`: probe completion timeout (default `60`)

For more than two workers, set `WORKER_NODE_IDS` to a comma-separated list (rank order), e.g.:

```bash
export WORKER_NODE_IDS="2,3,4,5,6,7"
```

## WAN Broadcast Microbenchmark (10GB artifacts)

For networking-focused evaluation (independent of RL compute), use `examples/rl/src/broadcast_bench.py` to broadcast a file-backed artifact and measure completion time/goodput.
The benchmark uses `send_file` / `receive_to_file` to avoid reading multi-GB payloads into memory.

For a multi-datacenter deployment helper (one-command SSH orchestration), see `examples/rl/multidc/README.md`.

### What “star” and “two_level” mean

- `star`: the trainer sends directly to every worker (depth 1, no relays).
- `two_level`: the trainer sends to a small set of relays (depth 2), and each worker receives either directly from the trainer or via the relay that maximizes `min(C(src, relay), C(relay, worker))`.

### Running the microbenchmark

The benchmark is a **broadcast-only** path (no ML compute) and can send arbitrarily large artifacts even if the model is small.
Use `--generate-bytes` to create a (sparse) on-disk file of the desired size and then stream it over the multicast tree.

**Trainer (source)**

```bash
# from repo root (with nextmini_py installed)
export WORKER_NODE_IDS="2,3,4,5,6,7"
python -m examples.rl.src.broadcast_bench \
  --role trainer \
  --config <trainer-config.toml> \
  --controller-config <controller-config.toml> \
  --algorithm cf_bottleneck \
  --rounds 20 \
  --file artifacts/broadcast.bin \
  --generate-bytes $((10 * 1024 * 1024 * 1024)) \
  --output-json artifacts/results.json
```

**Worker (receiver)**

```bash
# one process per worker node
export TRAINER_NODE_ID="1"
export RANK="0"  # worker rank index
python -m examples.rl.src.broadcast_bench \
  --role worker \
  --config <worker-config.toml> \
  --rounds 20
```

**Docker helper**

In `examples/rl` Docker environments, you can use:

```bash
bash examples/rl/scripts/run_broadcast_bench.sh trainer configs/trainer-config.toml --rounds 20
bash examples/rl/scripts/run_broadcast_bench.sh worker configs/worker0-config.toml --rank 0 --rounds 20
```

### Single-host (generated compose)

For a one-command single-host setup with any number of workers/relays (and optional probing), use:

```bash
cd nextmini
python -m examples.rl.scripts.single_host run \
  --mode broadcast \
  --workers 6 \
  --relays 4 \
  --bytes $((10 * 1024 * 1024 * 1024)) \
  --algorithm cf_bottleneck
```

You can spin up a larger pool of relay containers and let the planner select a subset via a relay budget:

```bash
cd nextmini
python -m examples.rl.scripts.single_host run \
  --mode broadcast \
  --workers 2 \
  --relays 5 \
  --max-relays 2 \
  --relay-scoring coverage \
  --trainer-gpu 0 \
  --worker-gpus 1,2
```

To run the end-to-end RL loop (training + weight multicast), switch to `--mode rl`:

```bash
cd nextmini
python -m examples.rl.scripts.single_host run \
  --mode rl \
  --workers 2 \
  --relays 5 \
  --max-relays 2 \
  --relay-scoring coverage \
  --trainer-gpu 0 \
  --worker-gpus 1,2
```
