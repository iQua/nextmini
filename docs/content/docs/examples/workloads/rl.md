---
title: "RL Training with Multicast Weights"
description: "Trains a GSM8K policy with a trainer/worker topology where model weights are synchronized through Nextmini multicast."
---

The `examples/rl` workload runs one trainer and two workers on top of Nextmini. The trainer owns the multicast group, computes routes (with LP helpers from `examples/lp`), and sends weight updates. Workers join the group, receive those updates losslessly, run rollouts, and return results to the trainer.

The compose stack wires Postgres, controller, trainer, and workers together. Each RL container runs `scripts/run_rl_node.sh`, which creates an isolated Python environment, installs dependencies, and builds or installs `nextmini_py`.

## Run

From the repository root:

```bash
cd examples/rl
docker compose up --build
```

To watch only training services:

```bash
cd examples/rl
docker compose logs -f trainer worker_0 worker_1
```

## Verification

Check trainer logs for these milestones:

- topology and group readiness
- LP multicast route installation
- repeated training step output
- `Model saved to output/qwen-gsm8k-rl`

Check worker logs for handshake and multicast receive completion.

Then confirm the saved model exists on the host-mounted output directory:

```bash
cd examples/rl
ls -lah output/qwen-gsm8k-rl
```

You can run evaluation without the Nextmini runtime once a model is saved:

```bash
cd examples/rl
uv venv --python 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
uv run evaluate_trained_only.py --model_path output/qwen-gsm8k-rl --samples 50
```

## Cleanup

```bash
cd examples/rl
docker compose down --volumes
```

If you want a clean rerun without previous artifacts:

```bash
cd examples/rl
rm -rf output
```
