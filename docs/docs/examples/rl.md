# RL Training on GSM8K (Python API example)

This example runs a trainer/worker workload that uses `nextmini_py` to exchange messages and to broadcast large payloads losslessly.

See also: [Python dataplane API](../design/python-api.md).

## Run (Docker)

```bash
cd examples/rl
docker compose up --build
```

Follow logs:

```bash
docker compose logs -f trainer worker_0 worker_1
```

Stop:

```bash
docker compose down
```

## What it exercises

- Control messages: `send_to_node` + `register_receiver_from_node`
- Lossless multicast: `send_data` / `receive_data` + `lossless_wait`
- Optional multicast route planning via the LP solver (`examples/lp`)

## Evaluation (no Nextmini required)

```bash
cd examples/rl
uv venv --python 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
uv run evaluate_comparison.py
```

