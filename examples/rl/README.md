# RL Training on GSM8K (Nextmini)

## Run (Docker)

```bash
cd examples/rl
docker compose up --build
```

Logs:

```bash
docker compose logs -f trainer worker_0 worker_1
```

Stop:

```bash
docker compose down
```

In Docker, the trainer probes link capacities via the controller DB before planning routes.

## Evaluation (no Nextmini required)

```bash
cd examples/rl
uv venv --python 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
uv run evaluate_comparison.py
```
