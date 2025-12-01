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
