# RL Training with Nextmini

## Run with Docker

```bash
cd examples/rl
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

## Run Locally

**Note**: Local setup uses different ports (8080, 8081, 8082) to avoid conflicts. Configs are in `configs/` directory.

### Setup

```bash
cd examples/rl

# Create Python environment
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Build and install nextmini_py
cd ../../python-api
maturin build --release -F python-extension
pip install ../target/wheels/nextmini_py-*.whl
cd ../examples/rl
```

### Start Controller

Terminal 1:

```bash
cd controller
./start-database.sh
cargo run --release
```

### Run Training

Terminal 2 (Trainer):

```bash
cd examples/rl
source .venv/bin/activate
export TRAINER_CONFIG=$PWD/configs/trainer-config.toml
python src/trainer.py
```

Terminal 3 (Worker 0):

```bash
cd examples/rl
source .venv/bin/activate
export WORKER0_CONFIG=$PWD/configs/worker0-config.toml
python src/worker.py --rank 0 --gpu 0
```

Terminal 4 (Worker 1):

```bash
cd examples/rl
source .venv/bin/activate
export WORKER1_CONFIG=$PWD/configs/worker1-config.toml
python src/worker.py --rank 1 --gpu 0
```

## Evaluation

If you are running evaluation on a separate machine (e.g. a Linux GPU server), follow these steps to set up the environment:

1. **Create and activate environment**:

   ```bash
   cd examples/rl
   uv venv
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
