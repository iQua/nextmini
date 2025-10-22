# Waterfilling Example(Not worked yet.)

## Run

```bash
cd /home/ubuntu/nextmini/examples/routing/waterfilling

# 1. Start (if not running)
docker compose up -d
sleep 15

# 2. Start traffic (if not started)
./start_traffic.sh
sleep 5

# 3. Run algorithm
uv sync
uv run run_waterfilling.py
```

## Clean

```bash
./clean.sh
```
