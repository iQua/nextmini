# Prime-RL on Nextmini - Quick Start

## Build

```bash
cd /home/xindan/nextmini/examples/prime-rl
docker compose -f docker-compose.gpu.yml build
```

## Start

```bash
docker compose -f docker-compose.gpu.yml up -d
docker compose -f docker-compose.gpu.yml ps
```

## Test Network

```bash
docker exec node3 bash /var/nextmini/scripts/test.sh
```

## Run Training

```bash
bash run_reverse_text.sh
```

## Monitor Flows

```bash
cd scripts
uv sync
uv run monitor_flows.py --continuous
```

## View Logs

```bash
docker logs node4 -f                          # Inference
docker logs node3 -f                          # Orchestrator
docker exec node1 cat /var/nextmini/trainer.log   # Trainer
```

## Check GPU

```bash
nvidia-smi
docker exec node1 nvidia-smi
docker exec node4 nvidia-smi
```

## Stop

```bash
docker compose -f docker-compose.gpu.yml down
```

## Clean

```bash
docker compose -f docker-compose.gpu.yml down -v
```
