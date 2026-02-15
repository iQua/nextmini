---
title: "Large Node Count on One Host"
description: "Generates and runs a large Docker Compose topology (default 100 dataplane nodes) for scale testing on a single machine."
---

`examples/multi-nodes` is a single-host scale test. It does not use Docker Swarm manager/worker roles. Instead, `generate_compose.py` rewrites `docker-compose.yml` with a long chain of dataplane services (`node1` through `nodeN`).

The default generator value is `NUM_NODES = 100`.

## Prerequisites

- One Linux machine with enough CPU, RAM, and file descriptors for many containers.
- Docker and Docker Compose.
- Python 3 for `generate_compose.py`.

## Deployment order

### 1. Set desired node count and generate compose file

From the example directory:

```bash
cd /path/to/nextmini/examples/multi-nodes
rg '^NUM_NODES' generate_compose.py
```

If needed, edit `NUM_NODES` in `generate_compose.py`, then regenerate `docker-compose.yml`:

```bash
python generate_compose.py
```

Verify how many node services were generated:

```bash
rg -c '^    node[0-9]+:' docker-compose.yml
```

### 2. Build and launch

```bash
docker compose build
docker compose up -d
```

The generated file starts `postgres`, then `controller`, then dataplane nodes in sequence (`node2` depends on `node1`, `node3` depends on `node2`, and so on).

## Verify

Check service status:

```bash
docker compose ps
```

Check controller and a dataplane node log:

```bash
docker compose logs --tail 100 controller
docker compose logs --tail 50 node1
```

For high node counts, also inspect a late node (example shown for 100):

```bash
docker compose logs --tail 50 node100
```

Optional: inspect one container directly:

```bash
docker exec -it node1 ifconfig
```

## Tear down

```bash
docker compose down
```

If you want to fully clear unused artifacts after large runs:

```bash
docker system prune -a
```
