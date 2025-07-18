# NextMini Docker Swarm Deployment

This example demonstrates how to deploy NextMini across multiple nodes using Docker Swarm.

## Architecture

- **Manager Node**: Runs PostgreSQL database and Controller
- **Worker Nodes**: Run NextMini dataplane nodes (node1, node2, node3)

## Prerequisites

1. Docker Swarm cluster initialized
2. At least one manager node and worker nodes available

## Setup

### 1. Initialize Docker Swarm (if not already done)

On manager node:
```bash
docker swarm init
```

On worker nodes:
```bash
docker swarm join --token <worker-token> <manager-ip>:2377
```

### 2. Deploy the stack

```bash
./deploy.sh
```

Or manually:
```bash
docker stack deploy -c docker-compose.swarm.yml nextmini
```

## Node ID Assignment

Dataplane nodes automatically get assigned IDs 1, 2, 3 based on their task slot:
- `node1` (hostname) → `node_id = 1`
- `node2` (hostname) → `node_id = 2` 
- `node3` (hostname) → `node_id = 3`

## Monitoring

Check service status:
```bash
docker stack services nextmini
```

View logs:
```bash
docker service logs nextmini_controller
docker service logs nextmini_dataplane
docker service logs nextmini_postgres
```

## Scaling

To change number of dataplane nodes:
```bash
docker service update --replicas 5 nextmini_dataplane
```

Update `controller-config.toml` to match new node count.

## Cleanup

```bash
docker stack rm nextmini
```