---
title: "Docker Swarm Quickstart (Simple)"
description: "Deploys controller, Postgres, and three dataplane replicas with Docker Swarm."
---

This example runs a small Nextmini swarm deployment using `examples/simple-swarm/docker-compose.swarm.yml`. The manager hosts `postgres` and `controller`, and dataplane replicas can run on manager or workers.

## Prerequisites

Prepare at least one Linux host for the swarm manager and one or more worker hosts. On every host:

- Install Docker with permission to run `docker` commands.
- Clone this repository in the same path so image builds use the same source tree.

## Deployment order

### 1. Build images on manager and workers

`docker stack deploy` will not build images for you. Build the images first on each swarm node that may run services.

```bash
cd /path/to/nextmini/examples/simple-swarm
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
```

### 2. Initialize swarm on the manager

```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

Copy the join command printed by Docker, then run it on each worker:

```bash
docker swarm join --token <WORKER_TOKEN> <MANAGER_IP>:2377
```

Confirm node membership on the manager:

```bash
docker node ls
```

### 3. Deploy the stack from the manager

```bash
cd /path/to/nextmini/examples/simple-swarm
docker stack deploy -c docker-compose.swarm.yml nextmini
```

The stack file sets this startup shape:

- `postgres` (replicas: 1) pinned to manager.
- `controller` (replicas: 1) pinned to manager.
- `dataplane` (replicas: 3) with hostname `node{{.Task.Slot}}`, and each task writes `node_id` into `/var/nextmini/config.toml` before launching.

## Verify

Check that all services are running:

```bash
docker stack services nextmini
docker service ps nextmini_postgres
docker service ps nextmini_controller
docker service ps nextmini_dataplane
```

Check dataplane logs for successful controller connection:

```bash
docker service logs --tail 100 nextmini_dataplane
```

Optional: open one dataplane container and inspect interfaces:

```bash
docker ps --filter name=nextmini_dataplane
# pick one container ID from the output
docker exec -it <CONTAINER_ID> ifconfig
```

## Tear down

Remove the deployed stack from the manager:

```bash
docker stack rm nextmini
```

If you are done with this swarm cluster entirely, leave swarm mode on workers and manager:

```bash
docker swarm leave -f
```
