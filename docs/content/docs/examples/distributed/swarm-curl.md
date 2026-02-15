---
title: "Swarm curl Client/Server Over Nextmini"
description: "Deploys external curl client and server services into the swarm network, inserts explicit routes, and validates end-to-end HTTP traffic."
---

This example adds external client/server services on top of a running swarm dataplane deployment (typically from `examples/multi-dc`).

For base swarm dataplane deployment, use [Multi-DC Swarm Dataplane](/docs/examples/distributed/multi-dc).

The flow is:

1. Deploy `curl-server` and `curl-client` services in the same overlay network as dataplane (`nextmini_nextmini-net`).
2. Insert explicit routes with `route/insert_route.py`.
3. Read service logs to confirm `curl` reaches the server through Nextmini.

## Prerequisites

Before running this page, ensure:

- Multi-DC controller + dataplane are already running (see `examples/multi-dc`).
- You can run commands on the swarm manager.
- `curl-server` and `curl-client` images are built on the nodes where they will be scheduled.

## Deployment order

### 1. Build server/client images

On the host that will run the server service:

```bash
cd /path/to/nextmini/examples/swarm-curl
docker build -t curl-server -f src/Dockerfile.server .
```

On the host that will run the client service:

```bash
cd /path/to/nextmini/examples/swarm-curl
docker build -t curl-client -f src/Dockerfile.client .
```

### 2. Manager: create server then client services

From the swarm manager, deploy server first:

```bash
docker service create \
  --name curl-server \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --publish 8080:8080 \
  --constraint 'node.hostname==<SERVER_HOSTNAME>' \
  --restart-condition on-failure \
  --restart-delay 5s \
  --restart-max-attempts 3 \
  curl-server:latest
```

Then deploy client:

```bash
docker service create \
  --name curl-client \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --constraint 'node.hostname==<CLIENT_HOSTNAME>' \
  --restart-condition on-failure \
  --restart-delay 10s \
  --restart-max-attempts 5 \
  curl-client:latest
```

### 3. Insert routes

`route/insert_route.py` does two important things:

- Reads service VIPs for `curl-client` and `curl-server` from Docker.
- Inserts two DB routes using `INTERMEDIATE_HOPS = [1, 2, 3]`:
  - client -> `1,2,3` -> server
  - server -> `3,2,1` -> client

Before running it, update database host in `route/insert_route.py` if needed:

```bash
cd /path/to/nextmini/examples/swarm-curl
rg 'host=' route/insert_route.py
```

Then insert routes:

```bash
cd route
uv run insert_route.py
```

## Verify

From swarm manager:

```bash
docker service ls
docker service logs --tail 100 curl-server
docker service logs --tail 100 curl-client
```

Successful client logs include a completed `curl` run and HTTP output from the server (`Hello World`).

Optional DB verification from the controller host:

```bash
cd /path/to/nextmini/examples/multi-dc
docker compose -f controller-standalone.yml exec postgres \
  psql -U pgusr -d nextmini -c "SELECT src_node_id, dst_node_id, route FROM routes LIMIT 10;"
```

## Tear down

Remove client/server services from the manager:

```bash
docker service rm curl-client curl-server
```

If this environment was created only for this test, also remove dataplane and control-plane services:

```bash
# swarm manager
docker stack rm nextmini

# controller host
cd /path/to/nextmini/examples/multi-dc
docker compose -f controller-standalone.yml down
```
