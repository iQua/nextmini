---
title: "Multi-DC Swarm Dataplane"
description: "Runs controller and database on one host, then deploys dataplane replicas from a Docker Swarm manager across worker hosts."
---

This example splits control-plane and dataplane responsibilities across machines:

- Controller host runs `examples/multi-dc/controller-standalone.yml`.
- Swarm manager and workers run dataplane tasks from `examples/multi-dc/dataplane-swarm.yml`.

Use this when controller/database should stay on a dedicated instance and dataplane nodes should be scheduled across multiple DC VMs.

If you also want to run external HTTP traffic across this swarm dataplane, continue with [Swarm curl Client/Server Over Nextmini](/docs/examples/distributed/swarm-curl) after dataplane services are stable.

## Prerequisites

You need:

- One controller/database host.
- One swarm manager host.
- One or more swarm worker hosts.
- Docker installed on all hosts.

`examples/multi-dc/dataplane-swarm.yml` expects the controller endpoint through `CONTROLLER_HOST`, so keep the controller host reachable from manager and workers on TCP `3000`.

## Deployment order

### 1. Controller host: build and start controller + Postgres

On the controller host:

```bash
cd /path/to/nextmini/examples/multi-dc
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
```

Before launch, make sure `controller-config.toml` matches your intended node count. By default it is `n_nodes = 15`.

```bash
rg 'full_mesh_config' controller-config.toml
```

Start the control-plane services:

```bash
docker compose -f controller-standalone.yml up -d
```

### 2. Manager and workers: build dataplane image

On every swarm node (manager and workers):

```bash
cd /path/to/nextmini/examples/multi-dc
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
```

### 3. Initialize swarm and join workers

On the manager:

```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

On each worker, run the join command printed by the manager:

```bash
docker swarm join --token <WORKER_TOKEN> <MANAGER_IP>:2377
```

Verify from the manager:

```bash
docker node ls
```

### 4. Manager: generate deploy file with controller address

`dataplane-swarm.yml` uses `REPLACE_WITH_MANAGER_IP` as a placeholder for `CONTROLLER_HOST`. Replace it with the real controller host IP and create a deploy file:

```bash
cd /path/to/nextmini/examples/multi-dc
CONTROLLER_IP=<CONTROLLER_PUBLIC_OR_PRIVATE_IP>
sed "s/REPLACE_WITH_MANAGER_IP/${CONTROLLER_IP}/g" dataplane-swarm.yml > dataplane-deploy.yml
```

Deploy dataplane services from the manager:

```bash
docker stack deploy -c dataplane-deploy.yml nextmini
```

## Verify

On the controller host:

```bash
cd /path/to/nextmini/examples/multi-dc
docker compose -f controller-standalone.yml ps
docker compose -f controller-standalone.yml logs --tail 100 controller
```

On the manager:

```bash
docker service ls
docker service ps nextmini_dataplane
docker service logs --tail 100 nextmini_dataplane
```

If you run external traffic tests later (for example `swarm-curl`), verify dataplane services are stable first and only then insert routes.

## Tear down

On the manager:

```bash
docker stack rm nextmini
```

On the controller host:

```bash
cd /path/to/nextmini/examples/multi-dc
docker compose -f controller-standalone.yml down
```

Optional: if you are done with this cluster, leave swarm mode on workers and manager:

```bash
docker swarm leave -f
```
