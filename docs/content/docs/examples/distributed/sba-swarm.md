---
title: "SBA Swarm for Distributed Training"
description: "Deploys two PyTorch-capable dataplane nodes in Docker Swarm with a separate controller/Postgres host, then runs distributed training tests."
---

This example uses the files in `examples/sba-swarm/` to run distributed workloads over Nextmini:

- `controller-swarm.yml` starts controller + Postgres on a dedicated host.
- `dataplane-swarm.yml` deploys `node1` (manager) and `node2` (worker) through Docker Swarm.
- `run-test.sh` and `train_*.sh` run OpenMPI-based verification and model training.

For complete walkthroughs centered on workload goals, see [Distributed PyTorch Trainers on Sim, Boston and Arbutus](/docs/examples/distributed/pytorch-sba) and [Distributed ring all-reduce on Sim, Boston and Arbutus](/docs/examples/distributed/ring_allreduce-sba).

## Prerequisites

- One controller host.
- One swarm manager host.
- One swarm worker host.
- Docker on all hosts.

The dataplane file currently hardcodes controller IP `206.12.89.244` inside node startup commands. Replace it before deployment.

## Deployment order

### 1. Controller host: build and launch control plane

On the controller host:

```bash
cd /path/to/nextmini/examples/sba-swarm
docker build -t nextmini_controller_pytorch -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
docker compose -f controller-swarm.yml up -d
```

### 2. Manager and worker hosts: build dataplane image

On both swarm nodes:

```bash
cd /path/to/nextmini
docker build -t nextmini_datapath_pytorch -f ./examples/sba-swarm/Dockerfile .
```

### 3. Initialize swarm and join worker

On the manager:

```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

On the worker:

```bash
docker swarm join --token <WORKER_TOKEN> <MANAGER_IP>:2377
```

Verify on manager:

```bash
docker node ls
```

### 4. Manager: patch controller IP and deploy dataplane stack

From `examples/sba-swarm`, generate a deployment file with your real controller IP:

```bash
cd /path/to/nextmini/examples/sba-swarm
CONTROLLER_IP=<CONTROLLER_IP>
sed "s/206\.12\.89\.244/${CONTROLLER_IP}/g" dataplane-swarm.yml > dataplane-deploy.yml
```

Deploy:

```bash
docker stack deploy -c dataplane-deploy.yml nextmini
```

## Verify

On manager, ensure both services are running:

```bash
docker service ls
docker service ps nextmini_node1
docker service ps nextmini_node2
```

Find the `node1` container and run quick distributed test:

```bash
docker ps --filter name=nextmini_node1
# copy container ID, then:
docker exec -it <NODE1_CONTAINER_ID> bash
sh /var/nextmini/run-test.sh
```

Expected output from `test.py` is two `Hello World!` lines (one per rank).

You can then run one of the training scripts from the same container shell:

```bash
sh /var/nextmini/train_lenet5.sh
# or train_gpt2.sh / train_resnet.sh / train_vgg16.sh
```

## Tear down

On manager:

```bash
docker stack rm nextmini
```

On controller host:

```bash
cd /path/to/nextmini/examples/sba-swarm
docker compose -f controller-swarm.yml down
```

Optional: leave swarm mode when done:

```bash
docker swarm leave -f
```
