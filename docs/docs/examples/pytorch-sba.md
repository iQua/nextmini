# PyTorch on SBA (Docker Swarm)

Run distributed PyTorch training on a multi-node Docker Swarm using the `examples/sba-swarm` scenario.

## Prerequisites

- 1 controller VM (runs controller + Postgres via `examples/sba-swarm/controller-swarm.yml`)
- 1 Swarm manager + 1 or more Swarm workers (runs dataplane nodes via `examples/sba-swarm/dataplane-swarm.yml`)
- Docker Engine installed everywhere

For legacy Arbutus-specific notes, see [Arbutus setup notes](arbutus.md).

## 1) Start controller + Postgres (controller VM)

```bash
cd nextmini/examples/sba-swarm
docker compose -f controller-swarm.yml up --build
```

## 2) Build the dataplane image (manager + workers)

Build this image on every Swarm node (or build once and push it to a registry):

```bash
cd nextmini
docker build -t nextmini_datapath_pytorch -f examples/sba-swarm/Dockerfile .
```

## 3) Create the Swarm and deploy the dataplane stack

On the manager:

```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

On each worker:

```bash
docker swarm join --token <SWARM_JOIN_TOKEN> <MANAGER_IP>:2377
```

Edit `examples/sba-swarm/dataplane-swarm.yml` and replace the hard-coded controller address (`206.12.89.244:3000`) with your controller VM IP, then deploy:

```bash
cd nextmini/examples/sba-swarm
docker stack deploy -c dataplane-swarm.yml nextmini
docker service ls
```

## 4) Run training inside `node1`

```bash
docker ps
docker exec -it <node1_container_id> /bin/bash
cd /var/nextmini
```

Sanity check OpenMPI:

```bash
mpirun --allow-run-as-root -np 2 -H 10.0.0.1:1,10.0.0.2:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by :OVERSUBSCRIBE uv run test.py
```

Then run a training script:

```bash
sh train_lenet5.sh
# sh train_gpt2.sh
# sh train_resnet.sh
# sh train_vgg16.sh
```

## Optional: emit metrics via the Python dataplane API

If you want to stream intermediate tensors (or scalar metrics) through Nextmini from Python, follow the [Python API quickstart](pytorch_python_api.md).

## Cleanup

On the manager:

```bash
docker stack rm nextmini
```

On the controller VM:

```bash
cd nextmini/examples/sba-swarm
docker compose -f controller-swarm.yml down
```
