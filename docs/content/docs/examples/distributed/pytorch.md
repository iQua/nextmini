---
title: "Distributed PyTorch Trainers on a Multi-VM Docker Swarm"
description: "Runs PyTorch DDP training over a Nextmini overlay across multiple Linux VMs (4-node example)."
---


Before starting, make sure all the containers are stopped and removed.

```bash
docker rm -f $(docker ps -aq)
```

And remove all the Nextmini related networks, for example, `nextmini_network`.

```bash
docker network rm nextmini_network
```

Before running this example, at least three Linux VMs (or bare-metal hosts) need to be available with Ubuntu 24.04, including one controller instance, one Docker Swarm manager, and one or more worker instances. The walkthrough below uses a 4-node setup (one controller + manager VM that also runs `node1`, plus three worker VMs that run `node2`, `node3`, and `node4`). Docker needs to be pre-installed with `sudo` privileges. It is suggested that the docker directory is moved out of root which usually has a small disk partition. You can refer to [Arbutus Cloud Deployment](/docs/examples/deployment/arbutus) for setup guidance.

### Step 1: Build the controller image and prepare config

On the controller instance, build the controller image and pull Postgres:

```bash
cd nextmini/examples/sba-swarm
docker build -t nextmini_controller_pytorch -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
```

Then, add the following to the `controller-config.toml` to ensure successful connection to controller:

```toml
[db]
user = "pgusr"
password = "pgpwrd"
host = "postgres"
database = "nextmini"
port = "5432"
```

On the manager instance, `<CONTROLLER_IP>` in `dataplane-swarm.yml` should be altered accordingly.

### Step 2: Build the dataplane image on every VM

Build the pytorch base image on all manager and worker instances:

```bash
cd nextmini/
docker build -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
# docker build --no-cache --pull -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
```

### Step 3: Initialize the Docker Swarm

Initialize the swarm on the manager *before* starting any compose stack so libnetwork is in its final state when compose creates its bridge network:

```bash
docker swarm init --advertise-addr <Manager IP>
```

On each worker instance, join the swarm with the token printed by the manager:

```bash
docker swarm join --token SWMTKN-1-xxxx <SWARM_MANAGER_IP>:<port>
```

On the manager instance, verify that all nodes are listed:

```bash
docker node ls
```

### Step 4: Start the controller and Postgres stack

With the swarm already initialized, bring up the controller and Postgres compose stack on the controller instance:

```bash
cd nextmini/examples/sba-swarm
docker compose -f controller-swarm.yml build; docker compose -f controller-swarm.yml up
# docker compose -f controller-swarm.yml build --no-cache; docker compose -f controller-swarm.yml up
```

### Step 5: Deploy the dataplane stack

After the controller is healthy, deploy the dataplane services on the manager instance:

```bash
cd examples/sba-swarm
docker stack deploy -c dataplane-swarm.yml nextmini
```

To check the status of services, use:
```bash
docker service ls
```

### Step 6: Run mpirun and start training

On the manager instance, find the container ID for node1 by:

```bash
docker ps -a
```

```bash
docker exec -it <containerID> /bin/bash
```

Once logged into `node1`, we can run a simple `mpirun` session with OpenMPI:

```bash
mpirun --allow-run-as-root -np 2 -H 10.0.0.1:1,10.0.0.2:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by :OVERSUBSCRIBE uv run test.py
```

We should see two `Hello World!` printed after the Python packages are downloaded and installed.

Finally, we can start distributed training with PyTorch:

```bash
# Train Lenet5 with
sh train_lenet5.sh

# Train GPT2 with
sh train_gpt2.sh

# Train Resnet with
sh train_resnet.sh

# Train VGG16 with
sh train_vgg16.sh
```

To train different variants of resnet, simply simply change the `--type` command line argument in `train_resnet.sh` on the manager instance.

### Optional: Emit metrics via the Python dataplane API

If you want these SBA scenarios to stream intermediate loss/activation tensors through Nextmini (instead of relying solely on TUN delivery), follow the steps in [Python API](/docs/python-api):

1. Install the `nextmini_py` wheel on the swarm nodes.
2. Add a small telemetry hook in your trainer script that instantiates `nextmini_py.Dataplane("/abs/path/node-config.toml")`.
3. Build `nextmini_py.PacketView` objects and publish metrics with `send_to_node(dst_node_id=...)`.

A companion receiver (launched on another trainer or analytics node) can call `rx.recv()` to ingest the payloads for dashboards or adaptive schedulers.

### Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & Postgres VM, use the command:

```bash
docker compose -f controller-swarm.yml down
```
