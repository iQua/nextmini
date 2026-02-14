---
title: "Distributed PyTorch Trainers"
description: ""
---


# Distributed PyTorch Trainers

## Running a Distributed PyTorch Trainer with OpenMPI on a Single Machine

`Nextmini` is designed to facilitate distributed machine learning training. We now show a simple example of training an MNIST model between multiple docker containers using PyTorch's own distributed data parallel framework and OpenMPI. All docker containers will be launched on the same physical machine (Linux or macOS).

Before starting to build the docker image, it is recommended to start from a clean slate:

```bash
docker system prune -a
```

This will remove all stopped containers, all networks not used by at least one container, all images without at least one container associated to them, as well as all build cache. If you wish to remove all existing volumes at the same time as well, run:

```bash
docker system prune -a --volumes -f
```

To build and run the docker image in this example, run the following in the `examples/pytorch` directory:

```bash
docker compose build && docker compose up
```

This will start four Nextmini dataplane nodes with OpenMPI installed, and connect them to a single Nextmini controller. To start training, open another terminal and attach to `node1` with

```bash
docker exec -it node1 /bin/bash
```

Once we are logged into `node1`, we can run a simple `mpirun` session with OpenMPI:

```bash
mpirun --allow-run-as-root -np 4 echo hello world
```

We can also run a Python script using `uv`:

```bash
mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by :OVERSUBSCRIBE uv run test.py
```

We should see four `Hello World!` printed after the Python packages are downloaded and installed.

Finally, we can start distributed training with PyTorch:

```bash
sh train_lenet5.sh
```

This should start a training session for a `LeNet-5` model to be trained with the `MNIST` dataset across four training nodes for 10 epochs, each running in its own Docker container.

## Optional: Stream training metrics through the Python dataplane API

When you want to push tensors or scalar metrics directly into the Nextmini dataplane from the trainers, use the Python bindings described in [PyTorch + Nextmini Python API Quickstart](/docs/examples/pytorch_python_api):

1. Build and install the `nextmini_py` wheel (`maturin build --release -m python-api/Cargo.toml; pip install target/wheels/nextmini_py-*.whl`).
2. In your trainer script, gate the telemetry path behind environment variables (or any equivalent config):

   ```bash
   export NEXTMINI_CONFIG=/absolute/path/node-config.toml
   export NEXTMINI_DST_NODE=2   # numeric node id that should receive telemetry
   ```

3. Run the training job (for example `python examples/pytorch/gpt2.py --num-epochs 1`). If your hook is enabled, build a `PacketView` from each tensor and ship it with `send_to_node`.

The current `examples/pytorch/*.py` files do not include this telemetry hook by default, so add it explicitly where needed. On the destination node you can mirror the setup with another Python worker and call `rx.recv(timeout_ms=2000)` to consume metrics. The bindings reuse the same routing tables as the Rust dataplane, so multicast fan-out and QoS policies apply automatically.

## Running a Distributed PyTorch Trainer across Multiple Machines

> **Warning**
> The following instructions have not been verified to work correctly.


Before starting, make sure all the containers are stopped and removed.

```bash
docker rm -f $(docker ps -aq)
```

And remove all the Nextmini related networks, for example, `nextmini_network`.

```bash
docker network rm nextmini_network
```

Before running this example, at least three linux machines (or virtual machine instances) need to be set up with Ubuntu 24.04, including one controller instance, one Docker Swarm manager, and multiple worker instances. Docker needs to be pre-installed with `sudo` privileges. It is suggested that the docker directory is moved out of root which usually has small disk partition. You can refer the `Step 2` in `nextmini/examples/arbutus/readme.md` for guides towards setting up docker properly.

### Step 1

On the controller instance, build controller and postgres image:

```bash
cd nextmini/examples/pytorch
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

Controller and postgres services can be started by:

```bash
docker compose -f controller-swarm.yml build; docker compose -f controller-swarm.yml up
# docker compose -f controller-swarm.yml build --no-cache; docker compose -f controller-swarm.yml up
```

On the manager instance, `<CONTROLLER_IP>` in `dataplane-swarm.yml` should be altered accordingly.

### Step 2

Build the pytorch base image on all manager and work instances :

```bash
cd nextmini/
docker build -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
# docker build --no-cache --pull -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
```

### Step 3

On the manager instance, start the docker swarm:

```bash
docker swarm init --advertise-addr <Manager IP>
```

On all worker instances, join into the swarm network with the swarm token logged out:

```bash
docker swarm join --token SWMTKN-1-xxxx <SWARM_MANAGER_IP>:<port>
```

On the manager instance, you can check the status of nodes by:

```bash
docker node ls
```

### Step 4

After all workers has joined the swarm, deploy services on the manager instance by:

```bash
cd examples/pytorch
docker stack deploy -c dataplane-swarm.yml nextmini
```

To check the status of services, use:
```bash
docker service ls
```

### Step 5

On the manager instance, find the container ID for node1 by:

```bash
docker ps -a
```

```bash
docker exec -it <containerID> /bin/bash
```

Once logged into `node1`, we can run a simple `mpirun` session with OpenMPI:

```bash
mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 echo hello world
```

We can also run a Python script using `uv`:

```bash
mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by slot uv run test.py
```

We should see four `Hello World!` printed after the Python packages are downloaded and installed.

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

### Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & db VM instance in DigitalOcean, use the command:

```bash
docker compose -f controller-swarm.yml down
```
