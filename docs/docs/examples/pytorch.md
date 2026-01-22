# Distributed PyTorch Trainers

## Running a Distributed PyTorch Trainer with OpenMPI on a Single Machine

`Nextmini` is designed to facilitate distributed machine learning training. We now show a simple example of training an MNIST model between multiple docker containers using PyTorch's own distributed data parallel framework and OpenMPI. All docker containers will be launched on the same physical machine (Linux or macOS).

Scenario directory: `examples/pytorch/`.

This runs a DistributedDataParallel job with OpenMPI across four Nextmini dataplane containers **on a single machine** (Linux or macOS).

!!! warning "Optional: start from a clean Docker slate"

    These commands remove stopped containers, unused networks/images, and (optionally) volumes.

    ```bash
    docker system prune -a
    # docker system prune -a --volumes -f
    ```

To build and run the docker image in this example, run the following in the `examples/pytorch` directory:

```bash
cd examples/pytorch
docker compose up --build
```

This will start four Nextmini dataplane nodes with OpenMPI installed, and connect them to a single Strato controller. To start training, open another terminal and attach to `node1` with:

```bash
docker exec -it node1 /bin/bash
```

Once we are logged into `node1`, we can run a simple `mpirun` session with OpenMPI:

```bash
mpirun --allow-run-as-root -np 4 echo hello world
```

Run a Python script with `uv`:

```bash
mpirun --allow-run-as-root -np 4 \
  -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 \
  -x MASTER_ADDR=node1 -x PATH \
  -bind-to none -map-by :OVERSUBSCRIBE \
  uv run test.py
```

Run training:

```bash
sh train_lenet5.sh
# sh train_gpt2.sh
# sh train_resnet.sh
# sh train_vgg16.sh
```

This should start a training session for a `LeNet-5` model to be trained with the `MNIST` dataset across four training nodes for 10 epochs, each running in its own Docker container.

Stop and clean up:

```bash
cd examples/pytorch
docker compose down -v
```

## Running a Distributed PyTorch Trainer across Multiple Machines

Before starting, make sure all the containers are stopped and removed.

```bash
docker rm -f $(docker ps -aq)
```

And remove all the Nextmini related networks, for example, `nextmini_network`.

```bash
docker network rm nextmini_network
```

Before running this example, at least three linux machines (or virtual machine instances) need to be set up with Ubuntu 24.04, including one controller instance, one Docker Swarm manager, and multiple worker instances. Docker needs to be pre-installed with `sudo` privileges.

Use the swarm manifests in `examples/pytorch/` when you have a Swarm manager + workers.

### 1) Start the controller + Postgres (controller VM)

```bash
cd examples/pytorch
docker compose -f controller-swarm.yml up --build
```

### 2) Build the dataplane image (manager + workers)

Build this image on every Swarm node (or build once and push to a registry):

```bash
cd nextmini
docker build -t nextmini_datapath_pytorch -f examples/pytorch/Dockerfile .
```

### 3) Deploy the dataplane stack

On the manager:

```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

On each worker:

```bash
docker swarm join --token <SWARM_JOIN_TOKEN> <MANAGER_IP>:2377
```

Edit `examples/pytorch/dataplane-swarm.yml` and replace `<CONTROLLER_IP>` with your controller VM IP, then deploy:

```bash
cd examples/pytorch
docker stack deploy -c dataplane-swarm.yml nextmini
docker service ls
```

### 4) Run training inside `node1`

```bash
docker ps
docker exec -it <node1_container_id> /bin/bash
cd /var/nextmini
sh train_lenet5.sh
```

### Cleanup

On the manager:

```bash
docker stack rm nextmini
```

On the controller VM:

```bash
cd examples/pytorch
docker compose -f controller-swarm.yml down
```

## Optional: Stream training metrics through the Python dataplane API

When you want to push tensors or scalar metrics directly into the Nextmini dataplane from Python (without going through TUN), use the `nextmini_py` bindings described in [PyTorch + Nextmini Python API Quickstart](pytorch_python_api.md):

1. Build and install the `nextmini_py` wheel (`maturin build --release -m python-api/Cargo.toml; pip install target/wheels/nextmini_py-*.whl`).
2. Add a small hook in your training loop (or gate it behind env vars in your custom script):

   ```bash
   export NEXTMINI_CONFIG=/absolute/path/node-config.toml
   export NEXTMINI_DST_NODE=2   # numeric node id that should receive telemetry
   ```

3. In Python, create the dataplane once and send bytes with `PacketView`:

   ```python
   import os
   import nextmini_py as nm

   dp = nm.Dataplane(os.environ["NEXTMINI_CONFIG"])
   dst = int(os.environ["NEXTMINI_DST_NODE"])
   dp.send_to_node(dst_node_id=dst, frozen=nm.PacketView(b"..."))
   ```

On the destination node you can mirror the setup with another Python process and call `register_receiver_from_node(src_node_id=...)` + `rx.recv(timeout_ms=...)` to consume the metrics. For a complete end-to-end reference (including large lossless transfers), see `examples/rl`.
