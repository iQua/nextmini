# Distributed PyTorch Trainers

This example runs PyTorch DistributedDataParallel across multiple Nextmini dataplane containers using OpenMPI.

Scenario directory: `examples/pytorch/`.

## Single host (Docker Compose)

```bash
cd examples/pytorch
docker compose up --build
```

Attach to `node1`:

```bash
docker exec -it node1 /bin/bash
```

Sanity check OpenMPI:

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

Stop and clean up:

```bash
cd examples/pytorch
docker compose down -v
```

## Multi-host (Docker Swarm)

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

When you want to push tensors or scalar metrics directly into the Nextmini dataplane from Python (without going through TUN), use the `nextmini_py` bindings described in [Python API quickstart](pytorch_python_api.md):

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
