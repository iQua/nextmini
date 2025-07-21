# Running a Simple Distributed PyTorch Trainer with Docker: Single Machine Setup

_Nextmini_ is designed to facilitate distributed machine learning training. We now show a simple example of training an MNIST model between multiple docker containers using PyTorch's own distributed data parallel framework and OpenMPI. All docker containers will be launched on the same physical machine (Linux or Mac).

Before starting to build the docker image, it is recommended to start from a clean slate:

```bash
docker system prune -a
```

This will remove all stopped containers, all unused networks and volumes, and all build cache. If you wish to remove all existing volumes at the same time, run:

```bash
docker system prune -a --volumes -f
```

To build and run the docker image in this example, simply execute the following:

```bash
cd ./examples/pytorch && docker compose build && docker compose up
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
mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by slot uv run test.py
```

We should see four `Hello World!` printed after the Python packages are downloaded and installed.

Finally, we can start distributed training with PyTorch:

```bash
sh train.sh
```

This should start a training session for a `LeNet-5` model to be trained with the `MNIST` dataset across four training nodes, each running in its own Docker container.

# Running a Simple Distributed PyTorch Trainer with Docker-Swarm: Multiple Machines Setup

## Step 0

At least three linux machines (1 controller instance, 1 manager, >= 1worker instances) need to be set up with Ubuntu 24.04. Docker needs to be pre-installed with sudo right. It is suggested that the docker directory is moved out of root which usually has small disk partition. You can refer the `Step 2` in `nexminit/examples/arbutus/readme.md` for guides towards setting up docker properly.

## Step 1

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
```

On the manager instance, the controller IP in the `dataplane-swarm.yml` should be altered accordingly.

## Step 2

Build the pytorch base image on all manager and work instances :

```bash
cd nextmini/
docker build -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
```

## Step 3

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

## Step 4

After all workers has joined the swarm, deploy services on the manager instance by:

```bash
cd examples/pytorch
docker stack deploy -c dataplane-swarm.yml nextmini
```

## Step 5

On the manager instance, find the container ID for node1 by:

```bash
docker ps -a
```

```bash
docker exec -it <continaerID> /bin/bash
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
sh train.sh
```

## Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & db VM instance in DigitalOcean, use the command:

```bash
docker compose -f controller-swarm.yml down
```
