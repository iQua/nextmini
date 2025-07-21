# How to run PyTorch example across datacenters

## Step 1

First, build controller and postgres image:

```bash
cd nextmini/examples/pytorch
docker build -t nextmini_controller_pytorch -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
sudo docker-compose -f controller-swarm.yml build
```

Controller and postgres services can be started by:

```bash
sudo docker-compose -f controller-standalone.yml up
```

On the manager instance, the controller IP in the `dataplane-swarm.yml` should be altered according to the actual controller ip.

## Step 2

On another terminal, build the pytorch base image on **all** instances :

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

Then, label all worker nodes to dataplane for training:

```bash
docker node update --label-add type=dataplane <host name>
```

## Step 4

After all workers has joined the swarm, deploy services on the manager instance by:

```bash
cd examples/pytorch
docker stack deploy -c dataplane-swarm.yml nextmini
```

## Step 5

On manager instance, find the container ID for node1 by:

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

# in Arbutus, use the command below:
sudo docker-compose -f controller-swarm.yml down
```
