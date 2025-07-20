# Guide run PyTorch example across datacenters

## Step 1

Controller and postgres should be deployed according to the `multi-dc/README.md` in this step. The controller IP in the `pytorch/dataplane-swarm.yml` should be altered accordingly on the manager instance.

## Step 2

Build the pytorch base image on all the instances :

```bash
cd nextmini/
docker build -t nextmini_datapath_pytorch -f ./examples/pytorch/Dockerfile .
```

Label the worker instances to dataplane:

```bash
docker node update --label-add type=dataplane <host name>
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

In the manager instance, you can check the status of the nodes by:

```bash
docker node ls
```

## Step 4

After all workers has joined the swarm, deploy services on the manager instance by:

```bash
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

Once we are logged into `node1`, we can run a simple `mpirun` session with OpenMPI:

```bash
mpirun --allow-run-as-root -np 4 echo hello world
```

We can also run a Python script using `uv`:

```bash
mpirun --allow-run-as-root -np 4 -H 10.0.0.1:1,10.0.0.2:1,10.0.0.3:1,10.0.0.4:1 -x MASTER_ADDR=node1 -x PATH -bind-to none -map-by slot uv run test.py
```

## Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & db VM instance in DigitalOcean, use the command:

```bash
docker compose -f controller-standalone.yml down

# in Arbutus, use the command below:
sudo docker-compose -f controller-standalone.yml down
```
