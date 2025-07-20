# Guide run PyTorch example across datacenters

## Step 1

Controller and postgres should be deployed according to the `multi-dc/README.md` in this step. The controller IP in the `pytorch/dataplane-swarm.yml` should be altered accordingly on the manager instance.

## Step 2

Build the datapath base image on all the instances :

```bash
cd nextmini/examples/pytorch
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
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
docker stack deploy -c dataplane-deploy.yml nextmini
```

After this, you could see the logging info by:

```bash
docker service logs nextmini_dataplane
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
