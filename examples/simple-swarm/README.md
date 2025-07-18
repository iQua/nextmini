# NextMini Docker Swarm Deployment

This example demonstrates how to deploy NextMini across multiple nodes using Docker Swarm.

## Architecture

- **Manager Instance**: Deploys the Nextmini datapath services to rest worker instances.
- **Worker Instances**: Join the docker swarm overlay network and runs the services disptached by the the Manager Instance.

## Prerequisites

You should have the following environment setup on all instances.

1. Ubuntu 24.04 Noble.
2. Docker installed with sudo right.

You should start the following guide on the **Manager Instance** first.

# Setup Guide

## Step 1: Clone and build nextmini

Clone the nextmini repository and checkout to the swarm branch:

```bash
git clone git@github.com:iQua/nextmini.git; cd nextmini; git checkout swarm
```

Then, build the base images for the instance.

```bash
cd nextmini/examples/simple-swarm
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
```

You can check if they are successfully installed with:

```bash
docker images
```

Logs similar to the following should appear in the console:

```bash
REPOSITORY            TAG       IMAGE ID       CREATED          SIZE
nextmini_datapath     latest    70969d365d22   3 hours ago      326MB
alpine                latest    9234e8fb04c4   2 days ago       8.31MB
rust                  alpine    4558dce739eb   3 weeks ago      968MB
```

## Step 2. Initialize Docker Swarm

**On Manager Instance:**

```bash
docker swarm init --advertise-addr <MANAGER-IP>
```

You should find something similar to the following in the logs:

```bash
docker swarm join --token <worker-token> <manager-ip>:2377

```

This shall be copied to join worker instances into the docker swarm overlay netwrk. At this time, you should repeat `Step 1` and `Step 2` for all instances before proceeding to the next step.

**On Worker Instance**:

Use the logs above copied from manager node to join a worker into the docker swarm network.

```bash
docker swarm join --token <worker-token> <manager-ip>:2377
```

## Step 3. Deploy the service

By runnning the following command on the **Manager Instance**, docker will automatically deploy the services to all worker instances. This depolyment may take serveral minutes.

```bash
docker stack deploy -c docker-compose.swarm.yml nextmini
```

You can run the following commands to check if the services have been replicated successfully.

```bash
docker stack services nextmini
```

## Step 4. Run tests

You need to get the container ID first on **target test instances**:

```bash
docker ps -a
```

You would see something similar as below logged out:

```bash
CONTAINER ID   IMAGE                      COMMAND                   CREATED          STATUS          PORTS     NAMES
02b2cdb3b073   nextmini_datapath:latest   "/bin/bash -c '\n  ec…"   19 minutes ago   Up 19 minutes             nextmini_dataplane.6.wzi42us4v92c2qfutdw3n16qz
920ba3432965   nextmini_datapath:latest   "/bin/bash -c '\n  ec…"   19 minutes ago   Up 19 minutes             nextmini_dataplane.7.0e7zgr4dwktbiyoexe4touicq
bc7d08d75bf4   nextmini_datapath:latest   "/bin/bash -c '\n  ec…"   19 minutes ago   Up 19 minutes             nextmini_dataplane.8.mqlb64edgcz6gqjfe6sttcni3
```

The container ID is on the left most column. Now, you can enter into the bash of a container and start any tests.

```bash
docker exec -it <continaer_Id> bash
```

## Complementary Information

### Node ID Assignment

Dataplane nodes automatically get assigned IDs 1, 2, 3 based on their task slot:

- `node1` (hostname) → `node_id = 1`
- `node2` (hostname) → `node_id = 2`
- `node3` (hostname) → `node_id = 3`

### Monitoring

On the manager instance, you can check service status by:

```bash
docker stack services nextmini
```

View logs:

```bash
docker service logs nextmini_controller
docker service logs nextmini_dataplane
docker service logs nextmini_postgres
```

### Scaling

On the manager instance, you can change number of dataplane nodes by:

```bash
docker service update --replicas 5 nextmini_dataplane
```

Update `controller-config.toml` to match new node count.

### Cleanup

```bash
docker stack rm nextmini
```
