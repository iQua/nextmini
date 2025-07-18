# Nextmini Arbutus Setup Guide

This is a guide for setting up a large scale Strato network on the Compute Canada Arbutus Cloud.

## Step 1: Launching an Arbutus instance

Follow through `step 1` and `step 2` in the `./examples/arbutus/readme.md` to set up an instance in arbutus.

## Step 2: Cloning nextmini

Clone the nextmini repository from github by:

```bash
git clone git@github.com:iQua/nextmini.git
```

Then, build the base images used for controller and dataplane. This step might take a long time.

```bash
cd nextmini/examples/simple-swarm
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
docker pull postgres:alpine
```

You can test with the following command:

```bash
docker images
```

Logs similar to the following should appear in the console:

```bash
REPOSITORY            TAG       IMAGE ID       CREATED          SIZE
nextmini_controller   latest    3e2a09dc26e3   34 minutes ago   320MB
nextmini_datapath     latest    70969d365d22   3 hours ago      326MB
alpine                latest    9234e8fb04c4   2 days ago       8.31MB
rust                  alpine    4558dce739eb   3 weeks ago      968MB
postgres              alpine    deedec4d7fe4   5 weeks ago      279MB
```

## Step 3: Create instance-snapshot on Arbutus

Follow the `step 7` in the `./examples/arbutus/readme.md`. This time, create snapshot for your nextmini instance in `step 1` and ignore all `_Starto_` related guides.

Then, run the following commands to build the dataplane base image on each snapshot:

```bash
cd nextmini/examples/simple-swarm
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
```

## Step 4: Set up docker swarm manager

First, initialize the swarm at one of the instance. This instance will be set as manager for the swarm. The private network IP shall be used as the `MANAGER-IP` in the following command:

```bash
docker swarm init --advertise-addr <MANAGER-IP>
```

You should see logs in the console similar to the following one:

```bash
docker swarm init --advertise-addr 192.168.99.100
Swarm initialized: current node (dxn1zf6l61qsb1josjja83ngz) is now a manager.

To add a worker to this swarm, run the following command:

    docker swarm join \
    --token SWMTKN-1-49nj1cmql0jkz5s954yi3oex3nedyz0fb0xx14ie39trti4wxv-8vxv8rssmk743ojnwacrr2e7c \
    192.168.99.100:2377

To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.
```

For more information, refer to Docker's official [guide](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/).

## Step 5: Connect worker nodes into the swarm

Now, use jump ssh techinque into other instances you created. Paste the logs from `step 4` into the console :

```bash
 docker swarm join \
  --token  SWMTKN-1-49nj1cmql0jkz5s954yi3oex3nedyz0fb0xx14ie39trti4wxv-8vxv8rssmk743ojnwacrr2e7c \
  192.168.99.100:2377
```

You will see the following logged out for successful joining:

```bash
This node joined a swarm as a worker.
```

Repeat this step for all other non-manager instances.

## Step 6: Run the example

Deploy and run the example to all nodes with the following command:

```bash
cd nextmini/examples/simple-swarm
docker stack deploy -c docker-compose.swarm.yml nextmini
```

This by default creates three replicas for the dataplane nodes. You can update the service with ideal number of replicas (5 here) with the following command:

```bash
docker service update --replicas 5 nextmini_dataplane
```

In this case, docker will automatically put dataplane nodes into different work instances.

## Step 7: Retrieve container ID for logs and tests

First, we need to get all containers by the following command:

```bash
sudo docker ps -a"
```

You should see something similar to the followinng:

```bash
ubuntu@nextmini:~/nextmini/examples/simple$ docker ps -a
CONTAINER ID   IMAGE                        COMMAND                  CREATED          STATUS                    PORTS      NAMES
e51b0571ae9a   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   17 seconds ago   Up 16 seconds                        nextmini_node1.1.6vgtu20657noc5i0thxkhu412
c3f921458a3c   nextmini_controller:latest   "/bin/bash -c 'sleep…"   18 seconds ago   Up 17 seconds             3000/tcp   nextmini_controller.1.mylyvt3c40dhmgi5p5rp6fbmg
9c605d5dd6bb   postgres:alpine              "docker-entrypoint.s…"   20 seconds ago   Up 19 seconds (healthy)   5432/tcp   nextmini_postgres.1.40qozl97ls0kbmvqbbol0tq7u
```

The docker container ID for node1 is `277d0e508ec8` in this case. You should obtain the docker continaer ID of other nodes on **other instances**.

With the container ID, you can see the container logs by

```bash
docker logs <container_ID>
```

you can enter the container bash by:

```bash
docker exec -it <container_ID> /bin/bash
```

## Step 8: Clean up the docker

```bash
# Remove the NextMini stack (removes services and networks)
docker stack rm nextmini

# Wait for around 10s and Clean up
docker system prune --volumes -f

# Verify cleanup
docker network ls
```
