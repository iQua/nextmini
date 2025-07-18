# Nextmini Arbutus Setup Guide

This is a guide for setting up a large scale Strato network on the Compute Canada Arbutus Cloud.

## Overview

Setting up Strato over the Arbutus Cloud can be generally divided into setting up the controller and the data plane. The core strategy is to first set up the controller, then set up _one_ data plane node on a single VM. Afterwards, clone image of the VM to get more nodes.

## Step 1: Launching an Arbutus instance

Follow through `step 1` and `step 2` in the `./examples/arbutus/readme.md` to set up an instance in arbutus.

## Step 2: Cloning and building nextmini

Clone the nextmini repository from github by:

```bash
git clone git@github.com:iQua/nextmini.git
```

Then, build the base images used for controller and dataplane. This step might take a long time.

```bash
cd nextmini
sudo docker build -t nextmini_controller -f controller/Dockerfile .
sudo docker build -t nextmini_datapath -f dataplane/Dockerfile .
docker pull postgres:alpine
```

You can test with the following command:

```bash
sudo docker images
```

Logs similar to the following should appear in the console:

```bash
ubuntu@nextmini:~/nextmini$ sudo docker images
REPOSITORY            TAG       IMAGE ID       CREATED          SIZE
nextmini_datapath     latest    00e6cda8837c   25 seconds ago   326MB
<none>                <none>    1c8a57e96426   48 seconds ago   2.13GB
nextmini_controller   latest    1ca91083b403   5 minutes ago    320MB
<none>                <none>    8837413ac609   6 minutes ago    1.75GB
alpine                latest    9234e8fb04c4   2 days ago       8.31MB
rust                  alpine    4558dce739eb   3 weeks ago      968MB
```

## Step 3: Create instance-snapshot on Arbutus

Follow the `step 7` in the `./examples/arbutus/readme.md`. This time, create snapshot for your nextmini instance in `step 1` and ignore all `_Starto_` related guides.

## Step 4: Set up docker swarm manager

First, initialize the swarm at one of the instance. This instance will be set as manager for the swarm. The private network IP shall be used as the `MANAGER-IP` in the following command:

```bash
sudo docker swarm init --advertise-addr <MANAGER-IP>
```

You should see logs in the console similar to the following one:

```bash
sudo docker swarm init --advertise-addr 192.168.99.100
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

First, you should go to the example directory in your **Manager Instance**:

```bash
cd nextmini/docker-swarm/simple
```

Deploy the example to all nodes with the following command:

```bash
sudo docker stack deploy -c docker-compose.yml nextmini
```

Run the example:

```bash
docker stack services nextmini
```

Logs similar to the following should be seen in the console:

```bash
ID             NAME                  MODE         REPLICAS   IMAGE                        PORTS
ftvp45rh511e   nextmini_controller   replicated   1/1        nextmini_controller:latest   *:3000->3000/tcp
o827k6poabm1   nextmini_node1        replicated   1/1        nextmini_datapath:latest
isucaltq9mao   nextmini_node2        replicated   1/1        nextmini_datapath:latest
3obvrx7zyxli   nextmini_node3        replicated   1/1        nextmini_datapath:latest
zkbmyv7u6h2h   nextmini_postgres     replicated   1/1        postgres:alpine              *:5432->5432/tcp
```

## Step 7: Run the Iperf3 test

First, we need to get the container name by the following command:

```bash
sudo docker ps -a"
```

You should see something similar to the followinng:

```bash
ubuntu@nextmini:~/nextmini/examples/simple$ docker ps -a
CONTAINER ID   IMAGE                        COMMAND                  CREATED              STATUS                            PORTS      NAMES
277d0e508ec8   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   6 seconds ago        Up 2 seconds                                 nextmini_node1.1.gh96ocrsuryribkmn54stdkid
e1e6a96b6000   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   19 seconds ago       Exited (0) 7 seconds ago                     nextmini_node1.1.z0bh07o2pwwywvmyofiqam0l2
5f0ac705ba43   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   31 seconds ago       Exited (0) 20 seconds ago                    nextmini_node1.1.npat5xeeyz7yyhctl88nj2ydu
ce3a5c7117a6   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   44 seconds ago       Exited (0) 32 seconds ago                    nextmini_node1.1.l3u8kz9b1ws6s0otfzdzrsrcj
d9e1b5af57a7   nextmini_datapath:latest     "/bin/bash -c 'sleep…"   57 seconds ago       Exited (0) 45 seconds ago                    nextmini_node1.1.v4of847akb29aq7altbpnsz4o
db65272d746a   nextmini_controller:latest   "/bin/bash -c 'sleep…"   About a minute ago   Up About a minute                 3000/tcp   nextmini_controller.1.uc45n3qxzuf61t0v0ujnmi5wi
9cc75306a1bc   postgres:alpine              "docker-entrypoint.s…"   About a minute ago   Up About a minute (healthy)       5432/tcp   nextmini_postgres.1.z2bkxfvc1v14e24tz2e9ai9qj
8173a99c7e25   nextmini_controller:latest   "/bin/bash -c 'sleep…"   About a minute ago   Exited (101) About a minute ago              nextmini_controller.1.ip8pnftnmuvelgyna9nzq3ngn
116413d6a5ef   postgres:alpine              "docker-entrypoint.s…"   About a minute ago   Exited (3) About a minute ago                nextmini_postgres.1.rjf6m35s49ok9kb4rf4oq3l2i
```

The docker container ID is `277d0e508ec8` in this case. You should obtain the docker continaer ID of other nodes on other instances as well. You can enter into the bash of containers with:

```bash
docker exec -it <container_ID> /bin/bash
```

# Remaining Issue

One urgent issue now is the pg database cannot be connected successfully by the controller. When using docker swarm, the IP address canot be assigned manually. In other words, the IP address of all containers are assigned at runtime dynamically. This nature has made it difficult to set the `host` field inside `controller-config.toml` file. One potential solution now is to make the host an environment variable and let controller access it while running. It is also important to clean up the docker history when deploying services.

## Other useful commands

```bash
sudo docker swarm leave
sudo docker ps -a
docker ps --filter "name=node3"
sudo docker node ls
sudo docker service ls
docker volume rm nextmini_postgres_data
```
