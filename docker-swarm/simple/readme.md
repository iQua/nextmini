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
 sudo docker stack services nextmini
```

Logs similar to the following should be seen in the console:

```bash
ID             NAME                  MODE         REPLICAS   IMAGE                        PORTS
ftvp45rh511e   nextmini_controller   replicated   1/1        nextmini_controller:latest   *:3000->3000/tcp
o827k6poabm1   nextmini_node1        replicated   1/1        nextmini_datapath:latest
isucaltq9mao   nextmini_node2        replicated   1/1        nextmini_datapath:latest
3obvrx7zyxli   nextmini_node3        replicated   1/1        nextmini_datapath:latest
zkbmyv7u6h2h   nextmini_postgres     replicated   0/1        postgres:alpine              *:5432->5432/tcp
```

## Step 7: Run the Iperf3 test

First, we need to get the container name by the following command:

```bash
sudo docker ps --filter "name=<nextmini_node1>"
```

Note : <nextmini_node1> should be replaced to the any other names logged out in `step 6`. Importantly, this command should be run on the correspondance instance where the <nextmini_node1> is running.

You should see something similar to the followinng:

```bash
CONTAINER ID   IMAGE                      COMMAND                  CREATED          STATUS          PORTS     NAMES
b6d3e0f533b4   nextmini_datapath:latest   "/bin/bash -c 'sleep…"   51 minutes ago   Up 51 minutes             nextmini_node1.1.oicwpz896u5rr5ibtsk2ahs8r
```

The docker container name is `nextmini_node1.1.oicwpz896u5rr5ibtsk2ahs8r` in this case. You should obtain the docker continaer name of other nodes on other instances as well. With the container name, you can docker execute into them by:

```bash
docker exec -it <container name> /bin/bash
```

# Remaining Issue

One urgent issue now is the pg database cannot be connected successfully by the controller. When using docker swarm, the IP address canot be assigned manually. In other words, the IP address of all containers are assigned at runtime dynamically. This nature has made it difficult to set the `host` field inside `controller-config.toml` file. One potential solution now is to make the host an environment variable and let controller access it while running.
