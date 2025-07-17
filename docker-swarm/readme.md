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

## Step 6:
