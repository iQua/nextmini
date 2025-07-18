Install docker on Arbutus:

```bash
sudo snap install docker
```

Then open the terminal and enter:

```bash
sudo bash deploy-swarm.sh
```

Logging:

```text
ubuntu@mini:~/nextmini/examples/swarm$ sudo bash deploy-swarm.sh
[INFO] Starting NextMini Docker Swarm deployment...
[INFO] Docker is running
[INFO] Ensuring this node is a Swarm Manager...
[INFO] This node is already a Swarm Manager. Skipping initialization.
[INFO] Labeling manager node with role=manager
i2fwmkj5rg9fryh7bd3hdqlzj
[INFO] Manager IP: 192.168.196.13
[INFO] Worker token obtained
[INFO] Building Docker images...
[INFO] Building controller image...
DEPRECATED: The legacy builder is deprecated and will be removed in a future release.
            Install the buildx component to build images with BuildKit:
            https://docs.docker.com/go/buildx/

Sending build context to Docker daemon    244MB
Step 1/14 : FROM rust:alpine AS build
 ---> 4558dce739eb
Step 2/14 : RUN apk update &&     apk upgrade --no-cache &&     apk add curl bash     build-base     openssl     ca-certificates
 ---> Using cache

...

Successfully built 2a826af4854c
Successfully tagged nextmini_datapath:latest
[INFO] Docker images built successfully
[INFO] Deploying NextMini stack...
Ignoring unsupported options: build, privileged

Since --detach=false was not specified, tasks will be created in the background.
In a future release, --detach=false will become the default.
Updating service nextmini-stack_controller (id: l4n0ej2ze3v7pe7q8pd2lrri4)
image nextmini_controller:latest could not be accessed on a registry to record
its digest. Each node will access nextmini_controller:latest independently,
possibly leading to different nodes running different
versions of the image.

Updating service nextmini-stack_dataplane-node (id: 9ngpjyog7iycjsptl31h4rz5s)
image nextmini_datapath:latest could not be accessed on a registry to record
its digest. Each node will access nextmini_datapath:latest independently,
possibly leading to different nodes running different
versions of the image.

Updating service nextmini-stack_postgres (id: q6ce26kpovkq5dlneq6x61jfx)
[INFO] Stack deployment initiated. Waiting for services to be ready...
[INFO] Waiting for core services... (attempt 1/30)
[INFO] Core services (postgres, controller) are running.
[INFO] Deployment completed successfully!

[INFO] To add worker nodes to this swarm, run the following command on each additional VM:

docker swarm join --token SWMTKN-1-52beoz0gswy6riqagub350b53z3oj8epwutmmcpqvepmqe74ru-4tcainvbcgnqxuw3oea6t0imq 192.168.196.13:2377

[INFO] After joining, label each worker node from the manager by running:
[WARN] docker node update --label-add role=worker <WORKER_NODE_ID>

[INFO] To check service status:
docker stack services nextmini-stack

[INFO] To scale dataplane nodes (if not using global mode):
docker service scale nextmini-stack_dataplane-node=<number>

[INFO] To view service logs:
docker service logs nextmini-stack_controller
docker service logs nextmini-stack_dataplane-node
```

SSH into another VM, then on the second VM:

```bash
ubuntu@mini2:~$ sudo docker swarm join --token SWMTKN-1-52beoz0gswy6riqagub350b53z3oj8epwutmmcpqvepmqe74ru-4tcainvbcgnqxuw3oea6t0i
mq 192.168.196.13:2377
```

```bash
git clone https://github.com/iQua/nextmini
```

Logging:

```text
This node joined a swarm as a worker.
```