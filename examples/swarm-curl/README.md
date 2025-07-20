# External client and server guide

You should first follow the `./multi-dc/README.md` to deploy basic `Nextmini` across multiple instances.

After that you should enter `swarm-curl` directory with:

```bash
cd nextmini/examples/swarm-curl
```

## Building Docker Images

Before deploying the services, build the required docker images on your target server and client instances:

**Build Server Image**

```bash
docker build -t curl-server -f src/Dockerfile.server .
```

**Build Client Image**

```bash
docker build -t curl-client -f src/Dockerfile.client .
```

## Deploying Services

You can change the `constraint` to your target nodes in this step.

Deploy the server service first.

You could enter the following command in manager instance.

```bash
docker service create \
  --name curl-server \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --publish 8080:8080 \
  --constraint 'node.hostname==ubuntu-sf' \
  --restart-condition on-failure \
  --restart-delay 5s \
  --restart-max-attempts 3 \
  curl-server:latest
```

Deploy the client service:

```bash
docker service create \
  --name curl-client \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --constraint 'node.hostname==ubuntu-gm' \
  --restart-condition on-failure \
  --restart-delay 10s \
  --restart-max-attempts 5 \
  curl-client:latest
```

## Insert Route

After deploying the services, you should insert routes into the database:

First, make sure `uv` is installed. If not, you can install it with:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.profile # Or open a new terminal for the PATH to take effect
```

Then, run the script:
```bash
cd route
uv run insert_route.py
```

---

Test instructions:

In this experiment, we take the `ubuntu-gm` instance as the client and the `ubuntu-sf` instance as the server. 
And mini-cc in Arbutus as the separate controller/db instance. 

Firstly, we need to build controller and db image in arbutus instance:

```bash
ssh ubuntu@206.12.91.13 # the real ip addr of mini-cc instance 
cd examples/multi-dc
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
```

Then start controller to listen on 3000 and 5432 ports:
```bash
docker-compose -f controller-standalone.yml up
```

Then ssh into ubuntu-syd instance in Sydney, and we take this as a manager instance with docker swarm.

```bash
ssh root@170.64.203.108
```

And open a terminal and enter:
```bash
cd nextmini/examples
# always use `leave` before init as if it's already part of a swarm node.
docker swarm leave -f
docker swarm init --advertise-addr 170.64.203.108
```

Then remember the token like below which will be used for other worker nodes to join(can't be used in controller or client/server instances):
```bash
docker swarm join --token SWMTKN-1-1ro2sbe6cy2t4lwt4cyphvfh54ryitw6iqmdbj3zn3nfvg7gyu-3sc32cc2pn8wtg4676m2b3aq1 170.64.203.108:2377
```

Open the termainl in atl and atl-02; then enter:
```bash
docker swarm join --token SWMTKN-1-1ro2sbe6cy2t4lwt4cyphvfh54ryitw6iqmdbj3zn3nfvg7gyu-3sc32cc2pn8wtg4676m2b3aq1 170.64.203.108:2377
```

In this case, syd instance works as the manager istance, atl and atl-02 work as the worker instances. 

Use `docker node ls` in syd instance:

```text
root@ubuntu-syd:~/nextmini# docker node ls
ID                            HOSTNAME        STATUS    AVAILABILITY   MANAGER STATUS   ENGINE VERSION
0n6xg7qcnwa1pd7xox64r1s24     ubuntu-atl      Ready     Active                          27.5.1
8it0ynjroc01y1i2sdl6lde7r     ubuntu-atl-02   Ready     Active                          27.5.1
xjxmwq64p60jzr1u0ooq3q3ae *   ubuntu-syd      Ready     Active         Leader           27.5.1
```

For simplicity, we change dataplane node replica into 3, which means the docker swarm will automatically assign three dataplane nodes into three instance evenly.

```bash
# ssh into manager instance
ssh root@170.64.203.108
cd examples/multi-dc
# change replicas into :3
```

Accordingly, we need to ssh into mini-cc arbutus instance as we only need three nodes:
```bash
cd examples/multi-dc
nvim controller=config.toml

# press `i` to edit: change n_nodes to 3
# press `esc` then `:wq!`
```

Next step is to build server and client image on the `ubuntu-sf` (server) and `ubuntu-gm` (client) nodes.

**Important**: Make sure `ubuntu-sf` and `ubuntu-gm` have joined the Swarm cluster as worker nodes.

```bash
# On the ubuntu-sf node (the server)
ssh root@<IP_OF_UBUNTU_SF>
cd nextmini/examples/swarm-curl
docker build -t curl-server -f src/Dockerfile.server .
```

```bash
# On the ubuntu-gm node (the client)
ssh root@<IP_OF_UBUNTU_GM>
cd nextmini/examples/swarm-curl
docker build -t curl-client -f src/Dockerfile.client .
```

We could ssh into syd to prepare for deployment. 
The controller addr is `206.12.91.13`

Then use in syd instance:
```bash
cd examples/multi-dc
sed 's/REPLACE_WITH_MANAGER_IP/206.12.91.13/g' dataplane-swarm.yml > dataplane-deploy.yml
```

After this, you could see a new file named `/root/nextmini/examples/multi-dc/dataplane-deploy.yml` is generated.

After server and client are successfully built in German and London instance,

enter the below command in syd instance:
```bash
cd examples/multi-dc
docker stack deploy -c dataplane-deploy.yml nextmini
``` 

You could see the following:
```text
root@ubuntu-syd:~/nextmini/examples/multi-dc# docker stack deploy -c dataplane-deploy.yml nextmini
Ignoring unsupported options: build, privileged

Since --detach=false was not specified, tasks will be created in the background.
In a future release, --detach=false will become the default.
Creating network nextmini_nextmini-net
Creating service nextmini_dataplane
```

Then enter:
```bash
docker service create \
  --name curl-server \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --publish 8080:8080 \
  --constraint 'node.hostname==ubuntu-sf' \
  --restart-condition on-failure \
  --restart-delay 5s \
  --restart-max-attempts 3 \
  curl-server:latest
```

and 

```bash
docker service create \
  --name curl-client \
  --replicas 1 \
  --network nextmini_nextmini-net \
  --constraint 'node.hostname==ubuntu-gm' \
  --restart-condition on-failure \
  --restart-delay 10s \
  --restart-max-attempts 5 \
  curl-client:latest
```

Only other three DCs should serve as dataplane node.
```bash
docker node update --label-add type=dataplane ubuntu-atl
docker node update --label-add type=dataplane ubuntu-atl-02
docker node update --label-add type=dataplane ubuntu-syd
```

Then we could use:
```bash
docker service logs curl-client 
docker service logs curl-server
```
in Sydney DC to see if it's successful.
To clean up,

```bash
docker service rm curl-client curl-server; docker stack rm nextmini
```

```bash
docker service logs curl-client
docker service logs curl-server
docker service logs nextmini_dataplane
```
```bash
docker service update --args "/bin/sh -c 'sleep 30; exec curl http://curl-server:8080/'" curl-client
```