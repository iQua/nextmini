# External client and server guide

You should first follow the `./multi-dc/README.md` to deploy basic `Nextmini` across multiple instances. Importantly, you need to replace the `dataplane-swarm.yml` file in `multi-dc` file with the one in current folder.

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

```bash
docker service create \
  --name swarm-splice-server \
  --replicas 1 \
  --network nextmini-net \
  --publish 8080:8080 \
  --constraint 'node.role==worker' \
  --restart-condition on-failure \
  --restart-delay 5s \
  --restart-max-attempts 3 \
  swarm-splice-server:latest
```

Deploy the client service:

```bash
docker service create \
  --name swarm-splice-client \
  --replicas 1 \
  --network nextmini-net \
  --constraint 'node.role==worker' \
  --restart-condition on-failure \
  --restart-delay 10s \
  --restart-max-attempts 5 \
  swarm-splice-client:latest
```

## Insert Route

After deploying the services, you should cd into a dataplane node on the manager instance and insert routes into the database:

```bash
cd /var/nextmini/tools/route
uv run insert_route.py
```
