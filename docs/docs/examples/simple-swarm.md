# Docker Swarm deployment (simple-swarm)

`examples/simple-swarm` deploys Nextmini across multiple machines using Docker Swarm.

## Prerequisites

- Ubuntu 24.04 (or similar)
- Docker installed on every machine
- SSH access between machines (for setup/debugging)

## Steps (high level)

1. On every node, build the dataplane image:

   ```bash
   cd examples/simple-swarm
   docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
   ```

2. On the manager, initialize the swarm:

   ```bash
   docker swarm init --advertise-addr <MANAGER-IP>
   ```

3. On each worker, join using the token printed by the manager:

   ```bash
   docker swarm join --token <WORKER-TOKEN> <MANAGER-IP>:2377
   ```

4. On the manager, deploy the stack:

   ```bash
   cd examples/simple-swarm
   docker stack deploy -c docker-compose.swarm.yml nextmini
   ```

## Debugging

- List services: `docker stack services nextmini`
- View logs: `docker service logs nextmini_controller` / `nextmini_dataplane` / `nextmini_postgres`
- Exec into a dataplane task: `docker exec -it <container_id> bash`

Remember to update `controller-config.toml` when you change the dataplane replica count.

