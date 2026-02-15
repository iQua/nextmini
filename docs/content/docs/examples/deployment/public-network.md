---
title: "Public Network Compose Deployment"
description: "Run Nextmini across separate VMs with per-role Docker Compose files, host networking, and public-IP controller wiring."
---

This example runs Nextmini across three VMs without Docker Swarm, using the assets in `examples/public-network/`.

## VM Role Split

| VM role | Runs | Files on that VM |
| --- | --- | --- |
| Controller VM | `controller` + `postgres` | `controller-docker-compose.yml`, `controller-config.toml` |
| Node 1 VM | `node1` dataplane | `node1-docker-compose.yml`, `node1-config.toml` |
| Node 2 VM | `node2` dataplane | `node2-docker-compose.yml`, `node2-config.toml` |

## Why Host Networking Is Required

`node1` and `node2` use `network_mode: host` so the dataplane can bind to the VM's real interfaces (`public_network_interface`) and public IP (`public_network_addr`) from `node*-config.toml`.

If you switch nodes to Docker bridge networking, containers only see Docker-internal interfaces/IPs and cross-VM wiring will fail.

## Controller Address Wiring

Each node compose file starts the dataplane with:

```bash
/var/nextmini/nextmini ws://<controller_public_ip>:3000
```

Set this to the actual controller VM public IP before starting dataplane nodes.

## Startup Order (Reproducible)

### 1) Controller VM

```bash
cd ~/nextmini/examples/public-network
docker compose -f controller-docker-compose.yml up -d --build
docker compose -f controller-docker-compose.yml ps
```

### 2) Node 1 VM

```bash
cd ~/nextmini/examples/public-network
export CONTROLLER_IP=<controller_public_ip>
sed -i "s#ws://[^:]*:3000#ws://${CONTROLLER_IP}:3000#g" node1-docker-compose.yml
grep -n "ws://" node1-docker-compose.yml
docker compose -f node1-docker-compose.yml up -d --build
docker compose -f node1-docker-compose.yml ps
```

### 3) Node 2 VM

```bash
cd ~/nextmini/examples/public-network
export CONTROLLER_IP=<controller_public_ip>
sed -i "s#ws://[^:]*:3000#ws://${CONTROLLER_IP}:3000#g" node2-docker-compose.yml
grep -n "ws://" node2-docker-compose.yml
docker compose -f node2-docker-compose.yml up -d --build
docker compose -f node2-docker-compose.yml ps
```

### 4) Quick health checks (any VM)

```bash
docker logs --tail 100 controller
docker logs --tail 100 node1
docker logs --tail 100 node2
```

## Cleanup (Reverse Order)

### Node 1 VM

```bash
cd ~/nextmini/examples/public-network
docker compose -f node1-docker-compose.yml down --remove-orphans
```

### Node 2 VM

```bash
cd ~/nextmini/examples/public-network
docker compose -f node2-docker-compose.yml down --remove-orphans
```

### Controller VM

```bash
cd ~/nextmini/examples/public-network
docker compose -f controller-docker-compose.yml down --remove-orphans
```

For a full reset of Postgres state on the controller VM, add `-v` to the final `down` command.
