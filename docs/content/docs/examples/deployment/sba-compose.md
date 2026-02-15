---
title: "SBA Compose Deployment"
description: "Deploy Nextmini across Sim/Boston/Arbutus-style VMs with host-network dataplanes and explicit controller public-IP wiring."
---

Use `examples/sba-compose/` when you want a manual, per-VM Compose deployment across separate public-networked machines.

## VM Role Split

| VM role | Runs | Files on that VM |
| --- | --- | --- |
| Controller VM (for example Arbutus) | `controller` + `postgres` | `controller-docker-compose.yml`, `controller-config.toml` |
| Node 1 VM (for example Sim) | `node1` dataplane | `node1-docker-compose.yml`, `node1-config.toml` |
| Node 2 VM (for example Boston) | `node2` dataplane | `node2-docker-compose.yml`, `node2-config.toml` |

## Host Networking Requirement

Both node compose files use:

```yaml
network_mode: host
```

This is required because the dataplane config references host NIC names such as `enp0s31f6` and `eno2`. Bridge networking hides those host interfaces and breaks public-interface binding.

## Controller Address Wiring

`node1-docker-compose.yml` and `node2-docker-compose.yml` launch dataplanes with a websocket endpoint:

```bash
/var/nextmini/nextmini ws://206.12.89.244:3000
```

If your controller VM IP is different, rewrite both files before startup.

## Startup Order (Reproducible)

### 1) Controller VM

```bash
cd ~/nextmini/examples/sba-compose
docker compose -f controller-docker-compose.yml up -d --build
docker compose -f controller-docker-compose.yml ps
```

### 2) Node 1 VM

```bash
cd ~/nextmini/examples/sba-compose
export CONTROLLER_IP=<controller_public_ip>
sed -i "s#ws://[^:]*:3000#ws://${CONTROLLER_IP}:3000#g" node1-docker-compose.yml
grep -n "ws://" node1-docker-compose.yml
docker compose -f node1-docker-compose.yml up -d --build
docker compose -f node1-docker-compose.yml ps
```

### 3) Node 2 VM

```bash
cd ~/nextmini/examples/sba-compose
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
cd ~/nextmini/examples/sba-compose
docker compose -f node1-docker-compose.yml down --remove-orphans
```

### Node 2 VM

```bash
cd ~/nextmini/examples/sba-compose
docker compose -f node2-docker-compose.yml down --remove-orphans
```

### Controller VM

```bash
cd ~/nextmini/examples/sba-compose
docker compose -f controller-docker-compose.yml down --remove-orphans
```

For a full reset of controller database data, run the last command with `-v`.
