---
title: "Nextmini Hybrid Deployment: Local Server + Fly.io"
description: ""
---


This deployment configuration runs the **Controller and PostgreSQL locally** (on your physical/virtual server) while deploying **Dataplane nodes to Fly.io**.

## Architecture

```
Local Server (like 206.12.89.244)       Fly.io (edge)
┌──────────────────┐                ┌──────────────┐
│ Controller :3000 │◄───WebSocket───┤ Node 1       │
│ PostgreSQL :5432 │   (IPv4)       │ [fdaa::]:8080│
└──────────────────┘                └──────┬───────┘
                                           │ IPv6
                                    ┌──────┴───────┐
                                    │ Node 2       │
                                    │ [fdaa::]:8080│
                                    └──────────────┘
```

- **Node → Controller**: IPv4 (public internet WebSocket)
- **Node ↔ Node**: IPv6 (Fly.io private network)

## Quick Start

### 1. Start Local Controller

```bash
cd /home/ubuntu/nextmini/examples/localserver-flyio
uv run start-controller.py
```

Expected output:
```
Public IP: 206.12.89.244
Starting PostgreSQL and Controller...
   Controller is running!
   WebSocket: ws://206.12.89.244:3000
```

### 2. Deploy Fly.io Nodes

```bash
# Deploy 2 nodes
uv run deploy-flyio.py --public-ip 206.12.89.244 --nodes 2
```

### 3. Verify Connection

```bash
# Check controller logs
docker compose logs controller | tail -20

# Check node status
flyctl status -a nextmini-node-1
flyctl status -a nextmini-node-2

# Check node logs
flyctl logs -a nextmini-node-1 -n
```

## Testing

### SSH into Nodes

```bash
# Enter Node 1
flyctl ssh console -a nextmini-node-1

# Enter Node 2
flyctl ssh console -a nextmini-node-2
```

### Inside Node Testing

```bash
# View network interfaces and IPv6 addresses
ip addr show eth0

# View routing table
ip route

# View listening ports
netstat -tlnp

# View TUN device
ip addr show nextmini

# Test overlay network (10.0.0.x)
# From Node 1 ping Node 2
ping -c 3 10.0.0.2

# From Node 2 ping Node 1
ping -c 3 10.0.0.1
```

### Performance Testing (iperf3)

```bash
# Start iperf3 server on Node 1
iperf3 -s -B 10.0.0.1

# Run client on Node 2
iperf3 -c 10.0.0.1 -B 10.0.0.2 -t 10
```

## Common Commands

### Controller Management

```bash
# Check status
docker compose ps

# View logs
docker compose logs -f controller

# Restart controller
docker compose restart controller

# Stop services
docker compose down
```

### Node Management

```bash
# List all nodes
flyctl apps list | grep nextmini

# Check node status
flyctl status -a nextmini-node-1

# View node logs
flyctl logs -a nextmini-node-1 -n

# SSH into node
flyctl ssh console -a nextmini-node-1

# Restart node
MACHINE_ID=$(flyctl machine list -a nextmini-node-1 -q | head -1)
flyctl machine restart $MACHINE_ID -a nextmini-node-1

# Destroy node
flyctl apps destroy nextmini-node-1
```
