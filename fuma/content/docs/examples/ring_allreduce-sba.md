---
title: "Distributed ring all-reduce on Sim, Boston and Arbutus"
description: ""
---

# Distributed ring all-reduce on Sim, Boston and Arbutus

Before starting, make sure all the containers are stopped and removed.

```bash
docker rm -f $(docker ps -aq)
```

And remove all the Nextmini related networks, for example, `nextmini_network`.

```bash
docker network rm nextmini_network
```

Before running this example, at least three linux machines (or virtual machine instances) need to be set up with Ubuntu 24.04, including one controller instance, one Docker Swarm manager, and multiple worker instances. Docker needs to be pre-installed with `sudo` privileges. It is suggested that the docker directory is moved out of root which usually has small disk partition. You can refer the `Step 2` in `nextmini/examples/arbutus/readme.md` for guides towards setting up docker properly.

### Step 1

On the controller instance, build controller and postgres image:

```bash
cd nextmini/examples/sba-swarm
docker build -t nextmini_controller_pytorch -f ../../controller/Dockerfile ../../
docker pull postgres:alpine
```

Then, add the following to the `controller-config.toml` to ensure successful connection to controller:

```toml
[db]
user = "pgusr"
password = "pgpwrd"
host = "postgres"
database = "nextmini"
port = "5432"
```

Controller and postgres services can be started by:

```bash
docker compose -f controller-swarm.yml build; docker compose -f controller-swarm.yml up
# docker compose -f controller-swarm.yml build --no-cache; docker compose -f controller-swarm.yml up
```

#### Monitoring Dashboard

To monitor network flows and system status, you can start the dashboard on the controller instance:

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Navigate to the monitor directory and run the dashboard
cd nextmini/tools/monitor
uv run dashboard.py
```

On the manager instance, `<CONTROLLER_IP>` in `dataplane-swarm.yml` should be altered accordingly.

### Step 2

Build the sba-swarm base image on all manager and work instances :

```bash
cd nextmini/
docker build -t nextmini_datapath_pytorch -f ./examples/sba-swarm/Dockerfile .
# docker build --no-cache --pull -t nextmini_datapath_pytorch -f ./examples/sba-swarm/Dockerfile .
```

### Step 3

On the manager instance, start the docker swarm:

```bash
docker swarm init --advertise-addr <Manager IP>
```

On all worker instances, join into the swarm network with the swarm token logged out:

```bash
docker swarm join --token SWMTKN-1-xxxx <SWARM_MANAGER_IP>:<port>
```

On the manager instance, you can check the status of nodes by:

```bash
docker node ls
```

### Step 4

After all workers has joined the swarm, deploy services on the manager instance by:

```bash
cd examples/sba-swarm
docker stack deploy -c dataplane-swarm.yml nextmini
```

To check the status of services, use:
```bash
docker service ls
```

### Step 5

On the manager instance, find the container ID for node1 by:

```bash
docker ps -a
```

```bash
docker exec -it <containerID> /bin/bash
```

After logging into the container, we can enter into the ring-emu directory and run the ring all-reduce launcher.

```bash
cd /var/nextmini/ring-emu && uv run launch_ring.py --ring ring.txt --bin /var/nextmini/ringallreduce --no-copy --remote-dir /var/nextmini/ring-emu --len 1048 --init rank --reps 5 --verify
```

# Ring All-Reduce Launcher

A Python-based SSH launcher for distributed ring all-reduce operations across multiple nodes.

## Usage

### Basic Usage

Launch a ring all-reduce operation across 2 nodes:

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048576 \
  --init rank \
  --reps 10 \
  --verify
```

**Note**
No need of `--no-copy` if you want to copy the binary and ring file to remote nodes. Then no need to mount ring.txt in docker-swarm.yml.

### Command Line Options

- `--ring <FILE>`: Path to ring file with IP:port per line (bind addresses)
- `--bin <PATH>`: Local path to compiled ringallreduce binary
- `--remote-dir <DIR>`: Remote working directory where ring.txt is located (use `/var/nextmini/ring-emu` in Docker containers)
- `--ssh-hosts <FILE>`: Optional file with SSH targets (one per line, user@host) in rank order
- `--ssh-port <PORT>`: SSH port (default: 22)
- `--len <N>`: Tensor length in elements (default: 1024)
- `--init <PATTERN>`: Initialization pattern: rank, ones, or random (default: rank)
- `--reps <N>`: Number of repetitions (default: 1)
- `--verify`: Enable verification after all-reduce
- `--no-copy`: Skip copying binary/ring file (assume already present remotely)
- `--remote-ring-name <NAME>`: Filename for ring file on remote side (default: ring.txt)
- `--strict-host-key-checking`: Enable StrictHostKeyChecking (off by default)

### Ring File Format

The ring file should contain one IP:port per line:

```txt
10.0.0.1:9000
10.0.0.2:9000
```

## Directory Structure

```txt
/var/nextmini/
├── pyproject.toml              # PyTorch dependencies (for training scripts)
├── ringallreduce               # Ring all-reduce binary
├── lenet5.py, gpt2.py, etc.    # Training scripts
└── ring-emu/                   # Ring launcher (isolated environment)
    ├── pyproject.toml          # Minimal config, no external dependencies
    ├── launch_ring.py          # Launcher script
    ├── ring.txt                # Ring topology file
    └── README.md               # This file
```

The `ring-emu` directory has its own `pyproject.toml` with no external dependencies, ensuring clean execution without warnings from parent project dependencies.

## Examples

All examples assume you're in the `ring-emu` directory:

```bash
cd /var/nextmini/ring-emu
```

### Small test with verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048 \
  --init rank \
  --reps 5 \
  --verify
```

### Large tensor without verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini/ring-emu \
  --len 10485760 \
  --init ones \
  --reps 100
```

### Using different SSH hosts

If your ring bind addresses differ from SSH endpoints:

```bash
uv run launch_ring.py \
  --ring ring_bind.txt \
  --ssh-hosts ssh_hosts.txt \
  --bin /var/nextmini/ringallreduce \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048576 \
  --verify
```

### Quick one-liner for testing

```bash
cd /var/nextmini/ring-emu && uv run launch_ring.py --ring ring.txt --bin /var/nextmini/ringallreduce --no-copy --remote-dir /var/nextmini/ring-emu --len 1048 --init rank --reps 5 --verify
```

## Troubleshooting

### Port already in use

If you see "Address in use" errors, kill existing processes:

```bash
pkill -9 ringallreduce
ssh 10.0.0.2 "pkill -9 ringallreduce"
```

### SSH connection issues

Ensure SSH keys are properly configured and nodes are reachable:

```bash
ssh-keyscan -H 10.0.0.1 >> ~/.ssh/known_hosts
ssh-keyscan -H 10.0.0.2 >> ~/.ssh/known_hosts
```

### Binary not found

Make sure the binary exists and is executable:

```bash
ls -la /var/nextmini/ringallreduce
chmod +x /var/nextmini/ringallreduce
```

### Ring file not found

If you see "failed to read ring file" errors, ensure you're using the correct `--remote-dir`:

```bash
# Correct: points to ring-emu directory where ring.txt is located
--remote-dir /var/nextmini/ring-emu

# Incorrect: ring.txt is not in /var/nextmini directly
--remote-dir /var/nextmini
```

### Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & db VM instance in DigitalOcean, use the command:

```bash
docker compose -f controller-swarm.yml down
```
