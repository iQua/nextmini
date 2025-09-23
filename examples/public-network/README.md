# Public Network Deployment Example

This example demonstrates how to deploy NextMini across multiple VMs using public IP addresses without Docker Swarm. 

Unlike the `multi-dc` example which uses Docker Swarm for automatic deployment, this approach requires manual configuration.

## Overview

### Architecture Requirements
- Minimum 3 VMs: 
  - 1 VM for controller and PostgreSQL database
  - 1 VM for `node1`
  - 1 VM for `node2`

## How Public/Private Network Selection Works

The controller automatically determines whether to use private or public addresses based on the `private_network_name` configuration:

```rust
// From controller/src/main.rs
let addr = if node.private_network_name == Some(private_network_name.clone()) {
    node.private_network_addr  // Same private network -> use private IP
} else {
    node.public_network_addr   // Different networks -> use public IP
};
```

**Key Point**: Nodes with different `private_network_name` values will communicate via public IP addresses.

## Prerequisites

### Required Files by VM

Controller VM:
- `controller-config.toml`
- `controller-docker-compose.yml`

Node1 VM:
- `node1-config.toml`
- `node1-docker-compose.yml`

Node2 VM:
- `node2-config.toml`
- `node2-docker-compose.yml`

### Network Configuration Requirements

**Host Network Mode**: All node configurations must use `network_mode: host`

```yaml
network_mode: host
```

> **Critical**: This allows containers to see the host's real network interfaces. Bridge mode would expose Docker-internal IPs, causing connection failures.


## Deployment Steps

### Step 1: Prepare VM IP Addresses

On each VM, find the public IP address:
```bash
ifconfig
```

### Step 2: Configure Controller Connection

In each `node*-docker-compose.yml` file, update the controller URL:
```yaml
command: /bin/bash -c "sleep 7 && /var/nextmini/nextmini ws://<controller_public_ip>:3000"
```

Replace `<controller_public_ip>` with the actual controller VM's public IP address.

### Step 3: Deploy Services

**On Controller VM:**
```bash
cd examples/public-network
docker compose -f controller-docker-compose.yml build
docker compose -f controller-docker-compose.yml up
```

**On Node1 VM:**
```bash
cd examples/public-network
docker compose -f node1-docker-compose.yml build
docker compose -f node1-docker-compose.yml up
```

**On Node2 VM:**
```bash
cd examples/public-network
docker compose -f node2-docker-compose.yml build
docker compose -f node2-docker-compose.yml up
```

## Arbutus Cloud Specific Setup

### Network Interface Configuration

On Arbutus, the `ens3` interface typically provides access to the `192.168.x.x` private network. Configure as:
- Set `public_network_interface = "ens3":

```toml
node_id = 2
public_network_interface = "ens3"
private_network_name = "vm2-private"
private_network_interface = "ens3"

num_tun_queues = 1
num_packet_processors = 1
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"
```

### Docker Installation and Setup on Arbutus

To install Docker:
```bash
sudo apt update
sudo apt install docker.io docker-compose -y
```

Arbutus instances typically have really small disk space on the root partition. To avoid running out of disk space, we can move the docker root directory to `/mnt`. We need to first stop the docker service with the following:

```bash
sudo systemctl stop docker
```

Then, we need to create a new folder for the docker root directory under.

```bash
sudo mkdir -p /mnt/docker
```

We can now edit the docker service file the change the docker root directory. To do so, open the docker daemon file with a text editor such as vi or nano,

```bash
sudo vi /etc/docker/daemon.json
```

Then edit the file to look like the following:

```bash
{
    "data-root": "/mnt/docker"
}
```

Finally, we can start the docker service again with the following command:

```bash
sudo systemctl start docker
```

We can test the root directory has been changed by running `docker info` and checking the `Docker Root Dir` field.

```bash
sudo docker info -f '{{.DockerRootDir}}'
```
It should show something like `/mnt/docker`.

Having to type `sudo` every time we run a docker command can be annoying. To avoid this, we can add the current user to the docker group with the following command:

```bash
sudo usermod -aG docker $USER
```

Log out and log back in to apply the changes.
