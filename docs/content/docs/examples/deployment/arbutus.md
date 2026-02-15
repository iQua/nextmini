---
title: "Arbutus Cloud Deployment"
description: "Runbook for deploying Nextmini on Arbutus VMs with Docker, snapshots, and optional node auto-start."
---

This runbook condenses the original Arbutus setup notes into a deployment path you can repeat quickly.

## Prerequisites

- Access to an Arbutus project and network.
- An Ubuntu VM with SSH access (floating IP or gateway access).
- This repository cloned on the VM.

## Operational Constraints

- Arbutus root disks are usually small. Move Docker's data root to `/mnt/docker` before building images.
- The dataplane config assumes `public_network_interface = "ens3"` in `examples/arbutus/config.toml`.
- `examples/arbutus/docker-compose.yml` runs the node in `privileged` mode with host networking.
- Before cloning a prepared VM, make sure the node command points to the controller VM address (not `127.0.0.1` unless controller and node always share the same host).

## Deployment Workflow

### 1. Prepare the first VM

```bash
ssh ubuntu@<floating-ip>
cd ~
git clone <your-nextmini-repo-url> nextmini
cd nextmini/examples/arbutus
sudo apt update
sudo apt install -y docker.io docker-compose net-tools
```

If your default interface is not `ens3`, update `config.toml`:

```bash
ip -o link show | awk -F': ' '{print $2}'
# Edit config.toml and set public_network_interface accordingly.
```

### 2. Relocate Docker root to `/mnt` (critical on Arbutus)

```bash
sudo systemctl stop docker
sudo mkdir -p /mnt/docker
sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "data-root": "/mnt/docker"
}
JSON
sudo systemctl start docker
sudo docker info -f '{{.DockerRootDir}}'
sudo usermod -aG docker "$USER"
```

Log out and back in after adding your user to the Docker group.

### 3. Build images and configure controller address

```bash
cd ~/nextmini/examples/arbutus
docker compose build
```

Set the controller address in `docker-compose.yml` (last command line for the node service):

```bash
CONTROLLER_IP=$(ip -4 -o addr show ens3 | awk '{print $4}' | cut -d/ -f1)
sed -i.bak "s#ws://127.0.0.1:80#ws://${CONTROLLER_IP}:80#" docker-compose.yml
```

### 4. Start controller and node

```bash
cd ~/nextmini/examples/arbutus
# Controller only (sanity check)
docker compose up controller

# Node only (will retry until controller is up)
docker compose up node

# Normal operation
docker compose up
```

### 5. Optional: auto-start node at boot

```bash
cd ~/nextmini/examples/arbutus
sudo cp strato-node.service /etc/systemd/system/
# Update ExecStart/ExecStop path if your repo is not /home/ubuntu/strato
sudo sed -i.bak 's#/home/ubuntu/strato#/home/ubuntu/nextmini#g' /etc/systemd/system/strato-node.service
sudo systemctl daemon-reload
sudo systemctl enable strato-node
sudo systemctl start strato-node
journalctl -u strato-node.service -f
```

### 6. Clone additional nodes from the prepared VM

- In Arbutus UI, create a snapshot from the prepared instance.
- Launch additional instances from that snapshot on the same network.
- New nodes should reconnect automatically to the controller if the controller address was set correctly before snapshotting.

## Verification

```bash
cd ~/nextmini/examples/arbutus
docker compose logs controller --tail=80
docker compose logs node --tail=80
```

Look for node registration and route-install logs on the controller.

## Cleanup Workflow

### Stop runtime services

```bash
cd ~/nextmini/examples/arbutus
docker compose down
sudo systemctl disable --now strato-node || true
```

### Remove optional local artifacts

```bash
sudo rm -f /etc/systemd/system/strato-node.service
sudo systemctl daemon-reload
# Optional, destructive for unused Docker artifacts:
# docker system prune -f
```

### Remove cloud resources

- Delete cloned instances.
- Delete snapshots created for cloning.
- Release/cleanup floating IP associations no longer needed.
