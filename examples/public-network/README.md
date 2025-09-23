We already have a `multi-dc` example for automatically deploying controller/postgre and dataplane nodes with docker swarm, which can be implemented with a few commands.

With docker swarm, we don't need to assign public addresses manually or configure toml files for each specific node.

In this example, we use the non-Swarm approach (no Docker Swarm) to connect the controller and dataplane nodes.

Use `ifconfig` on each VM to find the public IP used for TCP persistent connections between nodes and for connecting to the controller.

These instructions assume at least three VMs: one for the controller and PostgreSQL database, one for `node1`, and one for `node2`.

On the controller VM, only the following files are needed:

- controller-config.toml
- docker-compose.yml

On `node1`, only `examples/public-network/node1-config.toml` and `examples/public-network/node1-docker-compose.yml` are needed. Do the same for `node2` on its VM.

It is best to keep only the required files.

First, use `ifconfig` to find the controller VM's IP address; this will be the controller's public address. 

In node[idx]-docker-compose.yml file, substitute 

```
command: /bin/bash -c "sleep 7 && /var/nextmini/nextmini ws://<controller_public_ip>:3000"
```
with the real controller ip addr.

Open another terminal, and enter:

```bash
cd examples/public-network
docker compose build; docker compose up
```

In the second VM, open a new terminal:

```bash
cd examples/public-network
docker compose build; docker compose up
```

In node config file, the network mode must be set to host.

```bash
network_mode: host
```

This is very critical.

# Arbutus

In arbutus, "ens3" is the network_interface for private network `192.168.x.x`.

To test on arbutus,

## Install docker 

Firstly, we need to install docker in a new VM in arbutus.

```bash
sudo apt update && sudo apt install docker.io -y && sudo apt install docker-compose -y
```

## Set up arbutus

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

![image](./imgs/image14.png)

Having to type `sudo` every time we run a docker command can be annoying. To avoid this, we can add the current user to the docker group with the following command:

```bash
sudo usermod -aG docker $USER
```

Log out and log back in to apply the changes.

