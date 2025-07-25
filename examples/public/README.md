We already have a `multi-dc` example for automatically deploying controller/postgre and dataplane nodes with docker swarm, which can be implemented with a few commands.

With docker swarm, we don't need to assign public addresses manually or configure toml files for each specific node.

Now we use `ifconfig` in each VM instance to find the public address used for TCP persistent connections between two nodes and for connecting the controller.

The following instructions assume you've already created two VM instances. One for `controller & postgre database & node1` and the other for `node2`.

In the first VM instance which will be used for `controller & postgre database & node1`, only the following files should be used:

- controller-config.toml
- docker-compose.yml
- node1-config.toml

It's better to keep only the needed files.

First, use `ifconfig` to find the IP address of the first VM, which will be the public address of the controller.

Open another terminal, and enter:

```bash
cd examples/public
docker compose build; docker compose up
```

In the second VM, open a new terminal:

```bash
cd examples/public
docker compose build; docker compose up
```