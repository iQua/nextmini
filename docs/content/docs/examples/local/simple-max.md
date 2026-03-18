---
title: "Simple Max Mode Example"
description: "Run a three-node local path with a Max-mode ingress node and a controller-defined long flow."
---

This example sets up a three-node dataplane on one host. In `examples/simple-max/controller-config.toml`, node 1 is configured with `operating_mode = "max"`, the route is pinned to `[1, 2, 3]`, and a 1 GB flow is defined from node 1 to node 3. 

In the default toml configuration; node 2 flow tables aren't set, node 2 will ignore direct tcp/ip connections, but will act as a relay for nextmini. Customize `controller-config.toml` topology as required for your use case.

From the repository root:

```bash
cd examples/simple-max
docker compose build
docker compose up
```

Confirm the topology is up:

```bash
docker compose ps
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS nodes_connected FROM nodes;"
```

The expected node count is `3`.

Verify the path can carry traffic end to end:

```bash
docker exec node1 ping -c 3 10.0.0.3
```

If you want to confirm the active Max-mode setting directly from the example config:

```bash
rg -n "operating_mode|route =|flow_len" controller-config.toml
```

Stop containers when finished:

```bash
docker compose down
```
