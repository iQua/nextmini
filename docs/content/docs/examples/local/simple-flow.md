---
title: "Simple Flow Example"
description: "Run controller-defined flows across four local dataplane containers on one host."
---

This example runs a local Docker topology with four Nextmini dataplane nodes (`node1` to `node4`), one controller, and Postgres. The behavior is defined in `examples/simple-flow/controller-config.toml`: TCP transport, FIFO scheduler, `flow_transport = "lossless_unicast"`, and three controller-driven flows from node 1 to node 2.

From the repository root:

```bash
cd examples/simple-flow
docker compose build
docker compose up
```

When startup settles, verify that all services are running:

```bash
docker compose ps
```

Check that the controller has registered all four dataplane nodes:

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS nodes_connected FROM nodes;"
```

The expected count is `4`.

You can also verify dataplane addressing from the default `10.0.0.0/16` assignment:

```bash
docker exec node1 ip addr show | rg "10\\.0\\.0\\.1"
docker exec node2 ip addr show | rg "10\\.0\\.0\\.2"
```

Stop the example when finished:

```bash
docker compose down
```
