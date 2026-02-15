---
title: "Smoltcp Test Example"
description: "Exercise weighted flows and link-rate limits in a three-node local Docker topology."
---

This scenario runs three dataplane nodes and uses controller-defined flows to test scheduling behavior. `examples/smoltcp-test/controller-config.toml` sets `scheduler_type = "wrr"`, applies a link rate from node 1 to node 2, and defines two long flows from node 1 to node 2 with weights `1` and `2`.

From the repository root:

```bash
cd examples/smoltcp-test
docker compose build
docker compose up
```

Check that all services are healthy and all dataplane nodes are registered:

```bash
docker compose ps
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS nodes_connected FROM nodes;"
```

The expected node count is `3`.

To confirm the test inputs before or after a run:

```bash
rg -n "scheduler_type|flow_weight|rate =|bucket_size" controller-config.toml
```

You can also confirm basic reachability between the two flow endpoints:

```bash
docker exec node1 ping -c 3 10.0.0.2
```

Stop the containers when done:

```bash
docker compose down
```
