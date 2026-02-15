---
title: "Simple Scheduler Example"
description: "Run three local dataplane nodes with QUIC control-plane transport and test concurrent iperf3 streams."
---

This example runs three dataplane nodes with `protocol = "quic"` and `scheduler_type = "fifo"` from `examples/simple-scheduler/controller-config.toml`. The included helper scripts start three UDP `iperf3` server ports on `node3` and launch three matching clients on `node1`.

From the repository root:

```bash
cd examples/simple-scheduler
docker compose build
docker compose up
```

In a second terminal, start the three `iperf3` servers on `node3`:

```bash
cd examples/simple-scheduler
docker exec -it node3 /bin/bash -lc "/var/node/iperf3_s.sh"
```

In a third terminal, start the three clients on `node1`:

```bash
cd examples/simple-scheduler
docker exec -it node1 /bin/bash -lc "/var/nextmini/iperf3_c.sh"
```

Verify that `node3` is listening on all three UDP ports:

```bash
docker exec node3 ss -lun | rg "5201|5202|5203"
```

You can also confirm all dataplane nodes connected:

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS nodes_connected FROM nodes;"
```

The expected count is `3`.

Shut down the example:

```bash
docker compose down
```
