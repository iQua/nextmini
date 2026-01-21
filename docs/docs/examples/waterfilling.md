# Water-filling routing (experimental)

!!! warning
    These instructions are not routinely tested. Expect to tweak timeouts and ports on your machine.

This example demonstrates runtime route adaptation using live measurements. It starts a 4‑node Nextmini topology and configures **three parallel paths** from `node1` (source) to `node2` (destination): `1→2`, `1→3→2`, and `1→4→2`. Link rates are capped at **10/20/30 Mbps** so the water-filling controller can converge to a stable split.

## 1) Start the stack

```bash
cd examples/routing/waterfilling
docker compose build && docker compose up
```

Port `5432` is the default for PostgreSQL. On macOS, a locally running Postgres instance can conflict with the container; stop it if needed.

## 2) Generate workload (UDP iperf3)

This example uses UDP because iperf can enforce a target rate.

Recommended: use the helper script (starts 6 UDP flows):

```bash
./start_traffic.sh
```

Manual option (separate terminals):

Start iperf3 servers on `node2`:

```bash
docker exec -it node2 /bin/bash -c "./iperf3_s.sh"
```

Start iperf3 clients on `node1`:

```bash
docker exec -it node1 /bin/bash -c "./iperf3_c.sh"
```

## 3) Monitor throughput

Run the monitoring dashboard:

```bash
cd tools/monitor
uv run dashboard.py
```

## 4) Run the water-filling controller

```bash
uv sync
uv run run_waterfilling.py
```

## Expected outcome

After convergence, you should observe traffic split roughly according to the configured bottlenecks: **10 Mbps**, **20 Mbps**, and **30 Mbps** across the three paths.

## Caveat

Some runs may converge to a suboptimal split (for example 10/20/10). If that happens, restart the experiment and rerun the controller.
