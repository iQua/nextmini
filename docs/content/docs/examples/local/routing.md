---
title: "Routing Slot-Matching Example"
description: "Documents the slotmatching routing assets used to define multi-path candidates and run parallel iperf3 verification flows."
---

The `examples/routing/slotmatching` folder is a routing-focused fixture. It contains:

- `controller-config.toml` with explicit multi-path candidates and link rates
- `k_path.py` to generate `routes.json` for all source/destination pairs in a 6-node graph
- `iperf3_s.sh` and `iperf3_c.sh` to validate parallel UDP flows over predefined ports
- `docker-compose.yml` that launches the controller in host network mode

The runtime pattern is: generate or inspect route candidates, start the controller with the slotmatching config, then run server/client iperf traffic on participating dataplane nodes.

For controller-managed custom route definitions in the regular examples harness, see [Routing Examples](/docs/examples/local/routes).

The compose file in this folder starts only the controller. It does not start Postgres or dataplane services, so you need those running separately in your environment before starting traffic.

## Run

From the repository root:

```bash
./utils/start-database.sh
```

```bash
cd examples/routing/slotmatching
python k_path.py
```

Start the controller stack:

```bash
cd examples/routing/slotmatching
docker compose up --build
```

In the destination-node shell, start all six UDP servers:

```bash
cd examples/routing/slotmatching
bash iperf3_s.sh
```

In the source-node shell, launch the six matching UDP clients:

```bash
cd examples/routing/slotmatching
bash iperf3_c.sh
```

## Verification

Confirm that route generation produced entries:

```bash
cd examples/routing/slotmatching
jq 'length' routes.json
```

The value should be greater than `0`.

On the destination node, verify all expected UDP ports are listening:

```bash
ss -lun | rg "5201|5202|5203|5204|5205|5206"
```

On the source node, `iperf3_c.sh` should print transfer summaries for all six client streams. Controller logs should remain healthy while flows are active:

```bash
cd examples/routing/slotmatching
docker compose logs -f controller
```

## Cleanup

Stop iperf processes on the dataplane nodes:

```bash
pkill -f "iperf3 -s -p 520"
pkill -f "iperf3 -c 10.0.0.2"
```

Stop the controller container:

```bash
cd examples/routing/slotmatching
docker compose down
```
