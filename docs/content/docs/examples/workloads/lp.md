---
title: "LP Multicast Tree Example"
description: "Runs LP-based multicast tree selection and applies the result in a reproducible three-node toy deployment."
---

The `examples/lp` folder contains the multicast optimization logic used to compute directed multicast trees from a controller topology. The quickest way to run it end to end is the toy harness in `examples/lp/toy`, which starts one source node and two receivers.

At runtime, the source node waits for receiver handshakes, creates a multicast group, optionally probes link rates through the controller database, solves the LP with mFlow, converts the chosen tree to DAG edges, installs those edges with `set_group_routes()`, and sends a lossless payload to both receivers.

## Run

From the repository root:

```bash
cd examples/lp/toy
docker compose up --build
```

If you want to inspect only the solver output against the toy controller config (without launching containers), run this from the repository root:

```bash
uv venv --python 3.13
source .venv/bin/activate
uv pip install numpy cvxopt "psycopg[binary]"

python -m examples.lp.main \
  --controller-config examples/lp/toy/controller-config.toml \
  --src 1 \
  --dests 2,3
```

## Verification

Follow source and receiver logs:

```bash
cd examples/lp/toy
docker compose logs -f node1 node2 node3
```

A successful run should include:

- source: `group created`, `conversion solver=mflow`, and `send done ok=True`
- each receiver: `joined group ... and READY`, then `recv ok=True` with the expected payload

You can also confirm group membership state in Postgres:

```bash
cd examples/lp/toy
docker compose exec postgres psql -U pgusr -d nextmini -c "SELECT group_id, src_node_id FROM groups;"
docker compose exec postgres psql -U pgusr -d nextmini -c "SELECT group_id, member_node_id FROM group_members ORDER BY member_node_id;"
```

## Cleanup

```bash
cd examples/lp/toy
docker compose down --volumes
```
