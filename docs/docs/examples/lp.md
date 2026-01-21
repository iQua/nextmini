# LP Multicast Tree Selection

This example computes an optimal multicast DAG (tree) for a given topology and `(src → destinations)` demand using Linear Programming (mFlow).

## Features

- Builds a topology graph from a controller config file
- Optional link probing to measure capacities via the controller DB
- Solves a max-min fair multicast LP
- Converts the LP solution to multicast tree edges and installs them via `nextmini_py.Dataplane.set_group_routes()`

## Where to start

Use the toy demo for a complete end-to-end run:

- [Toy Demo](lp-toy.md)

## Compute edges (CLI)

Compute multicast edges from a controller config:

```bash
python -m examples.lp.main \
  --controller-config <path/to/controller-config.toml> \
  --src 1 \
  --dests 2,3
```

## Apply edges to a live controller (source node only)

If you already created a multicast group and you are running on the **source node**, you can apply an override DAG:

```bash
python -m examples.lp.main \
  --controller-config <path/to/controller-config.toml> \
  --src 1 \
  --dests 2,3 \
  --apply \
  --node-config <path/to/source-node-config.toml> \
  --group-id <group_id>
```

This requires `nextmini_py` to be installed in the Python environment.

## Link probing (optional)

The LP solver can optionally **probe link goodputs** before solving:

- It inserts probe flows into Postgres (`flows.is_probe = true`).
- Dataplane nodes execute those flows as user-space traffic.
- The solver estimates per-link capacity from probe completion times and updates the LP graph.

Enable probing:

```bash
python -m examples.lp.main \
  --controller-config <path/to/controller-config.toml> \
  --src 1 \
  --dests 2,3 \
  --probe-links \
  --probe-bytes 67108864 \
  --probe-timeout-secs 60
```

Notes:

- The probe step requires controller + dataplane nodes to be running and Postgres to be reachable from where you run the script.
- DB settings are read from the controller config `[db]` section (you can override via `NEXTMINI_DB_HOST`, `NEXTMINI_DB_PORT`, `NEXTMINI_DB_USER`, `NEXTMINI_DB_PASSWORD`, `NEXTMINI_DB_NAME`).
- Probes create real load. Run them when you can tolerate temporarily occupying the network.

## How this is used in the repo

- The [LP toy demo](lp-toy.md) runs probing + LP + multicast route installation end-to-end.
- `examples/rl` uses the same LP pipeline to plan multicast routes for weight broadcast; it can enable probing via env vars (see [RL Training](rl.md)).

## Notes

Only the **source node** should push routes via `set_group_routes()`. This avoids conflicting updates and keeps ownership consistent for a group’s `(S, G)` semantics.
