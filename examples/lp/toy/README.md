# Toy demo

This demo runs a minimal 3-node Nextmini deployment and validates:

- `convert_to_multicast_trees()` extraction (`examples/lp/tree_conversion.py`)
- `nextmini_py.Dataplane.set_group_routes()` end-to-end (controller installs multicast DAG edges)
- Reliable multicast delivery from node 1 → nodes 2 and 3
- Optional link probing to feed measured throughput into the LP solver

## Run

From the repo root:

```bash
cd examples/lp/toy
docker compose up --build
```

Look for logs like:

- source: `conversion solver=mflow ... edges=[...]`
- receivers: `payload=b'hello-nextmini'`

## Probe + LP in the toy stack

The compose stack runs **probe → LP** automatically on node1 using the defaults in
`examples/lp/toy/toy_demo.py`.

Notes:
- This is a one-shot measurement; re-run `docker compose up --build` to re-measure.
- The probe uses the controller DB settings from `controller-config.toml`.
- The demo waits for probe flows to finish (up to the timeout) before sending the payload.
- If you want per-second sampling, set `metrics_collection_interval = 1` in the node configs
  and change `PROBE_WINDOW_SECS = 1.0` in `examples/lp/toy/toy_demo.py`.
- We should let `PROBE_WINDOW_SECS >= metrics_collection_interval`.
