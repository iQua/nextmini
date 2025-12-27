# LP helpers for multicast tree selection (used by `examples/rl`)

## What this folder is for

- Compute a multicast DAG (tree) given a controller topology and `(src → destinations)` demand.
- Optionally push that DAG to the controller via `nextmini_py.Dataplane.set_group_routes(...)`.

## Why only the source node can push (`set_group_routes`)

Nextmini multicast groups are `(S, G)` (source-selected). Letting only the **group owner/source**
override routes avoids conflicting updates and prevents non-owners from hijacking delivery.

Practically for RL: the **trainer** (src) can compute and install the tree for the weight multicast.

## CLI usage (debugging)

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3
```

## Link probe mode

To request probe flows and overwrite link capacities with recent throughput:

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --probe-links --probe-window-secs 6 --probe-bytes 1000000000
```

This mode inserts probe flows into the controller DB (`flows.is_probe = true`),
waits for metrics, and then feeds measured link rates into the LP solver.
For per-second sampling, set `metrics_collection_interval = 1` in the dataplane
config and pass `--probe-window-secs 1`.

## End-to-end probe + LP run

1) Start the controller (Postgres up, controller running) and bring up dataplane nodes
   until the controller reports all nodes connected / topology ready.
2) Run the LP probe command from anywhere with DB access:

```bash
python -m examples.lp.main \
  --controller-config /path/to/controller-config.toml \
  --src 1 --dests 2,3 \
  --probe-links --probe-window-secs 6 --probe-bytes 1000000000
```

Notes:
- This is a one-shot measurement. Re-run the command to re-measure.
- The probe uses the database settings from controller-config.toml (or NEXTMINI_DB_* env vars).
- `--probe-window-secs` should be >= `metrics_collection_interval` so a full tick lands in `metrics`.

To apply the result live (must run as the **source node** and the group must already exist):

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --apply --node-config examples/rl/configs-docker/trainer-config.toml --group-id 7
```
