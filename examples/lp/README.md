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

To apply the result live (must run as the **source node** and the group must already exist):

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --apply --node-config examples/rl/configs-docker/trainer-config.toml --group-id 7
```

## Legacy DB injection

If you still need the older “write Postgres + pg_notify” flow, see `examples/lp/legacy_db/`.
