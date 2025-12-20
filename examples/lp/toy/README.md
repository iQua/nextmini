# Toy demo

This demo runs a minimal 3-node Nextmini deployment and validates:

- `convert_to_multicast_trees()` extraction (`examples/lp/tree_conversion.py`)
- `nextmini_py.Dataplane.set_group_routes()` end-to-end (controller installs multicast DAG edges)
- Reliable multicast delivery from node 1 → nodes 2 and 3

## Run

From the repo root:

```bash
cd examples/lp/toy
rm -f ./shared/group.json ./shared/joined_*
docker compose up --build
```

Look for logs like:

- source: `toy conversion ... edges=[(1, 2), (1, 3)]`
- receivers: `payload=b'hello-nextmini'`
