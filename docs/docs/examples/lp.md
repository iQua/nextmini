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

## Notes

Only the **source node** should push routes via `set_group_routes()`. This avoids conflicting updates and keeps ownership consistent for a group’s `(S, G)` semantics.

