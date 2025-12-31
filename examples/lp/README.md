# LP Multicast Tree Selection

Computes optimal multicast DAG (tree) for a given topology and `(src → destinations)` demand using Linear Programming (mFlow).

## Features

- Builds topology graph from controller config
- Optional link probing to measure actual capacities
- Solves max-min fair multicast LP
- Converts LP solution to multicast tree edges

## Usage

See `examples/lp/toy/` for a complete working example using Docker Compose.

## Architecture

Only the **source node** can push routes via `set_group_routes()`. This avoids conflicting updates and prevents non-owners from hijacking delivery.
