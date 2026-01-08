# LP Multicast Tree Selection

Computes multicast DAG edges for a given topology and `(src → destinations)` demand.
Supports:

- `mflow` (LP-based tree extraction)
- `cf_tree` (LP-guided hop-limited rounding)
- `cf_bottleneck` (bottleneck-aware CF-Tree rate search)
- `basic_tree` (capacity-only hop-limited tree)

## Features

- Builds topology graph from controller config
- Optional link probing to measure actual capacities
- Solves max-min fair multicast LP
- Converts LP solution to multicast tree edges
- Optional relay caps via LP-guided relay scoring (`--max-relays`)
- Optional destination forwarding (allow receivers to act as relays)

## Usage

See `examples/lp/toy/` for a complete working example using Docker Compose.

## Architecture

Only the **source node** can push routes via `set_group_routes()`. This avoids conflicting updates and prevents non-owners from hijacking delivery.
