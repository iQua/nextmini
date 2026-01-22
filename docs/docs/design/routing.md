# Source-selected Routing (internals)

This page describes how Nextmini installs routes and how dataplane nodes select a route for each flow at runtime.

For “how to configure routes”, see the user-facing guide: [Defining the Network Topology and Routes](../examples/routes.md).

## Route installation pipeline

1. The controller loads its config file.
2. It merges:
   - custom `[[routes]]` definitions
   - and (optionally) topology-generated routes (when `[routing].protocol = "shortest_path"`).
3. Routes are written into Postgres (table `routes`) as a list of directed edges.
4. The controller broadcasts `InstallRoutes { routes: Vec<RoutingTableEntry> }` to dataplane nodes.

## Topology edges vs routes

- **Topology edges** (`[topology]`) define which nodes should establish neighbor connections (`AddNode`).
- **Routes** (`[[routes]]` or topology-generated shortest paths) define end-to-end forwarding choices.

If a route uses an edge that is not present in the topology, dataplane nodes will not have a neighbor link to forward to.

## Worked example: next hops and route IDs

In the controller config, a route is expressed as either:

- a node path: `route = [1, 2, 3, 4]`
- or a DAG: `route = [[1, 2], [2, 4], [1, 3], [3, 4]]`

At startup (and on subsequent DB updates), the controller:

1. Stores routes in Postgres as a list of directed edges.
2. Assigns each unique route a `route_id`.
3. Broadcasts `InstallRoutes { routes: Vec<RoutingTableEntry> }` to every node.

Each `RoutingTableEntry` contains:

- `src_node_id`, `dst_node_id`
- a `route_id`
- the node-local `next_hop` for that `(route_id, src, dst)` at the receiving node

If a node is not on the route, the controller uses `next_hop = INVALID` and the dataplane will treat that entry as unusable.

## Per-flow route selection

Each dataplane node selects a `route_id` for a flow using jump consistent hashing over the available routes for the `(src_node_id, dst_node_id)` pair and caches the result. This keeps steady-state forwarding stable.

When a `route_id` contains multiple next hops (DAG branch points), the node picks one next hop at random from the candidate list.

## Bidirectional TCP

TCP is bidirectional, so you must provision both directions for each communicating pair:

- A → B, and B → A

If you only install A → B, then return traffic can fail with “no route found” errors on the reverse path.
