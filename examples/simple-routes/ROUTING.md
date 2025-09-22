# NextMini Source-Selected Routing Architecture

## Core Concept

- Route installation pipeline:
  1) Controller loads `controller-config.toml`.
  2) Merges custom `[[routes]]` and (if enabled) topology-generated routes.
  3) Writes routes into Postgres `routes(route_id SERIAL, src_node_id, dst_node_id, edges JSONB)`. Here (src_node_id, dst_node_id, edges) the three parts are inserted into DB. `route_id` is auto-generated (SERIAL); `edges` is stored as a JSONB array of directed edges (e.g., `[[1,2],[2,4],[1,3],[3,4]]`). 
  
  4) Broadcasts `InstallRoutes { routes: Vec<RoutingTableEntry> }` to all dataplane nodes.
- Per-node route selection:
  - Each node selects a `route_id` per flow using jump consistent hash over the available `route_id`s for `(src_node_id, dst_node_id)`, then caches the mapping.
- AddNode vs routes:
  - AddNode connections are built from `[topology]` edges (preset or custom), not from `[[routes]]`, which means the default topology edges cover all next hops that your routes require.

## Route Configuration in controller-config.toml

- Sequence (single path):

  ```toml
  [[routes]]
  route = [1, 2, 3, 4]
  ```

- Directed Acyclic Graph (multiple branches for one source → one destination):

  ```toml
  [[routes]]
  route = [[1, 2], [2, 4], [1, 3], [3, 4]]
  ```

The controller infers `src_node_id` as the node with outgoing but no incoming edges, and `dst_node_id` as the node with incoming but no outgoing edges.

## Topology-Generated Routes

- Topology edges are built from `[topology]` preset or `[topology].edges`.
- End-to-end routes are generated from topology only when you set:

  ```toml
  [routing]
  protocol = "shortest_path"
  ```
- If omitted, `protocol=None` and no topology routes are generated; only your custom `[[routes]]` are inserted into the database.

## Packet Processing (no header rewrite)

```
Flow arrives at node N
  └─ Extract (src_node_id, dst_node_id) from IPs
     └─ available = available_routes[(src_node_id, dst_node_id)]
        └─ if cache miss: route_id = jump_hash(flow_id, len(available))
           cache[flow_id] = route_id
           next_hops = route_next_hop[route_id]
           pick one next_hop (random if multiple) // now using fastrand crate
           forward or deliver locally
```

- Mapping is deterministic per-flow on each node (jump hash + cache).
- Nodes pick the same route_id when the available route ordering is identical across nodes.
- `available_routes` and `route_next_hop` are built from `InstallRoutes` entries at node boot.

## Bidirectional TCP

- TCP is bidirectional. You need to provision both directions for each communicating pair:
  - A → B, and B → A.
- If you only install A → B, B → A will have no `route_id`, leading to "No route is found for flow xxxxxx" errors on the reverse path.


## Notes

- AddNode neighbors are derived from `[topology]` edges.