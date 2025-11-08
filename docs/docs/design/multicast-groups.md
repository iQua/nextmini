# Multicast Groups in Nextmini

This document captures the current design for multicast group support inside Nextmini. A multicast group lets one source node transmit to many receivers via a single logical destination IP. The controller owns multicast tree computation and persistence, while the dataplane replicates packets hop-by-hop using the existing scheduler pipeline.

---

## Goals

- **(S, G) semantics** – each group `G` is owned by a single source node `S`.
- **Dynamic membership** – destinations join or leave at runtime; the controller recomputes trees and pushes incremental updates.
- **Packet fan-out in dataplane** – branching nodes duplicate packets to every next hop.
- **Backwards compatibility** – unicast flows remain untouched, and rollout can stage controller before dataplane.

Non-goals for the first iteration:

- IGMP snooping or L3 interoperability.
- Multi-source groups (`*, G`).

---

## Control Plane Responsibilities

### Persistence

| Table | Purpose |
|-------|---------|
| `groups` | Allocated multicast groups with `label`, `src_node_id`, and reserved `group_ip`. |
| `group_members` | Membership rows keyed by `(group_id, node_id)` with join timestamps. |
| `group_routes` | Cached multicast DAG edges for each group, persisted as JSON. |

Membership changes trigger `pg_notify('sync_group_routes', …)` so the controller can recompute routes.

### Message Surface

New MessagePack payloads (see `nextmini-messages` crate):

- **Dataplane → Controller**: `CreateGroup`, `JoinGroup`, `LeaveGroup`.
- **Controller → Dataplane**:
  - `GroupCreated { group_id, group_ip, src_node_id }` – sent to the creator.
  - `InstallGroupDirectory { groups: [...] }` – broadcast directory updates.
  - `InstallGroupRoutes { group_id, src_node_id, routes: [...] }` – per-node next-hop sets.

### Tree Computation Flow

1. On membership change, the controller loads all members for `(S, G)`.
2. For each member, it looks up the unicast path `S → member` from the `routes` table.
3. All edges are unioned into a DAG and persisted in `group_routes`.
4. Nodes participating in the tree (previous or current) receive `InstallGroupRoutes`.

Helper functions in `controller/src/utils.rs`:

- `compute_group_tree_edges` – unions shortest paths.
- `build_group_routes_for_node` – constructs `GroupRoutingTableEntry` including local delivery for members.

Unit tests verify both helpers.

---

## Dataplane Responsibilities

### Routing Table

`dataplane/src/node/route.rs` now distinguishes keys:

- `RouteKey::Unicast(src, dst)`
- `RouteKey::Multicast(src, group_id)`

The routing table stores:

- A directory mapping `group_ip → group_id`.
- Route caches keyed by `(src, group_id)` with next-hop vectors.
- Flow-level caches for consistent hashing (multicast entries reuse the same path for hop sets).

### Packet Processing

- **Processor** – uses `get_next_hops_by_flow` for every packet, cloning when multiple hops exist.

Processors react to `InstallGroupDirectory` and `InstallGroupRoutes` messages.

---

## Control Flow Summary

1. Source calls `CreateGroup`; controller allocates IP and replies with `GroupCreated`.
2. Controller pushes updated directories to all nodes.
3. Members issue `JoinGroup`; controller recomputes the multicast DAG.
4. Controller pushes `InstallGroupRoutes` for affected nodes.
5. Dataplane routes packets by resolving `dst_ip` to `group_id`, replicating per hop.
6. `LeaveGroup` triggers recomputation; empty groups can be garbage collected separately.

---

## Observability & Safety

- Existing tracing emits route installation logs per node.
- Controller logs warn when paths are missing or WebSocket writers are unavailable.
- DAG persistence allows auditing and diffing of multicast topology changes.
- TTL on packets still offers loop protection, though the DAG computation already avoids cycles.

---

## Testing Notes

Unit tests cover helper logic (`compute_group_tree_edges`, `build_group_routes_for_node`). Additional test work is tracked in the project plan:

- Dataplane routing-table tests that validate group directory lookups.
- Integration tests that drive membership changes via Postgres notifications.
- Performance checks for high-fan-out multicast branches.

See `docs/docs/examples/multicast-flow.md` for an end-to-end walkthrough.
