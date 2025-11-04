# Multicast Groups in Nextmini

This document proposes an end-to-end design to add **multicast group** support to Nextmini. It enables a source node to create a group and send to a **group IP**, while any number of destination nodes can **join or leave** the group at any time. Packets are **replicated** along a multicast tree (a DAG), reusing the controller’s route representation and the dataplane’s packet scheduler.

---

## Goals

- **(S, G) semantics**: Each group `G` has exactly one **source node** `S`. Multiple groups can co-exist.
- **Dynamic membership**: Destinations can join/leave `G` at any time. Controller updates the multicast tree and pushes new next-hops.
- **Data path replication**: Dataplane **duplicates packets to all next hops** at each branching node (already supported by our multicast fan-out change).
- **Backwards compatible**: Unicast routes and flows continue to work unchanged.
- **Minimal control plane surface**: Add small set of messages & tables to manage groups and memberships.

Non-goals (for the first iteration):
- IGMP compatibility / L3 snooping on arbitrary apps (we’ll use a control message–based join/leave).
- Multi-source groups (*,G). We implement (S,G) where S is the owner/creator.

---

## High-Level Architecture

1. **Group identity**  
   - `group_id` (integer) and `group_ip` (IPv4 in a reserved range, e.g., `239.255.0.0/16` inside the virtual network).
   - `group_ip` is where the source application sends traffic (TUN sees packets to `group_ip`).

2. **Control-plane (Controller)**
   - New DB tables: `groups`, `group_members`, `group_routes`.
   - New WS protocol (MessagePack) messages for CreateGroup/JoinGroup/LeaveGroup and route installs.
   - Tree computation: **Union of shortest paths** from `S` to each member, producing a **directed acyclic graph** (DAG) of edges.
   - Per-node **next_hops** built from DAG; pushed via `InstallGroupRoutes`.

3. **Dataplane**
   - Maintains a **group directory** (map `group_ip -> group_id`) installed by controller.
   - RoutingTable supports **two keys**:
     - `Unicast(src_node, dst_node)`
     - `Multicast(src_node, group_id)`
   - Packet classification: if `dst_ip` ∈ group directory ⇒ multicast; else unicast.
   - `get_next_hops_by_flow` returns **all next hops** for multicast; **Processor** duplicates packet to each next hop.
   - Leaf nodes that are members receive **local-delivery** via `next_hops` containing the local node id. Non-members never have local delivery for that group.

4. **Membership dynamics**
   - **Join**: controller adds row to `group_members`, recomputes DAG, updates `group_routes`, and pushes `InstallGroupRoutes`.
   - **Leave**: controller removes member, recomputes DAG. If no more members, routes become empty and the controller may optionally tear down the group.

---

## Control Messages (summary)

_Status — 2025-11-04 (RedBear): Message enums landed; controller plumbing now emitting GroupCreated + directory broadcasts._

- **Dataplane → Controller**
  - `CreateGroup { label }` (from source)
  - `JoinGroup { group_id }` (from any node)
  - `LeaveGroup { group_id }`

- **Controller → Dataplane**
  - `GroupCreated { group_id, group_ip, src_node_id }` (to source)
  - `InstallGroupDirectory { groups: [{group_id, group_ip}] }` (to all nodes; incremental updates supported)
  - `InstallGroupRoutes { group_id, src_node_id, routes: [GroupRoutingTableEntry...] }` (to all nodes that appear in the DAG)

---

## Data Model

_Status — 2025-11-04 (RedBear): Groups/members/routes tables + membership trigger committed; DAG recompute scaffold hooked up._

- `groups(group_id SERIAL PK, label TEXT UNIQUE, src_node_id INT NOT NULL, group_ip TEXT UNIQUE NOT NULL, created_at BIGINT)`
- `group_members(group_id INT FK, node_id INT, joined_at BIGINT, PRIMARY KEY (group_id, node_id))`
- `group_routes(group_id INT FK, src_node_id INT NOT NULL, edges JSONB NOT NULL, updated_at BIGINT)`
  - `edges` is `[[a,b],[b,c],...]` representing the multicast DAG.

Triggers/Notifications:
- On `group_members` INSERT/DELETE → `pg_notify('sync_group_routes', '{"group_id":...}')`.
- Controller listener recomputes routes and pushes new installs.

---

## Tree Computation

_Status — 2025-11-04 (RedBear): Controller now recomputes DAGs from installed unicast routes and pushes per-node fan-out entries._

- **Union of shortest paths**: For each member `m`, run shortest path `S → m` over the **topology** (existing controller graph). Union all edges; direct edges in both directions (bidirectional graph) as needed for forwarding semantics.
- Store as `group_routes.edges` (directed).
- Build per-node **next_hops** = all outgoing neighbors in the DAG. For a **member** node, include `local_node_id` in `next_hops` to cause local delivery.

---

## Dataplane Behavior

_Status — 2025-11-04 (OrangeBear): Pending kickoff; waiting on controller/message payload availability._

- On `InstallGroupDirectory`: update group IP → id map.
- On `InstallGroupRoutes`: update the `RoutingTable`:
  - Insert/replace `route_id = group_id` next hop sets for all nodes.
  - Map key `Multicast(src, group_id)` → `[route_id]`.
- On packet:
  - If `dst_ip` in map: treat as multicast, key becomes `(S, G)`, replicate to **all** next hops returned.
  - If a hop equals local node id: deliver to TUN (local).

**Operating modes**:
- **Normal**: scheduler-based replication (already implemented).
- **Max**: connector replicates to **all downstream next hops**; relays fan out upstream bytes to many peers (already scaffolded).

---

## Observability & Safety

- Existing metrics aggregation works per-hop; aggregate bytes reflect replication.
- No loops: tree is DAG from controller. TTL is relied upon as a safety backstop; we do not generate cycles.

---

## Backward Compatibility

- Unicast messages unchanged; new variants introduced.
- RoutingTable supports both `Unicast` and `Multicast` keys.
- Rollout: deploy controller first; dataplane ignores unknown messages until upgraded.

---

## Test Plan (summary)

_Status — 2025-11-04 (RedBear & OrangeBear): Controller helpers now covered by unit tests (`cargo test -p controller`); dataplane fast-path + joint integration coverage pending._

- **Unit**: route key selection; group dir lookup; DAG → per-node next_hops; processor fan-out.
- **Integration**: source creates group; N nodes join; controller recompute; verify each member receives; remove member; verify updates; tear down group.
- **Performance**: saturate multicast with 2, 4, 8 branches; ensure no queue starvation or deadlocks.

---

## Implementation Status (2025-11-04)

- [ ] Messages/config/database scaffolding (RedBear) — in progress per agent mail, awaiting interface handoff.
- [x] Dataplane routing table refactor and group directory plumbing (OrangeBear) — routing table internals rewritten with `RouteKey`/multicast cache; processor + connector now ingest `InstallGroupDirectory`/`InstallGroupRoutes` and fan out packets per hop.
- [x] Processor/connector multicast fan-out updates (OrangeBear) — normal + Max paths now replicate per hop with cached schedulers.
- [x] Controller DAG recompute helpers and notification wiring — helper utilities + unit tests landed; listener/recompute wiring still pending.
- [ ] Docs/examples/test refresh — design + example docs added; unit/integration test expansion still pending.

### Testing Work in Progress

- ✅ **Dataplane unit coverage**: tests in `dataplane/src/node/route.rs` cover `install_group_directory`, `install_group_routes`, and multicast `get_next_hops_by_flow`.
- **Controller integration**: scripted test to drive `CreateGroup`/`JoinGroup` against Postgres (requires live DB) and assert `InstallGroupRoutes` emission.
- **Max mode soak**: stress test per-hop scheduler caching once Max-mode harness is ready.
