# Multicast Groups – Implementation Notes

This note captures the practical details and verification status for the multicast work that landed in this branch. For the full design, see the canonical [`multicast-groups.md`](./multicast-groups.md) document now linked from the navigation.

---

## Implementation highlights

- **Controller**
  - Persists group metadata in `groups`, `group_members`, and `group_routes`.
  - Listens for `pg_notify('sync_group_routes', …)` to recompute multicast DAGs.
  - Emits `GroupCreated`, `InstallGroupDirectory`, and `InstallGroupRoutes` messages using the existing MessagePack channel.
- **Dataplane**
  - Extends the routing table with `RouteKey::Multicast` and caches of `(src, group_id) → next_hops`.
  - Branching happens inside the processor/connector; the Max profile reuses the same fan-out helpers as unicast.
  - Local delivery is modeled as `next_hop == local_node_id`, so receivers automatically process group traffic.
- **Messages**
  - Directory entries map `group_ip → group_id`, letting the dataplane classify packets before consulting route caches.
  - Route installs are incremental; repeated messages replace previous hop sets atomically.

---

## Configuration & rollout tips

- Reserve a multicast IP range in your controller config (`multicast_pool_base` / `multicast_pool_mask`) and document it for operators.
- Deploy the controller before rolling out upgraded dataplanes—older dataplanes ignore the new messages safely.
- Enable extra tracing with `RUST_LOG=info,controller::multicast=debug` when validating new topologies.

---

## Verification status (2025-11-05)

- ✅ Controller helper unit tests cover DAG construction (`compute_group_tree_edges`) and per-node routing tables.
- ✅ Dataplane unit tests exercise directory / route installs and packet fan-out.
- 🔁 Integration harness: Postgres-backed join/leave scenarios are scripted but still need to run in CI (tracked in `docs/testing/python_api_validation.md`).
- 🔁 Performance soak: Max-mode fan-out benchmarking pending once the shared multi-node harness is revived.

---

## Follow-up ideas

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
- [x] Dataplane routing table refactor and group directory plumbing (OrangeBear) — routing table internals rewritten with `RouteKey`/multicast cache; processors now ingest `InstallGroupDirectory`/`InstallGroupRoutes` and fan out packets per hop.
- [x] Processor multicast fan-out updates (OrangeBear) — per-hop replication now uses cached schedulers.
- [x] Controller DAG recompute helpers and notification wiring — helper utilities + unit tests landed; listener/recompute wiring still pending.
- [ ] Docs/examples/test refresh — design + example docs added; unit/integration test expansion still pending.

### Testing Work in Progress

- ✅ **Dataplane unit coverage**: tests in `dataplane/src/node/route.rs` cover `install_group_directory`, `install_group_routes`, and multicast `get_next_hops_by_flow`.
- **Controller integration**: scripted test to drive `CreateGroup`/`JoinGroup` against Postgres (requires live DB) and assert `InstallGroupRoutes` emission.
- **Fan-out soak**: stress test per-hop scheduler caching once the harness is ready.
- Add automated clean-up for empty groups and stale directory entries.
- Extend the CLI to surface active multicast groups for operators (`controller groups list`).
- Evaluate QUIC datagram support for multicast to reduce duplication in high-throughput fan-out cases.
