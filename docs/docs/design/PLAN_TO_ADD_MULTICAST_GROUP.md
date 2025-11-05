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

- Add automated clean-up for empty groups and stale directory entries.
- Extend the CLI to surface active multicast groups for operators (`controller groups list`).
- Evaluate QUIC datagram support for multicast to reduce duplication in high-throughput fan-out cases.
