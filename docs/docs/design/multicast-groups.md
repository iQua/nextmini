# Multicast Groups in Nextmini

Multicast groups let a single source node deliver packets to many receivers through one logical destination IP. The controller owns group lifecycle and persistence; multicast DAG edges are supplied externally and pushed to the dataplane, which mirrors the group directory, fans out packets hop-by-hop, and preserves the existing scheduling pipeline.

---

## Feature Goals

- **(S, G) semantics** – each group `G` belongs to one source node `S`; the source pushes traffic to the group IP.
- **Dynamic membership** – destinations may join and leave while the system runs; the controller reapplies stored DAG edges and ships incremental updates for local delivery.
- **Fast-path fan-out** – branching dataplane nodes clone packets for every next hop, both in normal and Max scheduling paths.
- **Backwards compatibility** – unicast routing, flow installation, and rollout tooling continue to behave unchanged.

Non-goals for this iteration:

- IGMP snooping or transparent interoperability with arbitrary L3 applications.
- Shared, multi-source groups (`*, G`).

---

## Terminology & Group Identity

- **Group ID (`group_id`)** – numeric identifier allocated by the controller.
- **Group IP (`group_ip`)** – virtual IPv4 address used by applications. Addresses are allocated from a configurable pool (`multicast_pool_base` / `multicast_pool_mask` in `controller/src/config.rs`).
- **Directory entry** – `{ group_id, group_ip }` tuple distributed to all nodes (`GroupDirectoryEntry`).
- **Route entry** – `{ route_id, next_hops }` per `(src, group)` pair, delivered as `GroupRoutingTableEntry` objects.

The directory is global; route entries are scoped to nodes that appear in the multicast DAG.

---

## Control Plane Implementation

### Persistence & Notification

The controller persists multicast data in Postgres:

| Table | Purpose |
|-------|---------|
| `groups` | Defines each multicast group, including `label`, `src_node_id`, and `group_ip`. |
| `group_members` | Join table keyed by `(group_id, member_node_id)` with timestamps. |
| `group_routes` | Cached DAG edges (JSON) for each `(src, group)` combination. |

`controller/src/db.rs` installs database triggers so that any insert/delete on `group_members` fires `pg_notify('sync_group_routes', ...)`. A Tokio task (`setup_group_notification`) listens on that channel, reloads the stored DAG edges from `group_routes` (written via `SetGroupRoutes`), rebuilds per-node routes with `build_group_routes_for_node` (`controller/src/utils.rs`), and pushes fresh routes. Membership changes now only affect local delivery; the DAG itself is externally supplied.

### Message Surface

New MessagePack payloads (defined in `messages/src/lib.rs`):

- **Dataplane → Controller**: `CreateGroup`, `JoinGroup`, `LeaveGroup`, `SetGroupRoutes`.
- **Controller → Dataplane**:
  - `GroupCreated { group_id, group_ip, src_node_id }` (acknowledges creation back to the source).
  - `InstallGroupDirectory { groups }` (broadcast directory refresh for every node).
  - `InstallGroupRoutes { group_id, src_node_id, routes }` (per-node next-hop vectors, sent only to nodes participating in the DAG).

### Lifecycle Walkthrough

1. **Create** – A node invokes `CreateGroup`. The controller reserves an IP from the configured pool, stores the group, and replies with `GroupCreated`. It then calls `broadcast_group_directory` so every node learns the new mapping.
2. **Install DAG** – The source (or an external solver) calls `SetGroupRoutes` with explicit DAG edges. The controller persists the edges in `group_routes` and pushes `InstallGroupRoutes` to nodes referenced by the DAG.
3. **Join / Leave** – Each member submits `JoinGroup` or `LeaveGroup`. The resulting database mutation triggers `sync_group_routes`. The controller reuses the stored DAG edges, rebuilds per-node routes (including local delivery for members), and calls `push_group_routes` to send `InstallGroupRoutes` updates.
4. **Delivery** – Once the directory is replicated, the source sends packets toward `group_ip`. Membership changes eventually propagate through the same notification channel.
5. **Tear-down** – When the last member leaves, the group remains until explicitly deleted or garbage-collected; future work may add timers to prune empty groups.

The controller tolerates intermittent websocket outages—if a node lacks an active writer when routes are pushed, the update is skipped and retried once the node reconnects (leveraging the full snapshot sent during handshake).

---

## Dataplane Implementation

### Routing Table Structure

`dataplane/src/node/route.rs` extends the routing table with:

- `RouteKey::Unicast(src, dst)` and `RouteKey::Multicast(src, group_id)` variants.
- A global `group_dir` map from `group_ip → group_id` refreshed via `install_group_directory`.
- Per-key route pools stored in `available_routes`, with next-hop vectors cached in `route_next_hop`.
- Jump consistent hashing (`JumpHasher`) reused for multicast to keep flow-to-route mapping stable even as routes churn.

### Packet Processing

- **Processor** (`dataplane/src/node/processor.rs`) calls `get_next_hops_by_flow` for every packet. When multiple hops exist, it clones the packet payload and enqueues each hop on the scheduler. Flow-to-route caching accelerates steady-state traffic but is cleared whenever new routes arrive. Processors react to `InstallGroupDirectory` and `InstallGroupRoutes` messages.

- **Directory lookups** happen inline: if a flow’s destination IP appears in `group_dir`, the route key converts to `(src, group_id)` before hashing.

If no route is available, the dataplane logs a warning and drops the packet (matching the existing unicast behaviour). Stale cache entries are purged automatically when the controller pushes updated route IDs.

---

## Client APIs

### Python bindings (`nextmini_py`)

`nextmini_py.Dataplane` exposes a minimal set of helpers so applications can manage multicast membership without touching
the controller CLI:

| Method | Purpose |
| --- | --- |
| `create_group(label)` | Requests a new `(group_id, group_ip)` pair for the local node (the source). |
| `group_is_ready(timeout_ms=None)` | Blocks until the controller acknowledges a group and returns `(group_id, group_ip, src_node_id)`. |
| `set_group_routes(group_id, edges)` | Persists DAG edges for a multicast group so the controller can install routes. |
| `wait_for_group_routes(group_id, src_node_id, min_routes=1, timeout_ms=None)` | Blocks until multicast routes are installed on the local node. |
| `join_group(group_id)` / `leave_group(group_id)` | Adds or drops the local node from the specified group. |
| `register_receiver_for_group(src_node_id, group_ip, src_port=None, dst_port=None)` | Binds a Python-side queue to packets sourced from `src_node_id` and destined for `group_ip`. |

Example sender workflow:

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
dp.create_group("training-run-42")
group = dp.group_is_ready(timeout_ms=5_000)
assert group, "controller never acknowledged the group"
group_id, group_ip, _ = group
edges = [(1, 2), (1, 3)]
dp.set_group_routes(group_id, edges)
dp.wait_for_group_routes(group_id, 1, timeout_ms=5_000)
```

Example receiver workflow:

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
dp.join_group(group_id)
rx = dp.register_receiver_for_group(
    src_node_id=1,
    group_ip=group_ip,
)
payload = rx.recv(timeout_ms=2_000)
dp.leave_group(group_id)
```

All helpers run over the existing websocket between the dataplane and controller, so no additional services are
required. Events flow through the same `PythonEvent` queue used by the PyTorch bindings, enabling scripts to await group
creation or membership changes.

---

## Operational Notes

- **Logging** – `RUST_LOG=info` surfaces directory broadcasts, route pushes, and membership changes on both controller and dataplane sides.
- **Metrics** – Flow statistics code treats multicast flows identically; per-hop byte counters expand naturally as the same flow ID fans out.
- **Configuration** – The multicast pool defaults to `239.255.0.0/16`. Override `controller.config.multicast_pool_base` / `multicast_pool_mask` to carve a different range.
- **Resilience** – Controller retries on serialization errors and warns when websocket writers vanish. Dataplane caches clear on every install so stale entries never linger.

---

## Testing & Verification

Current coverage (see `controller/src/utils.rs` and `dataplane/src/node/route.rs`):

- Unit tests validate DAG construction, membership pruning, and per-node route assembly.
- Dataplane tests exercise directory installation, route fan-out, and cache flushing.

Planned follow-ups tracked in `docs/testing/python_api_validation.md` and project mail:

- Controller integration test that drives `CreateGroup`/`JoinGroup` against a live Postgres instance and verifies websocket pushes.
- End-to-end soak demonstrating packet fan-out across multiple branches (normal + Max mode).
- Automation to clean up idle groups and surface metrics dashboards.

---

## Related Material

- **Example walkthrough** – `docs/docs/examples/multicast-flow.md` shows the CLI/API flow for creating a group, joining members, and verifying delivery.
- **Testing harness plan** – `docs/testing/python_api_validation.md` describes the multi-node docker-compose scenario used to validate multicast plus the Python dataplane bridge.
- **Controller configuration** – See `controller/src/config.rs` for the multicast pool defaults and other tunables.
- Dataplane routing-table tests that validate group directory lookups.
- Integration tests that drive membership changes via Postgres notifications.
- Performance checks for high-fan-out multicast branches.

See `docs/docs/examples/multicast-flow.md` for an end-to-end walkthrough.
