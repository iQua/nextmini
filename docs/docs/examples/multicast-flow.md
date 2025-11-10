# Example: Multicast Flow Lifecycle

This walkthrough shows the controller/dataplane interactions for a simple multicast group where node `1` acts as the source and nodes `3` and `6` subscribe as members.

---

## 1. Source Creates a Group

Source node (dataplane) sends:

```
CreateGroup { label: "job-42" }
```

Controller responds to the source with:

```
GroupCreated { group_id: 7, group_ip: 239.255.0.10, src_node_id: 1 }
```

It also broadcasts the allocation to every node:

```
InstallGroupDirectory {
    groups: [{ group_id: 7, group_ip: 239.255.0.10 }]
}
```

Dataplane updates its `group_ip → group_id` directory upon receiving the directory install.

---

## 2. Members Join

Nodes `3` and `6` issue join requests:

```
JoinGroup { group_id: 7 }
```

Each membership change triggers a Postgres notification. The controller:

1. Fetches all members for group `7`.
2. Reuses the stored unicast topology to compute the multicast DAG.
3. Persists the DAG in `group_routes`.
4. Sends `InstallGroupRoutes` to every node affected by the DAG (including source and members).

Example payload for node `2` (an intermediate hop):

```
InstallGroupRoutes {
    group_id: 7,
    src_node_id: 1,
    routes: [{
        route_id: 7,
        next_hops: [3, 6],
        src_node_id: 1,
        group_id: 7
    }]
}
```

Node `3` receives a payload that includes local delivery:

```
InstallGroupRoutes {
    group_id: 7,
    src_node_id: 1,
    routes: [{
        route_id: 7,
        next_hops: [3],
        src_node_id: 1,
        group_id: 7
    }]
}
```

---

## 3. Traffic Delivery

The source application sends packets to `239.255.0.10`.

- The dataplane recognizes the destination IP as a multicast group.
- `get_next_hops_by_flow` returns all downstream next hops for `(src=1, group_id=7)`.
- The processor clones packets for each hop using the routing table’s next-hop list.
- Leaf nodes that are group members see local delivery via their routing entry.

---

## 4. Member Leaves

When node `6` leaves:

```
LeaveGroup { group_id: 7 }
```

The controller recomputes the DAG, persists the new edge set, and sends `InstallGroupRoutes` updates. Nodes that no longer participate receive an empty route list (or no message if they have no active connection), effectively removing the fan-out.

---

## Python helper snippet

The `nextmini_py` bindings expose helpers for create/join/leave and receiver registration, so applications do not need a
separate CLI:

```python
import nextmini_py as nm

# Source node
dp = nm.Dataplane("/abs/path/source-config.toml")
dp.create_group("job-42")
group = dp.group_is_ready(timeout_ms=5_000)
assert group, "controller did not acknowledge the group"
group_id, group_ip, src_node_id = group

# Receiver node
rx_dp = nm.Dataplane("/abs/path/receiver-config.toml")
rx_dp.join_group(group_id)
rx_dp.wait_for_local_membership(group_id, timeout_ms=5_000)
rx = rx_dp.register_receiver_for_group(
    src_node_id=src_node_id,
    group_ip=group_ip,
    payload_only=True,
)
payload = rx.recv(timeout_ms=2_000)
rx_dp.leave_group(group_id)
```

Use `wait_for_routes_installed(group_id, src_node_id)` when you want to confirm the controller pushed the latest
fan-out (e.g., in tests). `register_receiver_for_group` takes the source node ID and the allocated group IP, keeping the
flow classification consistent with the dataplane’s routing table.

---

## Verification Checklist

- `cargo check --workspace`
- `cargo test -p controller compute_group_tree_edges`
- `cargo test -p controller build_group_routes_for_node_includes_local_delivery`
- `cargo test -p dataplane returns_multicast_next_hops_from_group_routes`
- Follow the integration recipe in `docs/testing/python_api_validation.md` to exercise membership churn once the docker-compose harness is available.

---

## Next Steps

- Automate the Postgres-backed integration test so group membership churn runs in CI.
- Add operational guidance (failover procedures, idle group garbage collection/ACLs) once exercised in dev clusters—track progress in `docs/docs/design/multicast-groups.md#operational-notes`.
