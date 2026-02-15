---
title: "Multicast Groups"
description: "Architectural overview of Nextmini multicast groups, DAG propagation, and forwarding behavior."
---

Nextmini uses in-band logical groups to let one source node fan one flow toward many receivers.

Each group is treated as an `(S, G)` pair where `S` is a single source node and `G` is a group identity backed by a virtual destination IP. Applications send traffic to that group IP, and the dataplane expands it along pre-installed DAG branches. The controller owns group registration, membership, and group route persistence, while the dataplane keeps a fast forwarding path for both unicast and multicast lookups.

## Design goals and boundaries

The design intentionally targets one-source groups first. In this release, `*, G` style shared-source groups are not part of the contract, and multicast group behavior is not tied to IGMP snooping.

The controller requires explicit DAG guidance for each group. That lets routing and rollout stay deterministic and makes tree behavior predictable under testing and production control.

## Group data model

The controller maps each group to two logical artifacts:

- `group_id`: numeric identifier of the group.
- `group_ip`: virtual destination IP used by applications.

Group identity is synchronized as directory state across nodes, while actual forwarding state is stored as per-node routes derived from `(src_node_id, group_id)` combinations. That split is what allows membership and topology updates to be localized without changing every node’s policy at once.

## Controller flow

The controller stores multicast state in Postgres tables for durability and change propagation. Route materialization is then driven by notifications, not polling:

- `groups` holds definitions and allocation state.
- `group_members` tracks membership changes.
- `group_routes` stores DAG edges for each `(src, group)` pair.

A database trigger on `group_members` publishes `sync_group_routes`. The controller reloads the stored DAGs, rebuilds per-node route vectors, and pushes updates to nodes participating in each DAG.

If a node is temporarily disconnected when a route update is published, the update is not lost. That node receives a full snapshot during its next websocket handshake.

In message flow terms, the controller continues to use the same websocket envelope family to deliver directory and route installs:

- directory installs share the current group-to-id mapping across nodes,
- and route installs deliver per-node next-hop vectors for active DAGs.

## Dataplane flow and forwarding behavior

Within the dataplane routing table, group packets are handled by extending existing route-keyed lookup to include multicast source/group keys. Incoming packets are first checked for group destination context:

1. when destination resolves to a known `group_ip`, the key becomes `(src, group_id)`;
2. route lookup returns zero, one, or many next hops;
3. multiple next hops are fanned out as packet copies into the scheduler.

This keeps multicast inside the normal scheduling and transport stack instead of introducing a separate forwarding pipeline.

The dataplane also tracks two cache layers for multicast lookup:

- exact `(src_node_id, group_id, tree_id)` entries for steady-state forwarding;
- fallback control-tree cache entries when tree context is absent.

Both caches are refreshed when group routes are installed so stale entries are short-lived and replacement is consistent.

## Control-frame routing in the absence of `tree_id`

Lossless control frames do not always carry `tree_id`. For those cases, the dataplane uses a deterministic policy: prefer tree `0` when available for that `(src_node_id, group_id)`, otherwise choose the smallest available tree. If no tree exists for the pair, forwarding drops with the same missing-route path used for unicast miss behavior.

This policy is a local control-plane decision only; it does not alter packet formats.

## Lifecycle at a glance

A group can be thought of as five phases: create, install, join/leave, deliver, and retire. Create generates group identity, install binds DAG state, join/leave updates membership and route materialization, delivery streams to group endpoints, and retirement waits for explicit cleanup when the group is no longer needed.

The key operational rule is that membership churn and route projection share the same notification loop, so one update path drives both state transitions.

## Integration points

Application APIs and concrete call examples are documented in [`Python API`](/docs/python-api). Implementation notes for the Python embedding path are in [`Python API Design`](./python-api-design). This page focuses on control-plane and dataplane mechanics.

## Operational notes

`RUST_LOG=info` is useful for observing group directory sync, route installation, and membership transitions. Multicast counters are accumulated through ordinary flow accounting, so fanned payloads naturally increase hop-level counters as branching occurs.

The default multicast pool remains `239.255.0.0/16`; override controller pool settings (`multicast_pool_base`, `multicast_pool_mask`) only if your environment requires a different namespace.

## Validation focus

Most behavior is covered in controller and route-table tests. The main areas are DAG materialization, per-node route assembly, cache refresh behavior, and control-plane replay correctness when route updates arrive under churn.
