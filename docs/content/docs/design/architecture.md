---
title: "Architectural Design"
description: "Nextmini: a High-Performance Network Emulation and Experimentation Testbed"
---

Nextmini is organized as a small workspace of focused crates: `controller/` for control-plane services (topology, routing, flow installation, multicast lifecycle, and Postgres integration), `dataplane/` (`nextmini` package) for node runtime behavior like network I/O, packet processing, scheduling, and lossless sessions, `messages/` for shared MessagePack protocol types, `python-api/` (`nextmini_py`) for PyO3 bindings, and `raptorq/` for the FEC primitives.

### Control plane (`controller`)

The controller accepts dataplane websocket connections, assigns startup parameters (`StartUp`), computes routes from configured topology or explicit route definitions, and pushes updates (`InstallRoutes`, `AddFlows`, multicast directory and route messages). Its most important modules are `controller/src/config.rs` for schema and config shape, `controller/src/db.rs` for Postgres persistence and notifications, and `controller/src/utils.rs` for route construction helpers, including multicast per-node route derivation.

### Dataplane (`nextmini`)

Each dataplane node runs an async actor-style runtime that coordinates transport servers and clients (`tcp`, `udp`, `quic`, and max-mode connection-on-demand), packet processing and scheduling, route table lookups with next-hop fan-out, and optional Python delivery plus lossless session execution. The runtime is assembled in `dataplane/src/node/conductor.rs`, uses protocol logic from `dataplane/src/node/network/*`, performs packet parsing and forwarding in `dataplane/src/node/processor.rs`, stores forwarding state in `dataplane/src/node/route.rs`, and owns session-sender/receiver runtime checks in `dataplane/src/node/session/*`.

### Processor and Routing Path (Implementation)

The packet ingress and forwarding path is centralized in `dataplane/src/node/processor.rs`. At startup, `ProcessorHandle::new` chooses `Sequential` or `Concurrent` based on `feature`. Incoming packets are directed by `process_packet` / `process_packet_blocking` either to a local processor worker or the connector depending on `operating_mode`. In sequential mode, a lane is one ingress queue owned by one worker, which keeps ordering for packets within that lane while allowing parallel processing across lanes. A sequential worker uses per-worker channels (`PacketReceiver::Sequential`) and deterministic lane mapping, so packets remain stable under the same `(flow_id, tree_id)` hashing. Concurrent mode instead uses one shared MPMC processor channel (`PacketReceiver::Concurrent`) with fan-in across all workers, so scheduling can interleave traffic across trees through one queue. At ingress, non-FEC traffic uses `flow_id.hash(num_lanes)`, while FEC traffic that carries a tree id uses `JumpHasher::slot((flow_id, tree_id), num_lanes)` from `Packet::lossless_fec_tree_id()`. Route lookup happens per-packet and still includes tree context through `RoutingTable::get_next_hops_by_flow_and_tree(flow_id, fec_tree_id, reporter)`; when `fec_tree_id` is absent, behavior stays on the legacy flow-only path. After route selection, packets are forwarded hop-by-hop, and multicast fan-out clones payloads to all next hops. Missing multicast control-tree routes follow a warning/drop path (`Dropping packet because multicast tree route is unknown`) to preserve control/data consistency. Finally, all control updates such as route installs, group routes, senders, flow weights, route pins, reporters, and lossless handles are broadcast and applied uniformly to every processor. In `OperatingMode::Max`, connector routing still uses flow-only path (`Connector::get_next_hop_by_flow`) because host destination, not tree metadata, drives connection selection there.

### Shared protocol (`messages`)

Controller and dataplane communication is defined once in `messages/src/lib.rs` via `DataplaneToController` and `ControllerToDataplane`, backed by shared types like `Flow`, `FlowSpec`, and route/group payloads. This keeps both services aligned around the same schema and prevents duplicated wire definitions from drifting.

### Python embedding path (`nextmini_py`)

`python-api/src/lib.rs` constructs a dataplane `Conductor` in-process, disables local TUN ingestion, and exposes send/receive/group/lossless helpers to Python. Use this path when Python application code (including PyTorch, RL, or multicast tools) needs direct packet injection while preserving the same Rust routing and control-plane behavior.

### End-to-end lifecycle

From startup through operation, the end-to-end lifecycle is:

- Controller starts and loads config.
- Dataplane nodes connect, receive `StartUp`, and install initial topology/routes.
- Controller pushes flow/group updates as config or database state changes.
- Nodes forward packets through local route tables using the selected transport stack.
- Optional Python consumers consume those deliveries through `nextmini_py` receivers.

For deployment workflows, use the examples under `/docs/examples/`.
