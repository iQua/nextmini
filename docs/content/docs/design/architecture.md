---
title: "Architectural Design"
description: "Nextmini: a High-Performance Network Emulation and Experimentation Testbed"
---

Nextmini is organized as a small workspace of focused crates: `controller/` for control-plane services (topology, routing, flow installation, multicast lifecycle, and Postgres integration), `dataplane/` (`nextmini` package) for node runtime behavior like network I/O, packet processing, scheduling, and lossless sessions, `messages/` for shared MessagePack protocol types, `python-api/` (`nextmini_py`) for PyO3 bindings, and `raptorq/` for the FEC primitives.

### Control plane (`controller`)

The controller accepts dataplane websocket connections, assigns startup parameters (`StartUp`), computes routes from configured topology or explicit route definitions, and pushes updates (`InstallRoutes`, `AddFlows`, multicast directory and route messages). Its most important modules are `controller/src/config.rs` for schema and config shape, `controller/src/db.rs` for Postgres persistence and notifications, and `controller/src/utils.rs` for route construction helpers, including multicast per-node route derivation.

### Dataplane (`nextmini`)

Each dataplane node runs an async actor-style runtime that coordinates transport servers and clients (`tcp`, `udp`, `quic`, and max-mode connection-on-demand), packet processing and scheduling, route table lookups with next-hop fan-out, and optional Python delivery plus lossless session execution. The runtime is assembled in `dataplane/src/node/conductor.rs`, uses protocol logic from `dataplane/src/node/network/*`, performs packet parsing and forwarding in `dataplane/src/node/processor.rs`, stores forwarding state in `dataplane/src/node/route.rs`, and owns session-sender/receiver runtime checks in `dataplane/src/node/session/*`.

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
