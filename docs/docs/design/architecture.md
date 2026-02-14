## Architectural Design

Nextmini is organized as a small workspace of focused crates:

- `controller/`: control-plane service (topology/routing, flow installation, multicast lifecycle, Postgres integration).
- `dataplane/` (`nextmini` package): node runtime (network I/O, packet processing, scheduling, routing tables, lossless sessions).
- `messages/`: shared MessagePack protocol types between controller and dataplane.
- `python-api/` (`nextmini_py`): PyO3 bindings that embed the dataplane runtime in a Python process.
- `raptorq/`: FEC primitives used by lossless session features.

### Control plane (`controller`)

The controller accepts dataplane websocket connections, assigns startup parameters (`StartUp`), computes routes from configured topology or explicit route definitions, and pushes updates (`InstallRoutes`, `AddFlows`, multicast directory/route messages).

Key modules:

- `controller/src/config.rs`: controller config schema (topology, routes, flow transport, multicast pool, DB settings).
- `controller/src/db.rs`: Postgres persistence and notification handling.
- `controller/src/utils.rs`: route construction helpers (including multicast per-node route derivation).

### Dataplane (`nextmini`)

Each dataplane node runs an async actor-style runtime around these responsibilities:

- transport servers/clients (`tcp`, `udp`, `quic`, and max-mode connection-on-demand),
- packet processing and scheduling,
- route table lookup and next-hop fan-out,
- optional Python delivery interface and lossless session runtime.

Key modules:

- `dataplane/src/node/conductor.rs`: top-level lifecycle and task orchestration.
- `dataplane/src/node/network/*`: protocol-specific networking.
- `dataplane/src/node/processor.rs`: packet parsing + route resolution + forwarding.
- `dataplane/src/node/route.rs`: route tables, multicast directory, and route cache.
- `dataplane/src/node/session/*`: lossless session sender/receiver and runtime checks.

### Shared protocol (`messages`)

Controller/dataplane communication uses `messages/src/lib.rs` enums (`DataplaneToController`, `ControllerToDataplane`) and shared data structures (`Flow`, `FlowSpec`, route/group payloads). This keeps both sides consistent without duplicated wire schemas.

### Python embedding path (`nextmini_py`)

`python-api/src/lib.rs` constructs a dataplane `Conductor` inside Python, disables local TUN ingestion for in-process use, and exposes send/receive/group/lossless helpers.

Use this path when application code (PyTorch/RL/multicast tools) should inject or receive payloads directly in Python while reusing the same Rust routing and control-plane behavior.

### End-to-end lifecycle

1. Controller starts and loads config.
2. Dataplane nodes connect, receive `StartUp`, and install initial topology/routes.
3. Controller pushes flow/group updates as config or DB state changes.
4. Dataplane forwards packets using local route tables and selected transport stack.
5. Optional Python consumers receive payload deliveries through `nextmini_py` receivers.

For deployment workflows, see the examples under `docs/examples/`.
