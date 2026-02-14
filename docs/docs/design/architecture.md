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

### Processor and Routing Path (Implementation)

The packet ingress and forwarding path is centralized in `dataplane/src/node/processor.rs`:

- `ProcessorHandle::new` selects a concrete mode (`Sequential` vs `Concurrent`) from `feature`.
- `process_packet`/`process_packet_blocking` compute destination locality and send packets either to a processor worker or the connector depending on `operating_mode`.
- A **lane** is one ingress queue slot owned by one worker in sequential mode; packets mapped to different lanes are processed in parallel workers, packets on the same lane preserve relative order.
- `Sequential` mode creates:
  - per-worker processor channels (`PacketReceiver::Sequential`) and
  - per-flow/per-tree deterministic lane selection.
- `Concurrent` mode creates:
  - one shared MPMC processor channel (`PacketReceiver::Concurrent`) and
  - worker fan-in across all processors.
- `SequentialProcHandle::select_processor_ingress_lane` maps each packet at ingress:
  - non-FEC packets use `flow_id.hash(num_lanes)`.
  - FEC packets use `JumpHasher::slot((flow_id, tree_id), num_lanes)` where `tree_id` comes from `Packet::lossless_fec_tree_id()`.
- `Concurrent` mode does not apply lane-level FEC partitioning: all packets go through one shared channel and scheduler order can interleave trees.
- Route resolution happens inside each worker on receive and includes tree context:
  - `RoutingTable::get_next_hops_by_flow_and_tree(flow_id, fec_tree_id, reporter)`.
  - if `fec_tree_id` is absent, behavior is backwards-compatible flow-only selection.
- Processed packets are forwarded hop-by-hop; for multicast fan-out, `Processor` clones the packet for all next hops after route selection.
- Unknown multicast control-tree / tree-specific misses emit a dedicated warn/drop path (`Dropping packet because multicast tree route is unknown`), matching control/data consistency goals without crashing packet flow.
- Runtime control updates (routing install, group routes, user-space senders, flow weights, route pins, reporters, lossless handle) are fan-out through a broadcast channel and applied by every processor instance.
- Connector path (`Connector::get_next_hop_by_flow`) still uses flow-only routing because max-mode connection selection is based on destination host and not per-tree FEC metadata.

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
