# Messages and protocol

The controller and dataplane communicate over a WebSocket control channel using **MessagePack** (via `rmp-serde`). Message definitions live in `messages/src/lib.rs`.

## Where messages are used

- Controller: `controller/src/main.rs` reads `DataplaneToController` messages and writes `ControllerToDataplane` messages.
- Dataplane: `dataplane/src/node/controller/interface.rs` drives the handshake, consumes controller updates, and reports events/metrics back to the controller.
- Python API: `python-api/src/lib.rs` sends a subset of dataplane→controller messages (multicast helpers) and consumes dataplane→Python deliveries.

## Handshake overview

1. Dataplane connects to the controller WebSocket.
2. Dataplane sends `DataplaneToController::StartUp` (addresses + optional node id).
3. Controller replies with `ControllerToDataplane::StartUp`, which includes:
   - assigned `node_id`
   - network base address ranges and `net_mask`
   - `max_server_port`, `protocol`, and `scheduler_type`
   - per-node `NodeSpec` (including `OperatingMode`)

After `StartUp`, the controller sends:

- `AddNode` / `AddNodeAddress` to establish neighbor links
- `InstallRoutes` to install the routing table
- optional `SetLinkRate` and `AddFlows`
- multicast directory/routes when enabled

## Key message types (high level)

### Dataplane → Controller (`DataplaneToController`)

- `StartUp`: announce node address info and request handshake state
- `Metrics`: periodic metrics updates
- `FlowFinished`: completion events for controller-managed flows
- `CreateGroup` / `JoinGroup` / `LeaveGroup` / `SetGroupRoutes`: multicast group lifecycle

### Controller → Dataplane (`ControllerToDataplane`)

- `StartUp`: handshake response including base addresses, protocol, scheduler
- `AddNode` / `AddNodeAddress`: neighbor connectivity for routing substrate
- `InstallRoutes`: unicast routes (route_id + next hops)
- `SetLinkRate`: link token bucket shaping
- `AddFlows`: controller-managed user-space flows
- `TopologyReady`: signal that all expected nodes have connected
- `InstallGroupDirectory` / `InstallGroupRoutes`: multicast directory and per-node fan-out routes

## Versioning and compatibility

Messages are strongly typed in Rust, but they are not currently versioned as an external stable protocol. When you update message definitions, you must update both controller and dataplane in lockstep (and rebuild the Python extension if it depends on those definitions).

