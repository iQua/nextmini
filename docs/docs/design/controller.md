# Controller (control plane)

The Nextmini controller owns **topology**, **routing**, **flow installation**, and **multicast group state**. It persists state in Postgres and pushes updates to dataplane nodes over a WebSocket control channel.

## Responsibilities

- Accept dataplane node connections (WebSocket).
- Assign node IDs and hand out base address ranges.
- Generate and distribute unicast routes and topology neighbor links.
- Apply link rate limits and flow definitions from config/DB.
- Manage multicast groups: allocation, membership, and multicast route distribution.

## How it is configured

The controller reads a TOML config file via `controller/src/config.rs`.

Notes about filenames:

- The controller binary defaults to reading `config.toml` in its working directory.
- Many examples call the file `controller-config.toml` and mount it into the container as `/var/nextmini/config.toml`.

The config includes:

- `[topology]` (preset or explicit edges) to define neighbor connectivity.
- `[routing]` + `[[routes]]` to define route generation or custom routes.
- `[[link_rates]]` and `[[flows]]` to seed emulation state.
- multicast pool settings (`multicast_pool_base` / `multicast_pool_mask`).
- Postgres settings in `[db]`.

See: [Configuration Reference](config-reference.md).

## Control channel and message format

The controller and dataplane exchange MessagePack-encoded messages (`rmp-serde`) defined in `messages/src/lib.rs`.

Key message flows:

- **Dataplane → Controller**: `StartUp`, `Metrics`, `FlowFinished`, multicast group requests.
- **Controller → Dataplane**: `StartUp`, `AddNode`, `InstallRoutes`, `AddFlows`, multicast directory/routes.

See: [Messages and protocol](messages.md).

## Topology vs routing

Nextmini separates:

- **Topology edges**: which nodes should establish neighbor connections (`AddNode`).
- **Routes**: which next hops are valid for a `(src, dst)` pair (`InstallRoutes`).

If a route uses an edge that is not present in the topology, dataplane nodes will not have a neighbor link to forward to.

For a user-facing route guide, see: [Defining the Network Topology and Routes](../examples/routes.md).

## Database model (high level)

The controller stores state in Postgres:

- In Docker examples, `controller/init.sql` is mounted into the Postgres container to create the `nextmini` database and set ownership/privileges.
- Table schema is managed by SQLx migrations under `controller/migrations/`.
- By default, the controller **resets** the database on startup for dev/test. To preserve state, set `CONTROLLER_RESET_DB=0` (or any value other than `1/true/yes`).

- node liveness and addresses
- routes (unicast) and optional link rate caps
- controller-managed flows and their status
- multicast groups, members, and cached DAG edges

The controller uses Postgres notifications to trigger re-sync tasks (routes, flows, group membership).

## Multicast groups

Multicast is a controller-owned feature:

1. A source calls `CreateGroup`.
2. The controller allocates `(group_id, group_ip)` from the multicast pool and replies with `GroupCreated`.
3. The source (or an external solver) calls `SetGroupRoutes` with a DAG edge list.
4. Members call `JoinGroup` / `LeaveGroup`.
5. Membership changes trigger a route rebuild; the controller pushes `InstallGroupRoutes` to affected nodes.

See: [Multicast Groups](multicast-groups.md) and [Example: Multicast Flow Lifecycle](../examples/multicast-flow.md).
