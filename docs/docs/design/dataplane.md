# Dataplane node

A Nextmini dataplane node forwards traffic according to controller-installed routes. It supports multiple ingress paths:

- **TUN (overlay)**: run unmodified applications against virtual IPs.
- **User-space engines**: SmolTCP flows and lossless sessions for controller-managed traffic.
- **MAX / proxy**: connection-on-demand TCP forwarding and optional SOCKS5 ingress.
- **Python embedding** (optional): in-process send/receive via `nextmini_py`.

## Responsibilities

- Establish neighbor connections based on controller topology (`AddNode` / `AddNodeAddress`).
- Maintain routing state (unicast routes and multicast group routes).
- Forward packets hop-by-hop through the scheduler pipeline.
- Optionally run user-space flow engines and lossless sessions.

## Lifecycle

At a high level, a node does the following:

1. Load local config (`dataplane/src/node/config.rs`).
2. Connect to the controller over WebSocket and complete the handshake.
3. Receive base address ranges, protocol mode, scheduler type, and operating mode.
4. Establish neighbor links and install routing tables.
5. Start forwarding traffic and executing controller-managed flows.

The main runtime entry is `dataplane/src/node/conductor.rs`.

## Packet ingress paths

### TUN (overlay)

When `enable_local_interface = true`, the node reads packets from a TUN device and injects them into the processor pipeline.

This is how the standard Docker examples run tools like `ping` and `iperf3`.

### Network interfaces (node-to-node)

Neighbor connections use one of the configured transports (`Protocol::Tcp`, `Protocol::Quic`, `Protocol::Udp`). Packets arriving on a neighbor link are parsed into `Packet` objects and passed to the processor.

### Python interface (optional)

When built with the `python-extension` feature and run via `nextmini_py`, the node can deliver payloads directly to Python receivers without going through TUN.

See: [Python dataplane API](python-api.md).

### User-space flows and lossless sessions

The controller can install flows that are generated and consumed entirely in user space:

- SmolTCP TCP flows (`FlowTransport::Tcp`)
- Lossless unicast sessions (`FlowTransport::LosslessUnicast`)

See: [SmolTCP flows](smoltcp-flows.md) and [Lossless flows](lossless-flows.md).

### MAX / SOCKS5 proxy ingress

The MAX server listens on `max_server_port` and accepts:

- SOCKS5 `CONNECT` requests from external clients
- MAX header connections from other dataplane nodes

See: [Proxy flows (MAX + SOCKS5)](proxy-flows.md).

## Forwarding pipeline (conceptual)

Most traffic ultimately flows through the same conceptual pipeline:

1. **Ingress** (TUN, neighbor link, Python, user-space engine)
2. **Processor**: route lookup and packet cloning (for multicast)
3. **Scheduler**: per-next-hop pacing/queueing, optional link shaping
4. **Egress** (neighbor link or local delivery)

Route selection uses a consistent-hash mapping from flow IDs to available `route_id`s.

See: [Routing Internals](routing.md).

## Operating modes (Normal vs Max)

- `OperatingMode::Normal`: the processor forwards packets through the standard scheduler path.
- `OperatingMode::Max`: outbound traffic uses the MAX connector (connection-on-demand TCP streams) for higher throughput.

Proxy flows always use the MAX path because they start as inbound MAX/SOCKS5 connections.

