# Architecture Overview

Nextmini has two core runtime components:

- **Controller**: owns topology, routing tables, flow installation, and multicast group state (persisted in Postgres).
- **Dataplane node**: forwards traffic according to controller-installed routes and exposes multiple traffic “ingress” paths (TUN, user-space engines, and MAX). It can run as the `nextmini` binary (separate process/container) or be embedded in a Python process via `nextmini_py`.

The controller is typically a long-running service. Dataplane nodes are usually started for the lifetime of an experiment/job; when embedded, their lifecycle is tied to the Python process.

## Data paths at a glance

- **TUN (overlay) path**: run unmodified applications against virtual IPs (the common Docker examples use this).
- **User-space flows**: controller-managed engines that don't use TUN — [SmolTCP flows](smoltcp-flows.md) (synthetic TCP/UDP) and [lossless sessions](lossless-flows.md) (bulk transfer), also exposed via [Python dataplane API](python-api.md).
- **MAX / proxy flows**: connection-on-demand TCP streams plus optional SOCKS5 proxy ingress for external endpoints.

## Where to go next

- Configuration knobs: [Configuration Reference](config-reference.md)
- User-space engines: [SmolTCP flows](smoltcp-flows.md) and [Lossless flows](lossless-flows.md)
- SOCKS5 + MAX forwarding: [Proxy flows](proxy-flows.md)
- Multicast control-plane: [Multicast Groups](multicast-groups.md)
- Python embedding: [Python dataplane API](python-api.md)
- End-to-end bare-metal deployment: [Bare Metal](../examples/bare-metal.md)
