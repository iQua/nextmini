# Architecture overview

Nextmini has two long-running components:

- **Controller**: owns topology, routing tables, flow installation, and multicast group state (persisted in Postgres).
- **Dataplane node**: forwards traffic according to controller-installed routes and exposes multiple traffic “ingress” paths (TUN, user-space engines, and MAX).

## Data paths at a glance

- **TUN (overlay) path**: run unmodified applications against virtual IPs (the common Docker examples use this).
- **User-space flows**: SmolTCP-based flow engine and lossless sessions used for controller-managed synthetic traffic and Python in-process injection.
- **MAX / proxy flows**: connection-on-demand TCP streams plus optional SOCKS5 proxy ingress for external endpoints.

## Where to go next

- Configuration knobs: [Configuration Reference](config-reference.md)
- User-space engines: [User-space flows](user-space-flows.md)
- SOCKS5 + MAX forwarding: [Proxy flows](proxy-flows.md)
- Multicast control-plane: [Multicast Groups](multicast-groups.md)
- Python embedding: [Python dataplane API](python-api.md)
- End-to-end bare-metal deployment: [Bare Metal](../examples/bare-metal.md)
