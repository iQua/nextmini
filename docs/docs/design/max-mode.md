# MAX mode

MAX mode is a dataplane forwarding mode that routes **TUN-originated** TCP traffic through connection-on-demand “MAX” TCP streams between dataplane nodes (instead of the normal per-packet forwarding pipeline).

It is enabled per node via the controller’s node spec:

```toml
[[nodes]]
node_id = 1
operating_mode = "max"
```

Implementation notes:

- MAX transport server/client: `dataplane/src/node/network/tcp_max.rs`
- Connection-on-demand + stream splicing: `dataplane/src/node/connector.rs`

Proxy flows (SOCKS5 ingress) are also built on the MAX transport; see [Proxy flows (MAX mode + SOCKS5)](proxy-flows.md).
