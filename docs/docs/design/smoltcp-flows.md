# SmolTCP flows

SmolTCP flows are **controller-managed user-space TCP flows** executed inside the dataplane process (no application traffic required).

## Enabling SmolTCP flows

- Controller default: `flow_transport = "tcp"`
- Per-flow override: `flow_spec.transport = "tcp"`

Example:

```toml
flow_transport = "tcp"

[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000, flow_weight = 1 }
```

## Where it lives

- SmolTCP engine: `dataplane/src/node/flow/*`

For a runnable scenario, see [SmolTCP flows (example)](../examples/smoltcp-flows.md). For all config fields, see the [Configuration Reference](config-reference.md).
