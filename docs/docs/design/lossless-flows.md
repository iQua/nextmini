# Lossless flows

Lossless flows use the session subsystem to **deliver exactly `flow_len` bytes** (optionally paced) between a source and destination node.

## Enabling lossless flows

- Controller default: `flow_transport = "lossless_unicast"`
- Per-flow override: `flow_spec.transport = "lossless_unicast"`

Example:

```toml
flow_transport = "lossless_unicast"

[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000 }
```

## Where it lives

- Lossless sessions: `dataplane/src/node/session/*`

Runtime knobs live under `[lossless_runtime_config]`; see [Lossless Session Configuration](lossless_config.md). For a runnable scenario, see [Lossless flows (example)](../examples/simple-flow.md). For all config fields, see the [Configuration Reference](config-reference.md).
