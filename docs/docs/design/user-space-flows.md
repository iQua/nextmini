# User-space flows

Nextmini supports both **kernel-backed traffic** (via a TUN interface) and **user-space traffic** generated or injected directly inside the dataplane process. This page explains the user-space paths, including the SmolTCP flow engine and the lossless session subsystem.

## When to use user-space flows

Use user-space flows when you want one of the following:

- **Synthetic traffic generation** driven by controller config (`[[flows]]`) without running an application inside a container/VM.
- **In-process payload injection** from Python via `nextmini_py` (send/receive bytes without TUN).
- **Large, lossless transfers** (e.g., model weights) with explicit chunking/pacing.

If you want to run unmodified tools like `ping`, `iperf3`, `curl`, or distributed training frameworks over a virtual network, use the TUN-based path instead (the Docker examples do this by default).

## Address spaces (TUN vs user-space vs external)

At startup, the controller sends three base address ranges to every dataplane node:

- **TUN (overlay) network**: `virtual_base_addr` (default `10.0.0.0/16`)
- **User-space network**: `user_space_base_addr` (default `192.168.0.0/16`)
- **External network**: `external_base_addr` (default `172.16.8.3/16` in the Docker examples)

Each node derives its per-network IPs from its `node_id` and the controller-provided netmask. The user-space paths discussed below use the **user-space network**, not the TUN interface.

## SmolTCP flows (controller `FlowTransport::Tcp`)

Controller-managed `[[flows]]` are executed by a user-space TCP engine built on SmolTCP:

- **Where it lives**: `dataplane/src/node/flow/*`
- **What it does**: runs a user-space TCP client/server pair and drives traffic according to `flow_len`, `flow_rate`, and `flow_weight`.
- **How it’s identified**: flows are mapped to `(src_node_id, dst_node_id)` pairs, then routed using the same controller-installed routes as TUN traffic.

In controller config, flows look like:

```toml
[[flows]]
src_node_id = 1
dst_node_id = 2
flow_spec = { flow_len = { Bytes = 10_000_000 }, flow_rate = 10_000_000, flow_weight = 1, transport = "tcp" }
```

Notes:

- `flow_len` can be `Bytes = N` or `Duration = seconds`.
- For `Duration` flows, `flow_rate` must be set (bytes/second).
- The controller can set a default transport via `flow_transport`, and individual flows can override it via `flow_spec.transport`.

## Lossless flows (controller `FlowTransport::LosslessUnicast`)

Lossless flows use the session subsystem to reliably deliver a fixed byte count while optionally pacing throughput:

- **Where it lives**: `dataplane/src/node/session/*`
- **How it’s enabled**:
  - controller-wide: `flow_transport = "lossless_unicast"`
  - per-flow: `flow_spec.transport = "lossless_unicast"`
- **How it’s paced**: `flow_rate` and/or token buckets (see [Lossless Session Configuration](lossless_config.md)).

Lossless sessions are useful when you need deterministic “deliver exactly N bytes” behavior (for example, model checkpoint broadcast) without relying on application behavior.

## Python user-space injection (`nextmini_py`)

The `nextmini_py` bindings use the user-space network to send and receive bytes in-process:

- **Small messages**: `send_to_node` + `register_receiver_from_node`
- **Large payloads**: `send_data` / `receive_data` + `lossless_wait` + `get_data_buffer`

See [Python dataplane API](python-api.md) and [Python API quickstart](../examples/pytorch_python_api.md) for the concrete call patterns.

