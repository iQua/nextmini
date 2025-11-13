# Python dataplane API

The `python-api/` crate builds the `nextmini_py` extension, letting Python workloads instantiate the dataplane in the
same process, inject buffers directly into the Rust routing stack, and subscribe to reconstructed payloads without going
through TUN. This note documents the shipped API surface, how it maps onto the Rust implementation, and the knobs that
govern fragmentation, telemetry, and group coordination.

## Building and installing `nextmini_py`

1. Install [maturin](https://github.com/PyO3/maturin) inside the target virtualenv.
2. Build or develop the wheel against CPython 3.13 (the bindings are compiled with `abi3-py313`):

   ```bash
   pip install maturin
   maturin develop --release -m python-api/Cargo.toml    # editable install while iterating
   # or:
   maturin build --release -m python-api/Cargo.toml
   pip install target/wheels/nextmini_py-*.whl
   ```

3. When running the Rust unit tests for this crate, disable the default extension module feature so the binary links
   against `libpython`:

   ```bash
   PYO3_PYTHON=/opt/homebrew/opt/python@3.13/bin/python3.13 \
   cargo nextest run -p nextmini_py --no-default-features --features dev-tests
   ```

## Public surface at a glance

| Python type | Key members | Notes |
| --- | --- | --- |
| `nextmini_py.Dataplane` | `send_to_node`, `send_to_ip`, `send_batch_to_node`, `register_receiver_from_node`, `register_receiver_for_group`, `create_group`, `join_group`, `leave_group`, `group_is_ready`, `wait_for_local_membership`, `wait_for_routes_installed`, `flow_id_from_nodes` | Embeds a Tokio runtime, spins up the Rust dataplane (`Conductor`), wires the Python delivery interface, and proxies controller RPCs for multicast helpers. |
| `nextmini_py.FrozenBuffer` | `__len__`, `read()`, `slice(start, length=None)` | Read-only wrapper around `bytes` that implements the Python buffer protocol so the Rust sender can copy exactly once into the `Packet`. |
| `nextmini_py.PacketReceiver` | `recv(timeout_ms=None)`, `recv_async()` | Waits for traffic on a specific flow. Returns `bytes` when the receiver was registered in raw mode or a `PayloadDelivery` object when `payload_only=True`. |
| `nextmini_py.PayloadDelivery` | `.payload`, `.flow_id`, `.src_ip`, `.dst_ip`, `.src_port`, `.dst_port`, `.message_id`, `.total_len`, `.fragment_count`, `.payload_format` | Metadata-rich wrapper populated when payload-only receivers are used. Fragmentation metadata is only set when the feature flag is enabled. |

## Dataplane lifecycle

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
```

Instantiating `Dataplane` loads the TOML config, disables the local TUN reader (`enable_local_interface = false`),
constructs the `Conductor`, and attaches the `PythonInterfaceHandle` plus controller bridge. The Rust dataplane continues
to run on background Tokio tasks until the `Dataplane` object is dropped or the host process exits. Each call to
`Dataplane` is isolated; if you need multiple simultaneous nodes you should spawn multiple processes rather than
multiple `Dataplane` objects inside one interpreter.

## Sending payloads

All outbound APIs accept a `FrozenBuffer`. Creating one from NumPy or PyTorch tensors looks like:

```python
import numpy as np
import nextmini_py as nm

def frozen_from_tensor(tensor) -> nm.FrozenBuffer:
    host_tensor = tensor.detach().contiguous().cpu()
    return nm.FrozenBuffer(host_tensor.numpy().tobytes())

dp = nm.Dataplane("/abs/path/to/node-config.toml")
payload = frozen_from_tensor(loss_tensor)
dp.send_to_node(dst_node_id=2, frozen=payload)
```

`send_to_node` synthesizes an IPv4/TCP tuple using the node ID and the user-space port range defined in the config. Use
`send_to_ip(dst_ip="10.0.0.42", frozen=payload)` when you already know the destination IP (e.g., multicast group
address). `send_batch_to_node(dst_node_id, [FrozenBuffer, …])` is a thin helper that iterates over the list and reuses
the same flow tuple.

### Frozen buffers in detail

`FrozenBuffer` keeps a reference-counted `Bytes` backing store so cloners are cheap. The object:

- Accepts any `bytes` value in its constructor.
- Implements `memoryview(frozen_buffer)`/`np.frombuffer(...)` via the Python buffer protocol.
- Provides `slice(start, length=None)` for zero-copy views into subranges.
- Supplies `read()` if you need an owned `bytes` copy on the Python side.

The Rust bindings treat a `FrozenBuffer` as immutable; if you need to mutate the payload, build a new instance.

## Receiving payloads

Register receivers per flow using the node ID (or group IP) of the expected sender:

```python
import numpy as np
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
rx = dp.register_receiver_from_node(
    src_node_id=1,
    payload_only=True,   # recommended
)

delivery = rx.recv(timeout_ms=5_000)
if delivery:
    arr = np.frombuffer(delivery.payload, dtype=np.float32)
    print("flow:", delivery.flow_id, "message:", delivery.message_id)
```

- `payload_only=True` delivers `PayloadDelivery` objects whose `.payload` is already stripped of IPv4/TCP headers, and
  whose metadata reflects the reconstructed message. Leave it at `False` for legacy tooling that expects raw packets.
- `register_receiver_for_group(src_node_id=…, group_ip="239.1.1.1", payload_only=True)` uses the same interface for
  multicast traffic.
- `PacketReceiver.recv(timeout_ms)` blocks until a payload becomes available or the deadline expires, returning `None`
  on timeout. `recv_async()` returns an awaitable compatible with `asyncio`.

## Flow and group helpers

The bindings expose a few controller-facing helpers so Python workloads can manage multicast membership without a
secondary CLI:

| Method | Description |
| --- | --- |
| `flow_id_from_nodes(src_node_id, dst_node_id, src_port=None, dst_port=None)` | Computes the `FlowId` used by sender/receiver helpers; useful when correlating controller logs. |
| `create_group(label)` | Requests a new multicast group through the controller. |
| `join_group(group_id)` / `leave_group(group_id)` | Adds or removes the local node from a multicast group. |
| `group_is_ready(timeout_ms=None)` | Waits for a `GroupCreated` event and returns `(group_id, group_ip, src_node_id)` when the controller finishes provisioning. |
| `wait_for_local_membership(group_id, timeout_ms=None)` | Resolves to `True` once the controller confirms the local node joined the group. |
| `wait_for_routes_installed(group_id, src_node_id=None, timeout_ms=None)` | Returns the next-hop fan-out for the requested group/source pair so scripts can confirm topology installation. |

Each method leverages the `PythonEvent` queue maintained inside the dataplane (`PythonInterfaceHandle`). Events are only
delivered to Python receivers that have called one of the waiters above; they are not broadcast globally.

## Payload size behavior

The bindings now ship every payload as a single TCP frame; there is no application-level fragmentation to configure. The operating system handles any link-layer segmentation, and the dataplane enforces the configured MTU when chunking reliable multicast transfers. This keeps the API predictable: the bytes you send are the bytes delivered.

## Telemetry and troubleshooting

- `PayloadDelivery` metadata mirrors the transport tuple plus optional `message_id`, `total_len`, `fragment_count`, and
  `payload_format` (`"payload"` vs `"raw_packet"`). Since fragmentation is gone the optional fields are always `None`, but
  they remain for backward compatibility with older tooling.
- The bindings log backpressure warnings if a receiver queue fills up. Increase `channel_capacity` in node configs or
  call `recv()` more aggressively when this appears.

## Worked example: gating PyTorch metrics on env vars

`examples/pytorch/gpt2.py` and other training scripts already include opt-in hooks. Set the following environment
variables before launching the job:

```bash
export NEXTMINI_CONFIG=/absolute/path/node-config.toml
export NEXTMINI_DST_NODE=2
```

Inside the script:

```python
import nextmini_py as nm

dp = nm.Dataplane(os.environ["NEXTMINI_CONFIG"])
tx = dp.send_to_node
rx = dp.register_receiver_from_node(
    src_node_id=int(os.environ["NEXTMINI_DST_NODE"]),
    payload_only=True,
)

frozen = nm.FrozenBuffer(loss_tensor.detach().contiguous().cpu().numpy().tobytes())
tx(dst_node_id=int(os.environ["NEXTMINI_DST_NODE"]), frozen=frozen)
maybe_delivery = rx.recv(timeout_ms=200)
```

Receivers can run in a separate process (using the same config) and call `dp.register_receiver_from_node` with the
sender’s node ID to ingest the stream.

## Packaging checklist

- Build wheels with `maturin build --release -m python-api/Cargo.toml`. The crate targets `abi3`, so one build works
  across CPython 3.13 patch releases.
- Document the path to the TOML config in deployment scripts (`NEXTMINI_CONFIG`).
- Keep `python-api/README.md` instructions handy for developers who need to run `cargo test` on Apple Silicon (where the
  linker requires `libpython` headers).
- When distributing examples or tools under `examples/**` and `tools/**`, import `nextmini_py` dynamically so they keep
  working in environments where the extension is optional.
