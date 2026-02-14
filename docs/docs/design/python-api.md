# Python dataplane API

The `python-api/` crate builds the `nextmini_py` extension. It lets Python workloads run the Rust dataplane in-process, send payloads directly through the routing stack, and subscribe to delivered payloads without reading from TUN.

## Build and install `nextmini_py`

1. Install `maturin` in your Python environment.
2. Build against CPython 3.13 (`abi3-py313` is enabled in `python-api/Cargo.toml`).

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

## Run crate tests

When running Rust tests for `nextmini_py`, point PyO3 at Python 3.13 and disable the default extension-module build mode:

```bash
PYO3_PYTHON=/opt/homebrew/opt/python@3.13/bin/python3.13 \
cargo nextest run -p nextmini_py --no-default-features --features dev-tests
```

## Public API surface

| Python type | Key members | Notes |
| --- | --- | --- |
| `nextmini_py.Dataplane` | `send_to_node`, `register_receiver_from_node`, `register_receiver_for_group`, `create_group`, `join_group`, `leave_group`, `group_is_ready`, `wait_for_local_membership`, `set_group_routes`, `wait_for_group_routes`, `wait_for_topology_ready`, `send_data`, `receive_data`, `receive_data_async`, `lossless_wait`, `lossless_wait_async`, `get_data_buffer`, `get_network_info`, `node_id` | Owns the embedded dataplane runtime and controller bridge. |
| `nextmini_py.PacketView` | `__len__`, `read()`, `slice(start, length=None)` | Immutable bytes view with Python buffer protocol support. |
| `nextmini_py.PacketBuilder` | `write(bytes)`, `freeze()` | Mutable builder that produces a `PacketView`. |
| `nextmini_py.PacketReceiver` | `recv(timeout_ms=None)`, `recv_async()` | Receives `PayloadDelivery` objects for a registered flow. |
| `nextmini_py.PayloadDelivery` | `.payload`, `.frozen_payload`, `.flow_id`, `.src_ip`, `.dst_ip`, `.src_port`, `.dst_port`, `.message_id`, `.total_len`, `.fragment_count` | Payload bytes plus flow metadata. |

## Dataplane lifecycle

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
```

`Dataplane(...)` loads the TOML config, forces `enable_local_interface = false`, starts the Rust conductor/runtime, and wires Python delivery + controller events. Keep the `Dataplane` instance alive while traffic is active.

## Unicast payload send/receive

```python
import nextmini_py as nm

def packet_view_from_tensor(tensor) -> nm.PacketView:
    arr = tensor.detach().contiguous().cpu().numpy()
    return nm.PacketView(arr.tobytes())

payload = packet_view_from_tensor(loss_tensor)
dp = nm.Dataplane("/abs/path/to/node-config.toml")

# Send to node ID 2 using default user-space ports from config
dp.send_to_node(dst_node_id=2, frozen=payload)

# Receive from source node ID 1
rx = dp.register_receiver_from_node(src_node_id=1)
delivery = rx.recv(timeout_ms=5_000)
if delivery:
    print(delivery.flow_id, len(delivery.payload))
```

`register_receiver_for_group(src_node_id=..., group_ip="239.1.1.10")` is the multicast equivalent.

## Multicast group helpers

Use controller-backed helpers from Python when managing group lifecycle:

- `create_group(label)`
- `group_is_ready(timeout_ms=None)`
- `set_group_routes(group_id, edges)`
- `join_group(group_id)` / `leave_group(group_id)`
- `wait_for_local_membership(group_id, timeout_ms=None)`
- `wait_for_group_routes(group_id, src_node_id, min_routes=1, timeout_ms=None)`
- `wait_for_topology_ready(timeout_ms=None)`

## Lossless session helpers (`send_data` / `receive_data`)

`send_data(...)` and `receive_data(...)` expose lossless session APIs for larger multicast transfers. Optional FEC arguments on sender/receiver calls map to runtime preflight checks in Rust.

```python
sid = dp.send_data(
    group_id=7,
    dest_ip="239.255.0.10",
    receiver_ids=[2, 3],
    buffer=payload,
    chunk_size=8500,
    fec_enabled=True,
    fec_tree_ids=[1, 3],
)

rx_sid = dp.receive_data(
    group_id=7,
    dest_ip="239.255.0.10",
    source_node_id=1,
    expected_bytes=len(payload),
    fec_enabled=True,
)
```

Use `lossless_wait(...)` / `lossless_wait_async(...)` to block on completion, then `get_data_buffer(session_id)` on receivers to retrieve reconstructed bytes.

## Payload metadata behavior

Python deliveries are payload-only (TCP/IP headers stripped). Metadata fields `message_id`, `total_len`, and `fragment_count` are reserved compatibility fields and are currently `None` in the payload delivery path.

## Script integration pattern

Environment variables like `NEXTMINI_CONFIG` and `NEXTMINI_DST_NODE` are conventions used by your scripts, not variables consumed directly by the Rust binaries. A common opt-in pattern is:

```python
import os
import nextmini_py as nm

dp = None
dst = os.getenv("NEXTMINI_DST_NODE")
config = os.getenv("NEXTMINI_CONFIG")

if dst and config:
    dp = nm.Dataplane(config)

# Later, only publish telemetry when configured
if dp is not None:
    payload = nm.PacketView(loss_tensor.detach().contiguous().cpu().numpy().tobytes())
    dp.send_to_node(dst_node_id=int(dst), frozen=payload)
```

For working end-to-end Python dataplane integrations in this repo, see `examples/rl/src/trainer.py`, `examples/rl/src/worker.py`, and `examples/multicast-docker/scripts/multicast_node.py`.

## Troubleshooting

- Increase `channel_capacity` in node config if Python receivers fall behind.
- Enable `RUST_LOG=info` (or `debug`) to inspect controller events and packet handling.
- If `Dataplane(...)` fails in Python, verify the config path and that the wheel was built for Python 3.13.
