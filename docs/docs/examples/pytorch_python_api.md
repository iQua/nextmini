# PyTorch + Nextmini Python API quickstart

This guide shows how to embed the Nextmini dataplane inside a Python process using the `nextmini_py` extension (described in [Python dataplane API](../design/python-api.md)), then send and receive payloads without going through TUN.

## Contents

- Install the extension (`maturin`)
- Start a dataplane node from Python
- Send/receive small payloads (`send_to_node`)
- Send/receive large payloads losslessly (`send_data` / `receive_data`)

## 1) Build and install `nextmini_py`

The wheel targets CPython 3.13 (`abi3-py313`).

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml    # editable install
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

## 2) Start the dataplane from Python

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
```

The constructor spawns the Rust dataplane on an embedded Tokio runtime and wires the Python delivery interface. Keep the `Dataplane` object alive as long as you need to send/receive.

## 3) Send small payloads (one payload per call)

`send_to_node` sends the provided bytes as a single user-space TCP payload.

```python
import torch
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")

tensor = torch.randn(1024, dtype=torch.float32)
payload = nm.PacketView(tensor.detach().cpu().numpy().tobytes())
dp.send_to_node(dst_node_id=2, frozen=payload)
```

If you need custom ports (for multiplexing multiple logical streams), pass `src_port=` and `dst_port=` to both the sender and receiver registration calls.

## 4) Receive payloads with metadata

Register a per-flow receiver and poll for deliveries:

```python
import pickle
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
rx = dp.register_receiver_from_node(src_node_id=1)

delivery = rx.recv(timeout_ms=2_000)
if delivery:
    msg = pickle.loads(delivery.payload)
    print("flow_id:", delivery.flow_id, "bytes:", len(delivery.payload))
```

`PayloadDelivery` includes the 5‑tuple (`src_ip`, `dst_ip`, `src_port`, `dst_port`) plus `flow_id`. Fragmentation metadata fields exist for backward compatibility but are currently always `None`.

## 5) Lossless transfer for large buffers (multicast helper)

For large payloads (for example model weights), use the lossless session helpers:

- Sender: `send_data(...)` → returns `session_id`, then `lossless_wait(session_id)`
- Receiver: `receive_data(...)` → returns `session_id`, then `lossless_wait(session_id)` and finally `get_data_buffer(session_id)`

Sender example (multicast):

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
dp.create_group("job-42")
group = dp.group_is_ready(timeout_ms=5_000)
assert group
group_id, group_ip, src_node_id = group

receiver_ids = [2, 3]  # member node IDs
edges = [(src_node_id, rid) for rid in receiver_ids]
dp.set_group_routes(group_id, edges)
dp.wait_for_group_routes(group_id, src_node_id, timeout_ms=5_000)

buf = b"..."  # large payload
builder = nm.PacketBuilder(size=len(buf))
builder.write(buf)
view = builder.freeze()

sid = dp.send_data(group_id=group_id, dest_ip=group_ip, receiver_ids=receiver_ids, buffer=view)
ok = dp.lossless_wait(sid, timeout_ms=60_000)
assert ok
```

Receiver example (run on each receiver node):

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
dp.join_group(group_id)  # group_id/group_ip/src_node_id come from the sender (out of band)
dp.wait_for_local_membership(group_id, timeout_ms=5_000)
sid = dp.receive_data(group_id=group_id, dest_ip=group_ip, source_node_id=src_node_id, expected_bytes=123456)
ok = dp.lossless_wait(sid, timeout_ms=60_000)
payload = dp.get_data_buffer(sid)  # PacketView
```

For a concrete end-to-end usage, see `examples/rl/src/trainer.py` (control messages via `send_to_node`, weight broadcast via `send_data`) and the matching `examples/rl/src/worker.py`.
