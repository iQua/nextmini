---
title: "Python API"
description: "User-facing nextmini_py documentation for Python applications."
---

`nextmini_py` is the Python surface for running the Rust dataplane inside the same process. It is meant for applications that want direct packet send/receive, multicast control, and optional lossless sessions without managing a separate TUN workflow.

## Build and install

Install with CPython 3.13 and build either a develop wheel or a release wheel.

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

## Start a dataplane object

The constructor takes a node TOML path and returns a live runtime handle.

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
print(dp.node_id)
```

Keep the object alive while sending, receiving, or coordinating sessions.

## Core classes

`Dataplane` is the entry point.  
`PacketBuilder` builds mutable payload buffers and converts them into `PacketView`.  
`PacketView` is the immutable payload object used by the send/receive APIs.  
`PacketReceiver` is returned by flow registration calls and yields `PayloadDelivery` objects through `recv` and `recv_async`.

## Unicast send and receive

Register a receiving flow, send a payload to a destination node, then wait for delivery.

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")
receiver = dp.register_receiver_from_node(src_node_id=1, src_port=40000, dst_port=50000)

payload = nm.PacketView(b"hello")
dp.send_to_node(dst_node_id=2, frozen=payload, src_port=40000, dst_port=50000)

delivery = receiver.recv(timeout_ms=5_000)
if delivery is None:
    print("timed out")
else:
    print(delivery.src_ip, delivery.src_port, len(delivery.payload))
```

`register_receiver_from_node` subscribes to a `(src_node_id, src_port, dst_port)` flow definition.  
`send_to_node` injects a `PacketView` for TCP-style payload traffic.  
`recv(timeout_ms=None)` blocks until a packet arrives or times out; `recv_async()` returns an awaitable receiver.

## Multicast control

Group operations go through the same controller-facing dataplane path used by native binaries.

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/source-node.toml")
dp.wait_for_topology_ready(timeout_ms=30_000)

dp.create_group("training-run")
group_id, group_ip, src_node = dp.group_is_ready(timeout_ms=30_000)

dp.set_group_routes(group_id, [(src_node, 2), (2, 3)])
if not dp.wait_for_group_routes(group_id, src_node, min_routes=1, timeout_ms=30_000):
    raise TimeoutError("routes were not installed in time")

control_rx = dp.register_receiver_for_group(src_node_id=src_node, group_ip=group_ip)
```

Use this pattern for source-side orchestration:
1. create a group
2. wait for group assignment
3. optionally set group routes
4. wait until routes are visible locally

Receivers use `join_group(group_id)` to become members, register a payload receiver with `register_receiver_for_group`, and optionally wait with `wait_for_local_membership`.

`wait_for_topology_ready(timeout_ms=None)` gates control-plane readiness before traffic start.

## Lossless sessions

Lossless workflows are grouped around session IDs returned by the start calls.

```python
import nextmini_py as nm

dp_src = nm.Dataplane("/abs/path/to/source-node.toml")
dp_src.wait_for_topology_ready(timeout_ms=30_000)
dp_src.create_group("weights-sync")
group_id, group_ip, src_node = dp_src.group_is_ready(timeout_ms=30_000)
dp_src.set_group_routes(group_id, [(src_node, 2), (2, 3)])

view = nm.PacketBuilder(size=4096)
view.write(b"...serialized payload...")
frozen = view.freeze()

send_sid = dp_src.send_data(
    group_id=group_id,
    dest_ip=group_ip,
    receiver_ids=[2, 3],
    buffer=frozen,
    block_size=8192,
)
print(dp_src.lossless_wait(send_sid, timeout_ms=60_000))
```

```python
import asyncio
import nextmini_py as nm

dp_dst = nm.Dataplane("/abs/path/to/receiver-node.toml")
dp_dst.wait_for_topology_ready(timeout_ms=30_000)
dp_dst.join_group(group_id=1)
dp_dst.set_group_routes(group_id=1, edges=[(1, 2), (2, 3)])

recv_sid = dp_dst.receive_data(
    group_id=1,
    source_node_id=1,
)

async def wait_for_session() -> None:
    ok = await dp_dst.lossless_wait_async(recv_sid, timeout_ms=60_000)
    if not ok:
        raise TimeoutError(f"lossless receive did not complete: {recv_sid}")

asyncio.run(wait_for_session())

payload = dp_dst.get_data_buffer(recv_sid, consume=True)
payload_bytes = bytes(payload.read())
print(len(payload_bytes), payload_bytes)
```

This is an intentional breaking change in the Python receive API: `receive_data(...)` and `receive_data_async(...)` no longer accept `expected_bytes` or a receiver-side `block_size`.

The sender fixes `total_bytes` and `block_size` in the first manifest for a session. Receivers only register by `(group_id, source_node_id)` plus optional port overrides, then read the reconstructed bytes from `get_data_buffer(...)`.

The core methods are `send_data`, `receive_data`, `receive_data_async`, `lossless_wait`, `lossless_wait_async`, and `get_data_buffer`.

## Packet buffers

Use `PacketBuilder` when assembling payload data in pieces.

`PacketBuilder(size=4096)` creates a mutable staging buffer.  
`write()` appends bytes, and `freeze()` creates a sendable `PacketView`.  
`PacketView(data)` or `PacketView.from_buffer(data)` creates an immutable payload.  
`slice()` returns a subview, while `read()` exposes bytes and `__len__()` reports length.

## Troubleshooting

`get_network_info()` returns resolved runtime networking details such as local node ID, controller address, and the active user-space address.

If no packets arrive, confirm the following:
- `Dataplane(...)` loaded a valid config
- the controller process is reachable
- `channel_capacity` and `operating_mode` fit your workload
- runtime logs are on with `RUST_LOG=info` or `RUST_LOG=debug`
