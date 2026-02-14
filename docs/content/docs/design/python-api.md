---
title: "Python dataplane API"
description: "Design-level guide to using nextmini_py for unicast, multicast, and lossless Python data paths."
---

`nextmini_py` lets Python code run the Rust dataplane in-process. It mirrors the same controller-to-dataplane behavior used in Rust binaries, while exposing a small set of Python classes that handle routing registration, unicast transport, multicast control, and lossless delivery without requiring a separate TUN workflow.

## Build and install `nextmini_py`

Build the extension against CPython 3.13, then install either a development build or a wheel:

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

## Runtime model

`Dataplane` owns the full in-process stack:

- It loads a node TOML config.
- It forces `enable_local_interface = false` so the embedded runtime stays in user-space API mode.
- It creates and runs the dataplane conductor in the same process.
- It wires the controller websocket and packet receive path used by all Python-facing callbacks.

Keep the `Dataplane` object alive while you are sending, receiving, or coordinating lossless sessions.

## Core types

`Dataplane` is the main entry point and also the home for control operations.

`PacketBuilder` helps build payloads incrementally before freezing them into immutable `PacketView` buffers.

`PacketReceiver` is the pull side for a flow registration, and each `recv` returns a `PayloadDelivery`.

`PayloadDelivery` contains flow metadata and a read-only payload (`payload`/`frozen_payload`).

## Buffer helpers

### PacketView

Use `PacketView` whenever you pass packet payloads into the dataplane.

- `PacketView(data)` or `PacketView.from_buffer(data)` copies bytes into the internal buffer.
- `read()` returns `bytes`.
- `slice(start, length=None)` returns a view slice with bounds checks.
- `__len__()` gives the current byte length.

### PacketBuilder

Use `PacketBuilder` when assembling bytes incrementally in Python.

- `PacketBuilder(size=4096)` reserves capacity.
- `write(bytes)` appends and returns appended length.
- `freeze()` returns an immutable `PacketView` that can be sent through `send_to_node`/`send_data`.
- `__len__()` returns the current staged length.

## Unicast path: send and receive packets directly

A common pattern is to register an incoming flow and then send messages by destination node.

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/node-config.toml")

rx = dp.register_receiver_from_node(src_node_id=1, src_port=40000, dst_port=50000)
view = nm.PacketView(b"hello")
dp.send_to_node(dst_node_id=2, frozen=view, src_port=40000, dst_port=50000)

delivery = rx.recv(timeout_ms=5_000)
if delivery:
    print(delivery.flow_id, len(delivery.payload))
```

- `Dataplane.register_receiver_from_node(src_node_id, src_port=None, dst_port=None)` subscribes to a flow using user-space ports.
- `Dataplane.send_to_node(dst_node_id, frozen, src_port=None, dst_port=None)` builds a TCP-style packet and injects it through the processor.
- `PacketReceiver.recv(timeout_ms=None)` blocks until a delivery arrives or timeout.
- `PacketReceiver.recv_async()` returns an awaitable `PayloadDelivery`.

If no packets arrive during `recv`, the method returns `None`.

## Multicast control plane workflow

This follows the controller-backed group model used by the rest of Nextmini:

1. Create a group: `create_group(label)`.
2. Wait for acknowledgement: `group_is_ready(timeout_ms=None)`.
3. Optionally install custom DAGs: `set_group_routes(group_id, edges)` where `edges` is a list of directed tuples.
4. Join/leave membership via `join_group(group_id)` and `leave_group(group_id)`.
5. Optionally block on membership and route propagation:
   - `wait_for_local_membership(group_id, timeout_ms=None)`
   - `wait_for_group_routes(group_id, src_node_id, min_routes=1, timeout_ms=None)`
   - `wait_for_topology_ready(timeout_ms=None)`

For data receive, use:

- `register_receiver_for_group(src_node_id, group_ip, src_port=None, dst_port=None)`

This produces the same `PacketReceiver` semantics as unicast and returns flow payload deliveries for multicast traffic.

### Multicast control-plane example

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/to/source-node.toml")
dp.wait_for_topology_ready(timeout_ms=30_000)

# 1) Create a group and wait for controller acknowledgement.
dp.create_group("training-run")
group_id, group_ip, group_src = dp.group_is_ready(timeout_ms=30_000)
print(f"group_id={group_id}, group_ip={group_ip}, src={group_src}")

# 2) Install explicit DAG routes if needed (source-directed and explicit node pairs).
dp.set_group_routes(group_id, [(group_src, 2), (2, 3), (2, 4)])

# 3) Block until routing materialization reaches local node before sending payloads.
if not dp.wait_for_group_routes(group_id, group_src, min_routes=1, timeout_ms=30_000):
    raise TimeoutError("group routes did not install in time")

# Optional control-channel registration path for application handshakes.
ctrl_rx = dp.register_receiver_for_group(src_node_id=group_src, group_ip=group_ip)
```

```python
import nextmini_py as nm

dp_recv = nm.Dataplane("/abs/path/to/receiver-node.toml")
dp_recv.wait_for_topology_ready(timeout_ms=30_000)
receiver_node = int(dp_recv.node_id)
group_id = 1
group_ip = "239.255.0.10"
print(f"receiver node id={receiver_node}")

# 1) Join the group before data session start.
dp_recv.join_group(group_id=group_id)

# 2) Register multicast payload delivery path.
payload_rx = dp_recv.register_receiver_for_group(src_node_id=1, group_ip=group_ip)
```

## Lossless session API (`send_data` / `receive_data`)

The lossless API is intentionally higher-level: callers pass payloads and expected sizes, while all coding/tuning policy stays in runtime config.

### Contract

- `send_data(group_id, dest_ip, receiver_ids, buffer, *, chunk_size=8500, src_port=None, dst_port=None) -> int`
- `receive_data(group_id, dest_ip, source_node_id, expected_bytes, *, chunk_size=8500, src_port=None, dst_port=None) -> int`
- `receive_data_async(group_id, dest_ip, source_node_id, expected_bytes, *, chunk_size=8500, src_port=None, dst_port=None) -> Awaitable[int]`

In practice, both sender and receiver compute deterministic session IDs from `(group_id, source_node_id)` so they match without manual session negotiation. A non-empty `buffer` and positive `chunk_size` are required on send. Receiver registration requires `expected_bytes > 0`.

Both `send_data` and receive methods return a `session_id`. Completion is tracked against this ID.

`lossless_wait(session_id, timeout_ms=None)` blocks until session completion and returns a boolean.
`lossless_wait_async(session_id, timeout_ms=None)` does the same in `await` form.

Once complete, call `get_data_buffer(session_id, consume=True)` to retrieve reconstructed bytes as a `PacketView`.
When `consume=True` (the default), the internal buffer is removed after retrieval.

### Lossless multicast workflow example

```python
import nextmini_py as nm

dp_src = nm.Dataplane("/abs/path/to/source-node.toml")
dp_src.wait_for_topology_ready(timeout_ms=30_000)
dp_src.create_group("weights-sync")
group_id, group_ip, src_node = dp_src.group_is_ready(timeout_ms=30_000)
dp_src.set_group_routes(group_id, [(src_node, 2), (src_node, 3)])

model_bytes = b"...serialized model shard..."
expected_bytes = len(model_bytes)
chunk_size = 8192
payload = nm.PacketBuilder(size=len(model_bytes))
payload.write(model_bytes)
view = payload.freeze()

session_id = dp_src.send_data(
    group_id=group_id,
    dest_ip=group_ip,
    receiver_ids=[2, 3],
    buffer=view,
    chunk_size=chunk_size,
)

ok = dp_src.lossless_wait(session_id, timeout_ms=60_000)
print(f"send session {session_id} finished={ok}")
```

```python
import nextmini_py as nm

dp_dst = nm.Dataplane("/abs/path/to/worker-node.toml")
dp_dst.wait_for_topology_ready(timeout_ms=30_000)
group_id = 1
group_ip = "239.255.0.10"
expected_bytes = 1_024_000
chunk_size = 8192
dp_dst.join_group(group_id=group_id)
receiver_id = 1

receive_sid = dp_dst.receive_data_async(
    group_id=group_id,
    dest_ip=group_ip,
    source_node_id=1,
    expected_bytes=expected_bytes,
    chunk_size=chunk_size,
)

ok = dp_dst.lossless_wait_async(receive_sid, timeout_ms=60_000)
if not ok:
    raise TimeoutError(f"lossless receive timed out for session {receive_sid}")

recovered = dp_dst.get_data_buffer(receive_sid, consume=True)
print(bytes(recovered.read()))
```


### One-file multicast sync example (source + receiver modes)


```python
import argparse
import nextmini_py as nm


def run_source(config_path: str, group_label: str) -> tuple[int, str, int]:
    dp = nm.Dataplane(config_path)
    dp.wait_for_topology_ready(timeout_ms=30_000)

    dp.create_group(group_label)
    group_id, group_ip, src_node = dp.group_is_ready(timeout_ms=30_000)
    dp.set_group_routes(group_id, [(src_node, 2), (src_node, 3)])
    if not dp.wait_for_group_routes(group_id, src_node, min_routes=1, timeout_ms=30_000):
        raise RuntimeError("multicast routes were not installed in time")

    payload = b"hello-multicast"
    view = nm.PacketView(payload)

    sid = dp.send_data(
        group_id=group_id,
        dest_ip=group_ip,
        receiver_ids=[2, 3],
        buffer=view,
        chunk_size=8500,
    )
    ok = dp.lossless_wait(sid, timeout_ms=60_000)
    print(f"[source] transfer={sid} ok={ok}")
    return group_id, group_ip, src_node


def run_receiver(
    config_path: str,
    group_id: int,
    group_ip: str,
    source_node_id: int,
    expected_bytes: int,
) -> bytes:
    dp = nm.Dataplane(config_path)
    dp.wait_for_topology_ready(timeout_ms=30_000)

    dp.join_group(group_id)
    sid = dp.receive_data(
        group_id=group_id,
        dest_ip=group_ip,
        source_node_id=source_node_id,
        expected_bytes=expected_bytes,
        chunk_size=8500,
    )
    ok = dp.lossless_wait(sid, timeout_ms=60_000)
    if not ok:
        raise TimeoutError(f"[receiver] transfer {sid} did not complete")

    payload = dp.get_data_buffer(sid, consume=True)
    return bytes(payload.read())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["source", "receiver"], required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--group-id", type=int, default=None)
    parser.add_argument("--group-ip", default=None)
    parser.add_argument("--source-node", type=int, default=1)
    parser.add_argument("--expected-bytes", type=int, default=14)
    args = parser.parse_args()

    if args.mode == "source":
        group_id, group_ip, src_node = run_source(args.config, "multicast-demo")
        print(
            f"share with receivers: --group-id {group_id} --group-ip {group_ip} --source-node {src_node}"
        )
        return

    if args.group_id is None or args.group_ip is None:
        raise ValueError("receiver mode requires --group-id and --group-ip")

    data = run_receiver(
        args.config,
        args.group_id,
        args.group_ip,
        args.source_node,
        args.expected_bytes,
    )
    print(f"received payload: {data!r}")


if __name__ == "__main__":
    main()
```
### Behavior details and guardrails

- Runtime control remains in `[lossless_runtime_config]` and controller/session negotiation.
- The sender/receiver path validates session preconditions at start, so failures appear early as runtime errors.

## Dataplane helper surfaces

`Dataplane.get_network_info()` returns a small dictionary with keys including:

- `node_id`
- `private_network_addr`
- `public_network_addr`
- `private_network_port`
- `public_network_port`
- `private_network_interface`
- `controller_addr`
- `user_space_address`

`Dataplane.node_id` is also exposed as a property.

## Troubleshooting

If Python receivers lag behind, increase `channel_capacity` in node config. Use `RUST_LOG=info` or `RUST_LOG=debug` when you need visibility into controller events, route pushes, and delivery diagnostics.

If `Dataplane(...)` fails, validate:

- the config path is correct,
- the extension was built for CPython 3.13,
- and the imported wheel path is the matching build for your environment.
