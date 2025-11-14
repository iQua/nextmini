# PyTorch + Nextmini Python API quickstart

This walk-through shows how to ship PyTorch tensors directly through the Nextmini dataplane using the `nextmini_py` extension described in [`docs/docs/design/python-api.md`](../design/python-api.md). It complements the hooks already present in `examples/pytorch/gpt2.py`, `examples/ml-tensor-multicast`, and the SBA demos.

## 1. Build and install the extension

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

The build must target CPython 3.13 because the crate ships as `abi3-py313`. Installing the wheel makes the `nextmini_py` module available to your virtualenv.

## 2. Bring up the dataplane from Python

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
```

The constructor embeds a Tokio runtime, spawns the Rust dataplane, and connects the Python delivery interface to the controller. Keep the `Dataplane` object alive for as long as you need to send/receive traffic.

## 3. Send tensors with `FrozenBuffer`

Wrap payloads in a `FrozenBuffer` before calling `send_to_node`. The helper accepts any `bytes` object, implements the buffer protocol, and exposes `slice()`/`read()` for convenience.

```python
import nextmini_py as nm

def frozen_from_tensor(tensor):
    arr = tensor.detach().contiguous().cpu().numpy()
    return nm.FrozenBuffer(arr.tobytes())

dp = nm.Dataplane("/abs/path/node-config.toml")
payload = frozen_from_tensor(loss_tensor)
dp.send_to_node(dst_node_id=2, frozen=payload)
```

## 4. Receive payloads with metadata

Register receivers per flow or per multicast IP. Pass `payload_only=True` (recommended) to receive the
`PayloadDelivery` metadata that records the flow tuple, optional fragment info, and reconstructed bytes.

```python
import numpy as np
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
rx = dp.register_receiver_from_node(
    src_node_id=1,
    payload_only=True,
)

delivery = rx.recv(timeout_ms=2_000)
if delivery:
    arr = np.frombuffer(delivery.payload, dtype=np.float32)
    print(
        f"flow={delivery.flow_id} message_id={delivery.message_id} len={delivery.total_len}"
    )
else:
    print("receiver timed out")
```

Use `register_receiver_for_group(src_node_id=…, group_ip="239.1.1.10", payload_only=True)` when subscribing to multicast flows. `PacketReceiver.recv_async()` integrates with `asyncio` if you prefer an async consumer.

## 5. Wire PyTorch training scripts behind env vars

`examples/pytorch/gpt2.py` already gates the Python bridge behind two environment variables so you can opt in per run:

```bash
export NEXTMINI_CONFIG=/absolute/path/node-config.toml
export NEXTMINI_DST_NODE=2         # numeric node id to target
python examples/pytorch/gpt2.py --num-epochs 1
```

When both variables are set the script:

1. Imports `nextmini_py`.
2. Instantiates `Dataplane(NEXTMINI_CONFIG)`.
3. Wraps each loss tensor in a `FrozenBuffer`.
4. Calls `send_to_node(dst_node_id=NEXTMINI_DST_NODE, frozen=payload)` on every step.

On the destination node, start a second Python process with the same config and call `register_receiver_from_node(src_node_id=<source>, payload_only=True)` to collect the metrics. The helper functions reuse the same routing tables as the Rust dataplane, so unicast, multicast, and QoS policies apply automatically.

## 6. Payload delivery semantics

Payloads now traverse the dataplane as single frames—the OS networking stack handles any link-layer segmentation, so no `python_fragmentation_*` toggles remain. This keeps the API simple: whatever byte buffer you hand to `send_to_node` arrives at the receiver unchanged.

For payload-only receivers, the `PayloadDelivery` metadata still exposes `message_id`, `total_len`, and `fragment_count` for backward compatibility, but those fields are always `None`. Use the `.payload`/`.frozen_payload` accessors for the tensor bytes and rely on your application-level framing if you need message identifiers.
