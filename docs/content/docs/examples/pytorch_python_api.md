---
title: "PyTorch + Nextmini Python API quickstart"
description: ""
---


This guide shows how to stream PyTorch tensors through `nextmini_py` using the current Python API (`PacketView`, `Dataplane`, and `PacketReceiver`).

## 1. Build and install the extension

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
# or:
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

The wheel targets CPython 3.13 (`abi3-py313`).

## 2. Bring up the dataplane from Python

```python
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
```

Keep this object alive for as long as you need to send/receive traffic.

## 3. Send tensors with `PacketView`

```python
import nextmini_py as nm

def packet_view_from_tensor(tensor):
    arr = tensor.detach().contiguous().cpu().numpy()
    return nm.PacketView(arr.tobytes())

payload = packet_view_from_tensor(loss_tensor)
dp = nm.Dataplane("/abs/path/node-config.toml")
dp.send_to_node(dst_node_id=2, frozen=payload)
```

## 4. Receive payloads with metadata

```python
import numpy as np
import nextmini_py as nm

dp = nm.Dataplane("/abs/path/node-config.toml")
rx = dp.register_receiver_from_node(src_node_id=1)

delivery = rx.recv(timeout_ms=2_000)
if delivery:
    arr = np.frombuffer(delivery.payload, dtype=np.float32)
    print(f"flow={delivery.flow_id} src={delivery.src_ip}:{delivery.src_port}")
else:
    print("receiver timed out")
```

For multicast receivers, use:

```python
rx = dp.register_receiver_for_group(src_node_id=1, group_ip="239.1.1.10")
```

## 5. Optional env-gated hook for training scripts

The current `examples/pytorch/*.py` scripts do not automatically publish telemetry via `nextmini_py`, so add a small opt-in hook if needed:

```python
import os
import nextmini_py as nm

NEXTMINI_CONFIG = os.getenv("NEXTMINI_CONFIG")
NEXTMINI_DST_NODE = os.getenv("NEXTMINI_DST_NODE")
DP = nm.Dataplane(NEXTMINI_CONFIG) if NEXTMINI_CONFIG and NEXTMINI_DST_NODE else None

def maybe_publish_tensor(tensor):
    if DP is None:
        return
    payload = nm.PacketView(tensor.detach().contiguous().cpu().numpy().tobytes())
    DP.send_to_node(dst_node_id=int(NEXTMINI_DST_NODE), frozen=payload)
```

## 6. Payload delivery semantics

`nextmini_py` delivers payload bytes directly to Python receivers. There are no `python_fragmentation_*` configuration fields; link-layer segmentation is handled by the networking stack, while lossless session chunk sizing is controlled through runtime options (for `send_data` / `receive_data` flows).
