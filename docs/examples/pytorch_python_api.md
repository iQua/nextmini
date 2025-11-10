# PyTorch + Nextmini Python API Quickstart

This walk-through shows how to inject training metrics/tensors directly into the Nextmini dataplane from a Python workload (e.g. HuggingFace fine-tuning) without relying on TUN delivery. It complements `examples/pytorch/gpt2.py`, which already includes the optional hooks described below.

## 1. Build and install the Python extension

The Python API lives in the `python-api/` crate and is built with [maturin].

```bash
pip install maturin             # one-time
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

> The crate exposes the `nextmini_py` module providing `Dataplane` and `PacketReceiver`.

## 2. Launch a Nextmini dataplane in-process

In your Python script:

```python
import nextmini_py as nm

dp = nm.Dataplane("/path/to/node-config.toml")
receiver = dp.register_receiver_from_node(src_node_id=1, payload_only=True)
```

* `Dataplane` embeds a Tokio runtime, spins up the Rust dataplane, and automatically wires the Python delivery interface.
* `register_receiver_from_node` arms a bounded queue keyed by flow ID. You can call `receiver.recv(timeout_ms=5000)` or `await receiver.recv_async()` in asyncio code.
* Pass `payload_only=True` (recommended) to receive `nextmini_py.PayloadDelivery` objects that expose `.payload`, `.message_id`, `.total_len`, and `.payload_format`. Omit the flag to keep the legacy raw-packet behavior for tooling that still expects IPv4/TCP frames.

## 3. Send payloads straight from Python buffers

```python
loss_val = outputs.loss.detach().float().cpu().numpy()
dp.send_to_node(dst_node_id=2, payload=memoryview(loss_val))
```

`send_to_node` accepts any object implementing the buffer protocol: `bytes`, `memoryview`, `numpy.ndarray`, or a contiguous `torch.Tensor` (`tensor.contiguous().cpu().numpy()`).

Batch sends are supported via `send_batch_to_node(dst_node_id, iterable)` to amortise crossings.

## 4. Enable the example hooks

`examples/pytorch/gpt2.py` already contains the glue—set the following environment variables before running the example:

```bash
export NEXTMINI_CONFIG=/absolute/path/node-config.toml
export NEXTMINI_DST_NODE=2         # numeric node id to target
python examples/pytorch/gpt2.py --num-epochs 1
```

When both env vars are present the script will:

1. Import and instantiate `nextmini_py.Dataplane`.
2. Stream the training loss each step with `send_to_node`.
3. Log failures but proceed with training so developers can triage without blocking jobs.

## 5. Receiving on the destination

On the peer node (or the same machine if you run a second process):

```python
import nextmini_py as nm
rx = nm.Dataplane("/path/to/node-config.toml").register_receiver_from_node(
    src_node_id=1,
    payload_only=True,
)
delivery = rx.recv(timeout_ms=2000)
if delivery:
    buf = delivery.payload
    arr = np.frombuffer(buf, dtype=np.float32)
    print(f"message_id={delivery.message_id} total_len={delivery.total_len}")
```

Use the symmetrical helpers to turn buffers into tensors (`torch.from_numpy(arr)`), then feed them into your analytics or distributed optimisation pipeline.

## Operational notes

* **Routing** – The helper functions synthesise IPv4/TCP headers that match the flow tuples the dataplane already understands. Group routing, policy, and scheduling continue to work unchanged.
* **Limits** – Delivery queues inherit `channel_capacity` from the node config. If your Python consumer can’t keep up, backpressure is signalled via log warnings (`PythonInterface: queue unavailable…`) and packets fall back to user-space transport.
* **Testing** – Unit tests cover the packet utilities and Python interface queue semantics (`cargo test -p nextmini packet::tests`). For integration testing, bring up two nodes locally, set the env vars above, and verify payloads are observed on the receiver.
* **Packaging** – The crate targets `abi3-py39`. Wheels built once can be installed on any compatible interpreter without a Rust toolchain.

## Enabling fragmentation for large tensors

By default the python API still emits a single IPv4/TCP frame per `FrozenBuffer`. To let the dataplane split oversized payloads automatically, flip the new `python_fragmentation_*` knobs in your node config (or CLI):

```toml
# Keep at or below mtu - 64 (1_336 bytes at the default MTU 1400).
python_fragmentation_enabled = true
python_fragmentation_max_message_bytes = 65536

# Receiver-side safety rails.
python_fragmentation_reassembly_window_bytes = 262144
python_fragmentation_fragment_timeout_ms = 1000
python_fragmentation_trace_flow_events = true
```

Restart the dataplane after editing the config. Once enabled, every call to `send_to_node`/`send_batch_to_node` slices the payload into MTU-safe chunks, stamps the PyPayloadSeg header, and automatically reassembles the payload on the destination Python receiver. When `payload_only=True`, the returned `PayloadDelivery` object ties the reconstructed bytes to the originating `message_id`, `total_len`, and `payload_format` so you can trace drops or metadata.

With `python_fragmentation_trace_flow_events = true`, the dataplane also emits `DataplaneToController::PythonFragmentEvents`
records whenever fragments are rejected (`kind = "assembler_drop"`/`"invalid_header"`), the reassembly window is exceeded
(`"window_overflow"`), or fragment groups time out. These show up in controller logs and can be scraped for alerting.
Every 5 seconds the dataplane also pushes a `DataplaneToController::PythonFragmentMetrics` snapshot so operators can
track the aggregate counters even if event tracing is disabled.

[maturin]: https://github.com/PyO3/maturin
