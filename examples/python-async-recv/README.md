# Python Async Receiver Example

This example demonstrates how to integrate the in-process dataplane exposed by
`nextmini_py` with an `asyncio` event loop. The `async_receiver.py` script awaits
`PacketReceiver.recv_async()` while continuing to run other coroutines, proving
the fourth validation scenario from `docs/testing/python_api_validation.md`.

## Topology

Two dataplane nodes connect to the local controller:

| Component | Config file                          | Notes                                 |
|-----------|--------------------------------------|---------------------------------------|
| Controller| `controller-config.toml`             | Full-mesh with two nodes              |
| Sender    | `node-sender.toml` (node_id = 1)     | Publishes JSON telemetry via Python   |
| Receiver  | `node-receiver.toml` (node_id = 2)   | Awaits `recv_async()` in `asyncio`    |

> `private_network_interface` defaults to `lo`. Adjust it if your workstation
> uses another interface name (e.g., `lo0` on macOS).

## Prerequisites

1. Install and build the Python extension:
   ```bash
   pip install maturin
   maturin build --release -m python-api/Cargo.toml
   pip install target/wheels/nextmini_py-*.whl
   ```
2. Start PostgreSQL (matches the credentials in `controller-config.toml`):
   ```bash
   ./start-database.sh    # populates postgres://pgusr:pgpwrd@localhost:5432/nextmini
   ```
3. Launch the controller in another terminal:
   ```bash
   cargo run -p controller -- --config examples/python-async-recv/controller-config.toml
   ```

## Running the example

Open two shells and activate the same virtual environment where `nextmini_py`
was installed.

**Receiver (async coroutine path)**
```bash
python examples/python-async-recv/async_receiver.py \
  --config examples/python-async-recv/node-receiver.toml \
  --src-node-id 1 \
  --expected 20
```
The script spins a background heartbeat task so you can see the event loop stay
responsive even when no packets arrive. Each payload is decoded from JSON and
reported with end-to-end latency measurements.

**Sender**
```bash
python examples/python-async-recv/sender.py \
  --config examples/python-async-recv/node-sender.toml \
  --dst-node-id 2 \
  --count 20
```
This publishes synthetic loss metrics (`loss = 1.5 * e^(-0.05 step)`) every 250 ms.

Both scripts exit after processing the requested number of payloads, but the
embedded dataplane keeps running inside each process until Python terminates.
Use `Ctrl+C` to stop them early.

## Customising

- `--sleep-ms` (sender) and `--timeout-ms` (receiver) let you explore different
  pacing/timeout combinations.
- `--src-port` / `--dst-port` options on both scripts demonstrate how to target
  different flow tuples.
- Increase `--count`/`--expected` to soak-test `recv_async()` alongside other
  coroutines or to gather artifact traces for CI.
