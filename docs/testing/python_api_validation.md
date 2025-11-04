# Python API Validation & Benchmark Plan

Last updated: 2025-11-04 (BrownSnow)

The Python dataplane bridge landed in `python-api/`. Integration coverage is still
blocked on a multi-node harness, so this document captures the test matrix and
benchmark recipe we should run once the environment is available.

## 1. Integration Test Plan

### 1.1 Topology

| Component          | Notes                                                                 |
|--------------------|-----------------------------------------------------------------------|
| Controller + DB    | Existing docker-compose stack from `/tools/devnet/`.                  |
| Dataplane node A   | Runs Rust dataplane + Python training script (PyTorch GPT-2 example). |
| Dataplane node B   | Runs Rust dataplane + Python receiver harness.                        |
| Network            | Local bridge (docker network) or two VMs joined via WireGuard.        |

### 1.2 Test Cases

1. **Scalar telemetry:** sender publishes per-step loss (float32) and receiver
   reconstructs `np.frombuffer` / `torch.from_numpy`.
2. **Batch tensor:** sender transmits a 1 MB activation tensor (`float16`), receiver
   validates checksum.
3. **Backpressure:** artificially shrink `channel_capacity` to 1, ensure Python
   path drops gracefully and falls back to user-space path.
4. **Async recv:** use `await PacketReceiver.recv_async()` inside `asyncio.run` to
   confirm coroutine compatibility.

### 1.3 Orchestration Sketch

```
# window 1 – controller
docker compose -f tools/devnet/docker-compose.yml up

# window 2 – node A
RUST_LOG=info cargo run -p dataplane -- --config configs/node_a.toml
python examples/pytorch/gpt2.py

# window 3 – node B
RUST_LOG=info cargo run -p dataplane -- --config configs/node_b.toml
python docs/testing/scripts/recv_harness.py
```

Collect logs (`RUST_LOG=info`) and dumps from the receiver harness (writes
payloads to `/tmp/nextmini_py/` for verification).

### 1.4 Automation TODO

- Add `docs/testing/scripts/recv_harness.py` (simple asyncio consumer).
- Provide docker compose overlay that instantiates the two dataplane nodes plus
  controller.
- Capture artifacts (loss CSV, tensor checksum) and push to S3 bucket for CI
  consumption.

## 2. Benchmark Plan

### 2.1 Goals

Measure Python injection overhead vs. existing user-space/TUN path. Metrics:

| Metric                     | Target |
|---------------------------|--------|
| End-to-end latency (P50)  | < 2 ms |
| Throughput @ 1 MB payload | Within 10% of user-space baseline |

### 2.2 Workload

- Sender: synthetic loop sending tensors of sizes {64 KB, 256 KB, 1 MB} at 100 Hz.
- Receiver: measures inter-arrival times, drops, and reconstruct success.
- Run for 5 minutes per payload size.

### 2.3 Tooling

- Extend existing `examples/bench/` harness (if available) or add
  `python-api/benches/publisher.py`.
- Use `mitmdump` or `tcpdump` to capture on-wire flow for cross-checks.
- Record CPU utilisation via `pidstat` and Python gc stats (`gc.get_stats()`).

### 2.4 Reporting

- Summarise in `docs/testing/results/python_api_bench-<date>.md`.
- Include comparison table vs. TUN baseline.

## 3. Outstanding Actions

- [ ] Provision reproducible multi-node environment (docker or VM).
- [ ] Implement send/receive harness scripts (see §1.4).
- [ ] Automate results upload to shared storage.
- [ ] Schedule regression benchmark in CI once environment is available.

Until the environment is ready, treat this document as the authoritative plan and
update when individual tasks are completed or re-scoped.
