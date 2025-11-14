# Python API Validation & Benchmark Plan

Last updated: 2025-11-05 (FuchsiaPond – documentation pass, quickstart link added)

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

- [x] Add `docs/testing/scripts/recv_harness.py` (simple asyncio consumer) and
  `docs/testing/scripts/send_harness.py` for deterministic payload injection. *(2025-11-04 – LilacLake)*
- [x] Provide docker compose overlay (`docs/testing/docker-compose.python-api.yml`)
  plus per-node configs under `docs/testing/configs/` to launch controller +
  two harness shells in one command. *(2025-11-04 – LilacLake)*
- [ ] Capture artifacts (loss CSV, tensor checksum) and push to S3 bucket for CI
  consumption.
- [ ] Mirror artifact summaries under `docs/testing/artifacts/latest/` during manual
  dry runs so reviewers can inspect payloads without S3 access.

### 1.5 Harness Quickstart

1. Launch the full stack (Postgres, controller, receiver + sender harness containers):

   ```bash
   docker compose -f docs/testing/docker-compose.python-api.yml up --build receiver sender
   ```

   The receiver waits for the controller (`WAIT_FOR=controller:3000`) before starting the async
   listener. Default parameters send 20 payloads of 32 KiB with 50 ms spacing.

2. Inspect results under `docs/testing/artifacts/`:

   - `receiver-summary.json` contains packet counts, byte totals, and timing.
   - `receiver_payloads/` holds raw payload dumps when `--output-dir` is enabled.

   The helper script `docs/testing/scripts/run_harness.sh` accepts the same arguments the
   docker-compose file passes. You can invoke it directly on a developer workstation once
   the controller and dataplanes are running:

   ```bash
   bash docs/testing/scripts/run_harness.sh sender docs/testing/configs/node_sender.toml \
     --src-node-id 1 --dst-node-id 2 --count 20 --size 4096
   ```

   Receiver side:

   ```bash
   bash docs/testing/scripts/run_harness.sh receiver docs/testing/configs/node_receiver.toml \
     --src-node-id 1 --expected 20 --summary-json /tmp/receiver-summary.json \
     --output-dir /tmp/receiver_payloads
   ```

3. Customise runs via environment overrides, for example:

   ```bash
   RECEIVER_EXPECTED=100 SENDER_COUNT=100 SENDER_SIZE=1048576 \
     docker compose -f docs/testing/docker-compose.python-api.yml up receiver sender
   ```

   Additional knobs: `SENDER_SLEEP_MS`, `RECEIVER_TIMEOUT_MS`, `SENDER_BATCH`, etc. Both harness
   services exit once their work completes; rerun `docker compose up` to perform another scenario.

4. Snapshot artifacts for review: copy `docs/testing/artifacts/receiver-summary.json`
   and the payload directory into `docs/testing/artifacts/run-<date>/` before publishing
   results upstream. This mirrors the outputs that will eventually land in S3.

### 1.6 Docs & references

- For developer-facing setup instructions, point reviewers to [Examples → PyTorch Python API](../examples/pytorch_python_api.md), which mirrors the wheel build and environment variables the harness expects (`NEXTMINI_CONFIG`, `NEXTMINI_DST_NODE`).
- Update the doc above whenever harness flags or defaults change so the quickstart stays accurate.

## 4. Automation roadmap

- [ ] Persist `receiver-summary.json` and payload dumps as CI artifacts (target: Github Actions runner).
- [ ] Ship a helper script that converts summary JSON into GitHub Step Summary / Markdown.
- [ ] Push artifacts to the shared S3 bucket (`s3://nextmini-python-api-ci/`) after every scheduled run.
- [ ] Gate the workflow on successful validation of scalar + batch + backpressure scenarios.

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

- [x] Provision reproducible multi-node environment (docker or VM). *(2025-11-04 – LilacLake: docker-compose overlay)*
- [x] Implement send/receive harness scripts (see §1.4). *(2025-11-04 – LilacLake)*
- [ ] Automate results upload to shared storage.
  - Use `docs/testing/scripts/upload_artifacts.sh` (requires `AWS_S3_BUCKET`, `ARTIFACT_PATH`, optional `CI_RUN_ID`).
- [ ] Schedule regression benchmark in CI once environment is available.

Until the environment is ready, treat this document as the authoritative plan and
update when individual tasks are completed or re-scoped.
