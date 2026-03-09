---
title: "Namespace Flow Example"
description: "Generate namespace configs and run many identical flows on one Linux host."
---

This example runs a single-host namespace workload where the controller and Postgres run in Docker, and one local dataplane process spawns many namespace nodes and flow senders.

## Linux prerequisites

- Linux host with `iproute2` (`ip` command), `sudo`, and `tmux`.
- Docker Engine with `docker compose` (or `docker-compose` fallback).
- `python3` (used by `examples/ns-flow/generate.py`).
- Rust toolchain (`cargo`) unless you already have a built `nextmini` binary.

Namespace-specific behavior from this example:

- `examples/ns-flow/run.sh` applies Linux `sysctl` tuning for large namespace/veth churn.
- `examples/ns-flow/generate.py` writes `examples/ns-flow/controller-config.toml` and `examples/ns-flow/config.toml`.
- Generated dataplane config sets `enable_local_interface = false` for namespace flow runs.

## 1. Generate config files

From the repository root, generate the default 136-node / 180-flow configuration:

```bash
python3 examples/ns-flow/generate.py --n-nodes 136 --n-flows 180 --src 1 --dst 136
```

To customize the workload, pass the same arguments that `run.sh` accepts:

```bash
python3 examples/ns-flow/generate.py \
  --n-nodes 200 \
  --n-flows 400 \
  --src 1 \
  --dst 200 \
  --flow-bytes 1000000 \
  --flow-rate 500000 \
  --flow-weight 1
```

`run.sh` regenerates these files automatically unless you pass `--no-generate`.

## 2. Start the example

Start everything with the orchestration script:

```bash
sudo ./examples/ns-flow/run.sh
```

`run.sh` orchestration details:

1. Applies sysctl tuning (unless `--no-sysctl`).
2. Regenerates config files (unless `--no-generate`).
3. Starts a tmux session (`nextmini-ns-flow`) with:
   - left pane: `docker compose up --build` in `examples/ns-flow`
   - right pane: build dataplane (if needed), wait for Postgres/controller readiness, then launch `nextmini` with `examples/ns-flow/config.toml`

If you only want tuning without startup:

```bash
sudo ./examples/ns-flow/run.sh --sysctl-only
```

## 3. Verify startup and flow completion

Check generated counts:

```bash
grep -c '^\[\[routes\]\]' examples/ns-flow/controller-config.toml
grep -c '^\[\[flows\]\]' examples/ns-flow/controller-config.toml
```

Check seeded and finished flows in Postgres:

```bash
docker exec postgres psql -U pgusr -d nextmini -c \
  "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_finished) AS finished FROM flows;"
```

Optional route sanity check:

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS routes FROM routes;"
```

## 4. Cleanup

Stop tmux/docker and remove created namespace networking artifacts:

```bash
sudo ./examples/ns-flow/cleanup.sh
```

The cleanup script kills the `nextmini-ns-flow` tmux session, brings down `examples/ns-flow/docker-compose.yml`, deletes `veth*` links for configured nodes, and removes `isobr*` bridge shards.

## Lossless integration harness

The same folder now also contains a smaller namespace-backed lossless session harness that runs host-local processes instead of `docker compose`.

Use this path when you want a real end-to-end file transfer test for the lossless subsystem with:

- plain mode and FEC mode
- one or two receivers
- one or two trees
- varying `block_size`
- varying symbol geometry via `symbols_per_block`

### Prerequisites

- Linux host with `iproute2` and `sudo`
- `python3`
- Postgres bootstrapped with `bash utils/start-database.sh`
- Rust toolchain unless you already have built `controller` and `nextmini` release binaries

### Run the default matrix

```bash
sudo ./examples/ns-flow/run-integration.sh
```

The runner builds `controller` and `nextmini --features python-extension` by default, starts Postgres through `utils/start-database.sh` if needed, and then runs the fixed case matrix.

This runs four cases:

1. plain, one receiver, one tree
2. FEC, one receiver, one tree
3. FEC, two receivers, two trees, smaller block size
4. FEC, two receivers, two trees, different `symbols_per_block`

### Run one case

```bash
sudo ./examples/ns-flow/run-integration.sh --case fec-2r-symbols
```

Available case names:

- `plain-1r`
- `fec-1r`
- `fec-2r-block`
- `fec-2r-symbols`

Pass `--no-build` to reuse existing release binaries:

```bash
sudo ./examples/ns-flow/run-integration.sh --case plain-1r --no-build
```

### Artifacts

Per-case outputs are written under `examples/ns-flow/integration-artifacts/<case-name>/`:

- `payload.bin`
- `artifacts/source.bin`
- `artifacts/source.bin.sha256`
- `artifacts/receiver-<node>.bin`
- `artifacts/receiver-<node>.bin.sha256`
- `artifacts/source-1.status`
- `artifacts/receiver-<node>.status`
- `dataplane.log`
- `controller.log`
- `controller-config.toml`
- `dataplane-config.toml`

The verifier compares every receiver artifact against `artifacts/source.bin` and fails on any missing file, non-`ok` status, size mismatch, or SHA256 mismatch.
