---
title: "Namespace Lossless Example"
description: "Runs one lossless file transfer per invocation through namespace mode and verifies matching hashes."
---

This example is a small Linux-only harness for the lossless session subsystem. It starts the controller locally, runs the dataplane in namespace mode, sends one real file from one source to one or two receivers, and verifies that every receiver wrote the same bytes as the sender.

Unlike [Namespace Flow Example](/docs/examples/networking/ns-flow), this example is dedicated to one end-to-end lossless transfer per run. It does not modify the existing `ns-flow` workload or its Docker-driven workflow.

## Linux prerequisites

- Linux host with `iproute2` and `sudo`
- `python3`
- local Postgres bootstrapped with `bash utils/start-database.sh`
- Rust toolchain unless you already have built `controller` and `nextmini` release binaries

## Run one transfer

From the repository root:

```bash
sudo ./examples/ns-lossless/run.sh --case plain-1r
```

If `--case` is omitted, `run.sh` defaults to `plain-1r`. Each invocation runs exactly one transfer and writes one sender artifact plus one artifact per receiver.

Available case names:

- `plain-1r`
- `fec-1r`
- `fec-2r-block`
- `fec-2r-symbols`

Use `--no-build` to reuse existing release binaries:

```bash
sudo ./examples/ns-lossless/run.sh --case fec-2r-symbols --no-build
```

## What varies between cases

- plain or FEC mode
- one or two receivers
- one or two trees
- `block_size`
- symbol geometry through `symbols_per_block`

## Artifacts

Per-run outputs are written under `examples/ns-lossless/artifacts/<case-name>/`:

- `payload.bin`
- `artifacts/source.bin`
- `artifacts/source.bin.sha256`
- `artifacts/receiver-<node>.bin`
- `artifacts/receiver-<node>.bin.sha256`
- `artifacts/source-1.status`
- `artifacts/receiver-<node>.status`
- `controller.log`
- `dataplane.log`
- `controller-config.toml`
- `dataplane-config.toml`

The verifier compares every receiver artifact against `artifacts/source.bin` and fails on any missing file, non-`ok` status, size mismatch, or SHA256 mismatch.

## Cleanup

The runner performs best-effort cleanup automatically, including failure paths. If you need to clean up manually:

```bash
sudo ./examples/ns-lossless/cleanup.sh --config examples/ns-lossless/artifacts/plain-1r/dataplane-config.toml
```
