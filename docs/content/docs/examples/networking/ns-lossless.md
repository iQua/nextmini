---
title: "Namespace Lossless Example"
description: "Runs one lossless file transfer per invocation through namespace mode and verifies matching hashes."
---

This example is a small Linux-only harness for the lossless session subsystem. It starts the controller locally, runs the dataplane in namespace mode, sends a real file from one source to a generated receiver set, and verifies that every receiver wrote the same bytes as the sender.

Unlike [Namespace Flow Example](/docs/examples/networking/ns-flow), this example is dedicated to one end-to-end lossless transfer per run. It does not modify the existing `ns-flow` workload or its Docker-driven workflow.

## Linux prerequisites

- Linux host with `iproute2`, `iptables`, and `sudo`
- `python3`
- local Postgres bootstrapped with `bash utils/start-database.sh`
- Rust toolchain unless you already have built `controller` and `nextmini` release binaries

If you reuse an existing `nextmini-database` container, it must publish host port `5432`; the controller connects from the host to `127.0.0.1:5432`.

## Run one transfer

From the repository root:

```bash
./examples/ns-lossless/run.sh --case plain-1r
```

If `--case` is omitted, `run.sh` defaults to `plain-1r`. A named case or one-off parameterized run executes one transfer. Sweep options execute many transfers sequentially and write one artifact set per generated case directory.

At the end of each successful run, the verifier prints a receiver throughput summary in Gbit/s with receiver min/avg/max goodput across the receiver set.

Receiver throughput starts when each receiver completes its first logical block and stops when that receiver completes the full object. This excludes manifest/READY setup time and makes the printed `avg_gbps` line the cleanest receiver-side throughput summary for multi-receiver runs.

The script escalates to `sudo` itself for the namespace setup. Running it without `sudo` avoids root `PATH` issues with user-local Rust installs in `~/.cargo/bin`.
The generated dataplane config also enables host bridge `FORWARD` rules automatically so namespace
nodes can reach each other on hosts where `bridge-nf-call-iptables=1`.

`run.sh` expects `127.0.0.1:3000` to be free for the local controller. If another controller or container is already listening on that port, stop it before starting a case.

Legacy named case presets:

- `plain-1r`
- `fec-1r`
- `fec-2r-block`
- `fec-2r-symbols`

Run one custom case without editing the script:

```bash
./examples/ns-lossless/run.sh --mode fec --trees 3 --receivers 20
```

For custom runs, `--mode` defaults to `fec` when omitted. `plain` mode is only valid with `--trees 1`.
Use `--packet-processors`, `--channel-capacity`, and `--queue-capacity` to exercise different namespace dataplane concurrency settings without editing the generated config.

Run the two sweep families you asked for:

```bash
./examples/ns-lossless/run.sh --tree-sweep-max 10 --receiver-sweep-max 100
```

That command runs in `fec` mode and uses the script defaults of `20` receivers for the tree sweep and `3` trees for the receiver sweep. Override them with `--tree-sweep-receivers`, `--receiver-sweep-trees`, `--block-size`, `--symbols-per-block`, `--payload-size`, `--receive-timeout-ms`, `--packet-processors`, `--channel-capacity`, `--queue-capacity`, or `--status-timeout-seconds` if needed.

Use `--receive-timeout-ms` to override the generated integration session completion timeout (default: `120000`). This is separate from `--status-timeout-seconds`, which controls how long the shell runner waits for case status files.

Run only one sweep family:

```bash
./examples/ns-lossless/run.sh --tree-sweep-max 10
./examples/ns-lossless/run.sh --receiver-sweep-max 100
```

The generated topology uses one source node, two relay nodes per tree, and then a shared receiver fanout. Total namespace nodes per run are `1 + (2 * trees) + receivers`.

Use `--no-build` to reuse existing release binaries:

```bash
./examples/ns-lossless/run.sh --case fec-2r-symbols --no-build
```

## What varies between cases

- plain or FEC mode
- receiver count
- tree count
- `block_size`
- symbol geometry through `symbols_per_block`

## Artifacts

Per-run outputs are written under `examples/ns-lossless/artifacts/<case-name>/`. Sweep runs create one directory per generated case name:

- `payload.bin`
- `artifacts/source.bin`
- `artifacts/source.bin.sha256`
- `artifacts/receiver-<node>.bin`
- `artifacts/receiver-<node>.bin.sha256`
- `artifacts/source-1.status`
- `artifacts/receiver-<node>.status`
- `artifacts/source-1.metrics`
- `artifacts/receiver-<node>.metrics`
- `controller.log`
- `dataplane.log`
- `controller-config.toml`
- `dataplane-config.toml`

The verifier compares every receiver artifact against `artifacts/source.bin` and fails on any missing file, non-`ok` status, size mismatch, SHA256 mismatch, or malformed/missing throughput metrics file. Each `*.metrics` file stores the transfer payload size, elapsed seconds, and derived throughput in Gbit/s for that node. Receiver-side durations begin at the first completed logical block and end when the receiver finishes the object.

## Cleanup

The runner performs best-effort cleanup automatically, including failure paths. If you need to clean up manually:

```bash
sudo ./examples/ns-lossless/cleanup.sh --config examples/ns-lossless/artifacts/plain-1r/dataplane-config.toml
```
