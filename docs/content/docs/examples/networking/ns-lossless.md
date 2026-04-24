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

Named case presets:

- `plain-1r`
- `fec-1r` (RaptorQ)
- `fec-2r-block` (RaptorQ)
- `fec-2r-symbols` (RaptorQ)
- `raptorq-2t-2r-k2400`
- `mettle-2t-2r-k2400`

Run one custom case without editing the script:

```bash
./examples/ns-lossless/run.sh --mode fec --trees 3 --receivers 20
```

For custom runs, `--mode` defaults to `fec` when omitted and `--fec-scheme` defaults to `raptorq`.
Use `--fec-scheme mettle` to run the METTLE backend. METTLE requires `--symbols-per-block >= 2400`.
For apples-to-apples backend comparisons, keep `--symbols-per-block`, `--block-size`, payload size, topology, and queue settings identical. Use at least two trees for both backends, and do not compare the legacy small-`K` RaptorQ presets against METTLE:

```bash
./examples/ns-lossless/run.sh --mode fec --fec-scheme raptorq --trees 2 --receivers 2 --symbols-per-block 2400 --block-size $((2400*1024)) --payload-size $((64*1024*1024))
./examples/ns-lossless/run.sh --mode fec --fec-scheme mettle --trees 2 --receivers 2 --symbols-per-block 2400 --block-size $((2400*1024)) --payload-size $((64*1024*1024))
```

`plain` mode is only valid with `--trees 1`.
Use `--packet-processors`, `--channel-capacity`, and `--queue-capacity` to exercise different namespace dataplane concurrency settings without editing the generated config.

Run the two sweep families you asked for:

```bash
./examples/ns-lossless/run.sh --tree-sweep-max 10 --receiver-sweep-max 100
```

That command runs in `fec` mode with the default `raptorq` backend and uses the script defaults of `20` receivers for the tree sweep and `3` trees for the receiver sweep. Override them with `--fec-scheme`, `--tree-sweep-receivers`, `--receiver-sweep-trees`, `--block-size`, `--symbols-per-block`, `--payload-size`, `--receive-timeout-ms`, `--packet-processors`, `--channel-capacity`, `--queue-capacity`, or `--status-timeout-seconds` if needed.

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

## Manual validation and triage

The namespace reproducer is intentionally a manual validation path because it requires `sudo` for namespace setup and cleanup. A representative plain-mode stress run is:

```bash
./examples/ns-lossless/run.sh \
  --mode plain \
  --trees 1 \
  --receivers 5 \
  --payload-size $((64*1024*1024)) \
  --block-size $((8*1024))
```

The generated artifact directory for that invocation is:

```bash
examples/ns-lossless/artifacts/custom-plain-1t-5r-b8192-s32-p1-c2048-q2048
```

For repeat runs, add `--no-build` once `target/release/controller` and `target/release/nextmini` are already current.

On success, `examples/ns-lossless/verify_hashes.py` reports matching receiver hashes and prints the receiver throughput summary. A clean pass looks like this:

- `artifacts/source-1.status` contains `ok`
- all five `artifacts/receiver-*.status` files contain `ok`
- `python3 examples/ns-lossless/verify_hashes.py examples/ns-lossless/artifacts/custom-plain-1t-5r-b8192-s32-p1-c2048-q2048/artifacts` succeeds

When a run fails or times out, inspect the per-run artifact directory without adding the namespace command to CI:

```bash
case_dir=examples/ns-lossless/artifacts/custom-plain-1t-5r-b8192-s32-p1-c2048-q2048
rg -n "^(ok|error:)" "$case_dir"/artifacts/*.status
rg -n "Lossless plain sender processed round feedback|session dropped inbound frame|timed out waiting for completion" \
  "$case_dir"/dataplane.log
rg -n "Integration test (source|receiver) finished successfully|timed out" \
  "$case_dir"/controller.log
perl -ne 'print if /Lossless sender|Lossless receiver|PlainStatus|timed out/' \
  "$case_dir"/dataplane.log
```

The `*.status` files tell you whether the source or any receiver timed out. The `dataplane.log` queries above isolate the plain-mode round-feedback path, late-frame replay warnings, and session timeout lines that are most useful when verifying convergence regressions.

## What varies between cases

- plain or FEC mode
- FEC backend, `raptorq` or `mettle`
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
