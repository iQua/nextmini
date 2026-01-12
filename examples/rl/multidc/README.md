# Multi-DC WAN Deployment (Automation)

This directory contains a small **SSH-orchestrated** runner for multi-datacenter experiments with the
Nextmini multicast dataplane and the WAN broadcast microbenchmark (`examples/rl/src/broadcast_bench.py`).

It aims to make the evaluation setup reproducible with a single command.

## Why not Docker Swarm?

For WAN measurements, Swarm’s overlay networking can add extra tunneling/encapsulation and make results
harder to interpret. The scripts here use **plain SSH + host-network containers** so the dataplane sees
real NICs/IPs and traffic follows the raw WAN path.

Swarm can still be used as a *deployment* tool, but avoid running the dataplane over Swarm overlay networks.
If you do use Swarm, prefer host-mode networking for services and treat Swarm as an orchestration layer only.

## Containers vs. Rust binaries

There are two reasonable deployment styles for WAN experiments:

- **Host-network containers (default here)**: simplest to reproduce; isolates Python deps for RL; overhead is
  negligible compared to WAN transfer times when using `--network host`.
- **Copy a prebuilt Rust agent binary**: fastest to roll out to many VMs and avoids Docker on relays, but you
  must ensure a compatible target (Linux distro/glibc) and keep configs/flags in sync across machines.

In practice, a good split is:
- **Relays**: Rust agent-only (binary or tiny container) for simplicity and minimal dependencies.
- **Trainer/workers**: containers (need Python + ML stack) with host networking.

## Prerequisites

- Controller VM: Docker installed, inbound TCP `3000` open.
- Each node VM (trainer/worker/relay): Docker installed, inbound TCP `8080` and `8081` open.
- SSH key access to all machines (recommended: `ssh-agent` for passphrase keys):
  - `ssh-add ~/.ssh/id_rsa`
- Local machine needs: `python3` (3.11+), `ssh`, `rsync`.
  - Optional: `pssh`/`pscp` if you prefer those tools; the provided runner already parallelizes SSH work internally.

### Quick bootstrap (fresh Ubuntu VMs)

On each VM (or via SSH), run:

```bash
sudo bash examples/rl/scripts/bootstrap_ubuntu.sh
```

This installs `docker`, `rsync`, and `iperf3`, and prints a reminder about required ports.

## Inventory

Copy and edit:

- `examples/rl/multidc/inventory.example.toml`
- Save as `examples/rl/multidc/inventory.toml`

Notes:
- The runner assumes **one Nextmini node per VM** (host-network binds fixed ports).
- Put the VM’s publicly reachable IP in `public_ip` (NAT is fine).
- Relays are optional but recommended for non-trivial trees.

## Run the WAN broadcast benchmark (one line)

```bash
python examples/rl/scripts/multidc.py run-bench \
  --inventory examples/rl/multidc/inventory.toml \
  --bytes $((10 * 1024 * 1024 * 1024)) \
  --rounds 20 \
  --algorithm cf_bottleneck
```

### Probing sanity checks (recommended on WAN)

When using `--probe-links`, you can also collect an ICMP ping matrix (RTT/loss) between nodes and
enable an automatic **outlier re-probe** pass (low-capacity probe results are re-measured sequentially).

```bash
python examples/rl/scripts/multidc.py run-bench \
  --inventory examples/rl/multidc/inventory.toml \
  --bytes $((1 * 1024 * 1024 * 1024)) \
  --rounds 5 \
  --algorithm cf_bottleneck \
  --probe-links \
  --collect-ping-matrix
```

Notes:
- Outlier re-probing is enabled by default; disable with `--no-probe-retest-outliers`.
- The ping matrix + re-probe details are written into `examples/rl/multidc/results/results.meta.json`.

Outputs:
- Syncs the repo to each VM (`remote_repo_dir`).
- Starts controller + postgres on the controller VM.
- Starts relay/worker benchmark containers (detached).
- Runs the trainer benchmark (foreground) and copies results back to:
  - `examples/rl/multidc/results/results.json`
- By default, cleans up containers afterwards; pass `--no-cleanup` to keep them running.

If you are iterating and the remote repo is already updated, you can skip syncing with `--no-sync`.

## Tear down

```bash
python examples/rl/scripts/multidc.py down --inventory examples/rl/multidc/inventory.toml
```
