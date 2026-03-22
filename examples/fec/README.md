# FEC WAN Benchmark — DigitalOcean Deployment

Fully automated end-to-end deployment of FEC experiments on DigitalOcean.
A single bash script provisions VMs, builds Docker images, runs the experiment, and collects results — no Ansible required.

## Architecture

5 VMs across 2 regions to simulate WAN latency. Each node runs on its own dedicated VM.

| VM  | Region | Roles      | Node ID |
|-----|--------|------------|---------|
| VM1 | nyc3   | Controller + Source | 1 |
| VM2 | nyc3   | Relay A    | 5       |
| VM3 | nyc3   | Receiver A | 2       |
| VM4 | sfo3   | Relay B    | 6       |
| VM5 | sfo3   | Receiver B | 3       |

**Topology:** `Source(1) → Relay(5) → Receiver(2)` and `Source(1) → Relay(6) → Receiver(3)`

## Prerequisites

- **DigitalOcean API token** with droplet create/destroy permissions
- **SSH key** registered with DigitalOcean (default key ID: `55047570`)
- Local tools: `ssh`, `scp`, `curl`, `python3`

## Quick Start

```bash
# 1. Set your API token
export DO_API_TOKEN=your_token_here

# 2. Run (provisions VMs, builds images, runs experiment, collects results)
bash examples/fec/do_run.sh --keep-vms

# 3. Re-run on the same VMs (auto-detects existing images, skips build)
bash examples/fec/do_run.sh --skip-provision --keep-vms

# 4. Destroy VMs when done
bash examples/fec/do_run.sh --destroy
```

## Options

| Flag | Description |
|------|-------------|
| `--keep-vms` | Preserve VMs after run (default: destroy on exit) |
| `--skip-provision` | Reuse existing VMs (requires prior successful provision) |
| `--skip-build` | Skip Docker image build entirely |
| `--mode=MODE` | `plain` (default) or `fec` |
| `--payload-size=SZ` | Payload size, e.g. `100MiB` (default), `1GiB` |
| `--destroy` | Destroy all VMs and exit |

## Script Phases

1. **Provision** — Creates 5 DigitalOcean droplets via API, waits for boot, installs Docker
2. **Build** — Uploads source to VM1, builds Docker images remotely, distributes compressed images to all VMs via direct VM-to-VM SCP
3. **Payload** — Generates random payload locally
4. **Run** — Generates `node.toml` configs via SSH, starts controller (Postgres + controller container) and node containers, coordinates file exchange (`group-info.json`, `receiver-ready-*.json`) between nodes
5. **Collect** — Downloads container logs and artifacts from all VMs
6. **Cleanup** — Destroys droplets via API (unless `--keep-vms`)

### Smart Caching

The script automatically detects previously built images:
- If VM1 already has the Docker image → **build is skipped**
- If a target VM already has the image → **distribution to that VM is skipped**
- Cross-region SCP failures automatically fall back to local relay

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DO_API_TOKEN` | *required* | DigitalOcean API token (or set in `~/.env`) |
| `DO_SSH_KEY_ID` | `55047570` | DO SSH key fingerprint/ID |
| `DO_SSH_KEY_FILE` | `~/.ssh/do_wan_benchmark` | Local path to SSH private key |

## File Structure

```
examples/fec/
├── do_run.sh                    # Main deployment script (pure bash)
├── Dockerfile                   # FEC node image (pre-existing)
├── Dockerfile.controller.local  # Controller image
├── .gitignore                   # Ignores runtime files
├── .do-state.json               # [generated] VM state
├── .do-image-tag                # [generated] Last built image tag
├── payload.bin                  # [generated] Test payload
└── results/                     # [generated] Experiment results
```

## Results

After each run, results are saved to `results/do-<timestamp>/`:

```
results/do-20260322-103422/
├── controller.log          # Controller container logs
├── fec-src/
│   ├── node.log            # Source container logs
│   ├── node.toml           # Source config
│   └── tensor-metadata.json
├── fec-relay-a/
│   └── node.toml
├── fec-recv-a/
│   ├── node.log
│   └── receiver-ready-2.json
└── ...
```

