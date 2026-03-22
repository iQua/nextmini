# FEC WAN Example

This example runs plain and FEC multicast experiments across real machines over SSH.

The workflow is intentionally small:
- edit one inventory file
- build and publish two images
- run one shell script

Cases run sequentially in the order they appear in the inventory. After each case, the runner removes remote containers and the remote run directory, but it keeps images so later cases can reuse them.

## Files

- `inventory.example.toml`
  Fill in your controller host, node hosts, image refs, SSH credentials, and ordered case list.
- `run_experiments.sh`
  Main entrypoint. Runs cases in inventory order, stages files, starts containers, collects logs, and cleans up.
- `plan_case.py`
  Parses TOML, renders configs, and generates the payload for one case.
- `templates/controller-config.toml`
  Controller template.
- `templates/node.toml`
  Node template.
- `Dockerfile`
  FEC node image used by the trainer, receivers, and relays.

## Prerequisites

- local machine:
  - `bash`
  - `python3`
  - `ssh` and `scp`
- remote machines:
  - Docker installed and usable by the SSH user
  - reachable over SSH from the machine running the script
- images:
  - one controller image
  - one FEC node image

## Build Images

Build the controller image from the repo root:

```bash
docker build -t registry.example.com:5000/nextmini-controller:latest -f controller/Dockerfile .
docker push registry.example.com:5000/nextmini-controller:latest
```

Build the FEC node image from the repo root:

```bash
docker build -t registry.example.com:5000/nextmini-fec:latest -f examples/fec/Dockerfile .
docker push registry.example.com:5000/nextmini-fec:latest
```

Use image refs in the inventory that are reachable from:
- the controller host for the controller image
- every trainer / receiver / relay host for the node image

## Fill In The Inventory

Start from [inventory.example.toml](/home/xindan/nextmini/examples/fec/inventory.example.toml).

You need to set:
- `[controller]`: controller SSH host/user/key
- `[images]`: controller and node image refs
- `[paths]`: optional remote run root
- `[defaults]`: shared SSH key, interface, and payload size
- `[[nodes]]`: one trainer, receivers, and relays
- `[[cases]]`: the ordered experiment sequence

Each case needs:
- `name`
- `mode`: `plain` or `fec`
- `receiver_ids`
- `relay_ids`
- `tree_ids`
- `block_size`
- `symbols_per_block`

## Run

Run from the repo root:

```bash
bash examples/fec/run_experiments.sh --inventory examples/fec/inventory.example.toml
```

The runner will:
1. plan the next case with `plan_case.py`
2. copy configs and payload to the remote hosts
3. start Postgres and the controller on the controller host
4. start source / receiver / relay containers on the active nodes
5. wait for the source/receiver handshake and receiver outputs
6. fetch logs locally
7. clean up remote containers and the remote run directory
8. continue to the next case in inventory order

## Output

By default, local outputs go under `/tmp/nextmini-fec/<run-id>/`.

Each run directory contains:
- `payload.bin`
- `controller-config.toml`
- `node-*.toml`
- `group-info.json`
- `receiver-ready-*.json`
- `tensor-metadata.json`
- `logs/controller.log`
- `logs/node-*.log`

The sender and receiver throughput lines are printed in the node logs.
