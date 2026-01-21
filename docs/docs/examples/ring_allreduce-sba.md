# Ring all-reduce on SBA (Docker Swarm)

This uses the `examples/sba-swarm` stack and the `ring-emu` launcher to start the `ringallreduce` binary across the Swarm nodes.

## Prerequisites

Deploy the SBA Swarm stack first (controller + dataplane) by following: [PyTorch on SBA (Docker Swarm)](pytorch-sba.md).

## Run ring all-reduce inside `node1`

Exec into `node1`:

```bash
docker ps
docker exec -it <node1_container_id> /bin/bash
```

Then run the launcher:

```bash
cd /var/nextmini/ring-emu
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini \
  --len 1048576 \
  --init rank \
  --reps 10 \
  --verify
```

For flags and troubleshooting tips, see: [Ring all-reduce launcher (ring-emu)](ring-emu.md).

## Optional: monitoring dashboard

From the controller VM:

```bash
cd nextmini/tools/monitor
uv run dashboard.py
```
