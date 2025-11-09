# Multicast Multi-Group (TOML) Demo

This example shows how to drive several multicast groups from Python, exercise
`leave_group`, and coordinate all runtime parameters via TOML files (no JSON or
ad-hoc markers). It uses two roles:

- **Source** – creates multiple groups, broadcasts an initial burst to every
  group, waits for the receiver to leave one group, then sends another burst to
  prove traffic no longer arrives on the departed group.
- **Receiver** – joins every group defined in the TOML config, confirms local
  membership/routes, consumes the first burst, calls `leave_group` on a selected
  label, and verifies the dataplane stops forwarding packets for that group.

## Prerequisites

1. Build and install `nextmini_py` (see `docs/examples/pytorch_python_api.md`).
2. Launch a controller plus two dataplane nodes (sender + receiver). The
   `docs/testing/configs/` TOML files work out-of-the-box.

## Running manually (two terminals)

Create a shared state directory (stores the TOML hand-off files):

```bash
mkdir -p /tmp/multigroup-demo
```

Terminal 1 – receiver:

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role receiver \
  --config docs/testing/configs/node_receiver.toml \
  --state-dir /tmp/multigroup-demo
```

Terminal 2 – source:

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role source \
  --config docs/testing/configs/node_sender.toml \
  --state-dir /tmp/multigroup-demo
```

### Expected log flow

Receiver:

```
[12:00:01] Joining group 'loss-stream' (id=1, ip=239.1.1.10)...
[12:00:04] Joining group 'activation-stream' (id=2, ip=239.1.1.11)...
[12:00:06] [initial] group 'loss-stream' received packet 1/3 (21 bytes).
[12:00:07] Leaving group 'loss-stream' to exercise leave_group().
[12:00:08] [post-leave] group 'activation-stream' received packet 1/2 (25 bytes).
[12:00:09] No payloads observed for 'loss-stream' post-leave. Demo complete.
```

Source:

```
[12:00:00] Creating multicast group 'loss-stream'...
[12:00:01] Metadata for 'loss-stream' written to /tmp/multigroup-demo/loss-stream.toml.
[12:00:02] Waiting for receiver readiness marker /tmp/multigroup-demo/loss-stream.ready.toml...
[12:00:05] Broadcasting 3 packet(s) per group (pre-leave).
[12:00:07] Receiver left 'loss-stream'. Broadcasting 2 more packet(s) per group.
```

### Docker Compose automation

The repo includes a compose stack that bootstraps Postgres, the controller, and
two harness containers running the script:

```bash
cd examples/multicast-multi-group
mkdir -p tmp
docker compose up --build
```

Validation tips:

1. Watch the compose logs; you should see the same sequence as the manual run.
2. Confirm both `source` and `receiver` containers exit with code `0`.
3. Inspect `tmp/` afterwards. You’ll find TOML files such as
   `loss-stream.toml`, `loss-stream.ready.toml`, and `loss-stream.left.toml`.
4. Re-run `docker compose logs -f receiver` to ensure the receiver prints
   “No payloads observed for 'loss-stream' post-leave,” verifying the `leave_group`
   behavior.

## What’s inside

- `multi_group_demo.py` – main driver (source/receiver roles, TOML hand-off).
- `run_multi_group.py` – helper used by Docker to build/install `nextmini_py`
  and launch the script.
- `docker-compose.yml` – orchestrates Postgres, controller, receiver, and source
  containers on a dedicated bridge network.
- `tmp/.gitkeep` – empty directory that becomes the shared state volume.

The entire flow relies on TOML for configuration and coordination (dataplane
configs, controller config, and the hand-off files between roles), making it
easy to audit or modify without ad-hoc formats.
