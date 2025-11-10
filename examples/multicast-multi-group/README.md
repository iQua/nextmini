# Multicast Multi-Group

This example shows how to drive several multicast groups from Python, exercise
`leave_group`, and coordinate all runtime parameters via TOML files (no JSON or
ad-hoc markers). It uses two roles:

- **Source** – creates multiple groups, broadcasts an initial burst to every
  group, waits for the receiver to leave one group, then sends another burst to
  prove traffic no longer arrives on the departed group.
- **Receiver** – joins every group defined in the TOML config, confirms local
  membership/routes, consumes the first burst, calls `leave_group` on a selected
  label, and verifies the dataplane stops forwarding packets for that group.

The Docker Compose workflow now runs two receiver instances: one exercises
`leave_group` on `loss-stream`, while the second leaves `activation-stream`
instead so it remains subscribed to `loss-stream` and proves multicast delivery
continues for listeners that stay behind.

## Prerequisites

1. Build and install `nextmini_py` (see `docs/examples/pytorch_python_api.md`).
2. Launch a controller plus the dataplane nodes you plan to exercise (one sender
   and one or two receivers). The `docs/testing/configs/node_sender.toml`,
   `node_receiver.toml`, and `node_receiver_sticky.toml` files work
   out-of-the-box.

## Running manually (three terminals)

Create a shared state directory (stores the TOML hand-off files):

```bash
mkdir -p /tmp/multigroup-demo
```

> **Note:** The demo now wipes the chosen state directory exactly once before
> the first role starts and again after the last role exits, so you no longer
> need to manually delete `tmp/` between runs. This coordination works even if
> the source and receiver launch simultaneously. Pass `--preserve-state-dir` if
> you intentionally want to reuse the existing TOML files (for example, when
> restarting just one role).

> The helper launcher also creates per-role virtualenvs inside this folder (see
> `.venv/source` and `.venv/receiver`) and wipes them before and after every
> run, guaranteeing a fresh `nextmini_py` build.

Terminal 1 – receiver:

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role receiver \
  --config docs/testing/configs/node_receiver.toml \
  --state-dir /tmp/multigroup-demo \
  --participant-tag receiver-primary
```

Terminal 2 – sticky receiver (keeps `loss-stream` active):

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role receiver \
  --config examples/multicast-multi-group/configs/node_receiver_sticky.toml \
  --state-dir /tmp/multigroup-demo \
  --leave-label activation-stream \
  --participant-tag receiver-sticky
```

Terminal 3 – source:

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role source \
  --config docs/testing/configs/node_sender.toml \
  --state-dir /tmp/multigroup-demo \
  --wait-tags receiver-primary receiver-sticky \
  --leave-tag receiver-primary
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
three harness containers (two receivers plus one source) running the script:

```bash
cd examples/multicast-multi-group
docker compose up --build
```

An init-style `state-cleaner` container now runs first and wipes `tmp/`
(including any stale `.ready.toml` files) before the controller, receiver, and
source services start, so leftovers from previous runs never interfere with the
handshake.

The compose services inherit the coordinated cleanup logic, so whichever role
starts first wipes the shared `tmp/` directory and the last role to exit clears
it again. Add `--preserve-state-dir` to either role's command if you need to
keep the TOML artifacts between container restarts.

Validation tips:

1. Watch the compose logs; you should see both receivers report ready before the
   source begins broadcasting.
2. Confirm `source`, `receiver`, and `receiver_sticky` containers exit with code `0`.
3. Peek at `tmp/` while the containers are running to watch TOML files such as
   `loss-stream.toml`, `loss-stream.ready.toml`, and `loss-stream.left.toml`
   appear—they are removed automatically once the roles exit.
4. Check the short-lived `state-cleaner` logs if you need to verify the shared
   directory was purged before the other services launched.
5. Re-run `docker compose logs -f receiver` (and `receiver_sticky`) to ensure the
   first receiver observes no traffic on `loss-stream` post-leave while the sticky
   receiver continues to receive it, validating multicast behaviour.
