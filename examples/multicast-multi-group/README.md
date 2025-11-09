# Multicast Multi-Group Smoke Test

This example demonstrates how to drive several multicast groups from Python and
exercise the `leave_group` API end-to-end. It is intentionally small and uses a
shared “state directory” on disk instead of Postgres or extra services so you
can run it in two terminals.

## Prerequisites

1. Build and install `nextmini_py` (see `docs/examples/pytorch_python_api.md`).
2. Launch a controller and two dataplane nodes (one acting as the source, the
   other as the receiver). You can reuse the configs under
   `docs/testing/configs/` or any equivalent setup.

## Running the demo

Terminal 1 (receiver – joins two groups, then leaves one after receiving the
first burst of packets):

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role receiver \
  --config docs/testing/configs/node_receiver.toml \
  --state-dir /tmp/multi-group-demo
```

Terminal 2 (source – creates the groups, sends packets, waits for the receiver
to leave the first group, then sends a second burst to prove no more packets are
delivered to the departed group):

```bash
python examples/multicast-multi-group/multi_group_demo.py \
  --role source \
  --config docs/testing/configs/node_sender.toml \
  --state-dir /tmp/multi-group-demo
```

Both processes log their progress. Watch the receiver output to see when it
stops observing traffic for the group it left while continuing to receive on the
remaining group.

### Docker Compose shortcut

To avoid juggling terminals manually, use the bundled Compose stack (brings up
Postgres, the controller, and two harness containers running the script):

```bash
cd examples/multicast-multi-group
mkdir -p tmp
docker compose up --build
```

The receiver container exits only after confirming `leave_group` stopped
delivery; the source follows once all payloads are transmitted. Logs for each
service are available via `docker compose logs -f <service>`.

## What it covers

- Creating multiple multicast groups from Python.
- Sharing the resulting `group_id`/`group_ip` metadata with receivers.
- Joining each group, verifying membership/route installation, and registering
  a multicast receiver.
- Calling `leave_group` and confirming the dataplane no longer delivers packets
  for that group.

The script is heavily commented so you can adapt it for more complex
distributed-ML scenarios or bake the handshake logic into your own orchestration
layer.
