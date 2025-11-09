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

### Expected console output (manual run)

Receiver (abridged):

```
[12:00:01] Joining group 'loss-stream' (id=1, ip=239.1.1.10)...
[12:00:03] Joining group 'activation-stream' (id=2, ip=239.1.1.11)...
[12:00:05] [initial] group 'loss-stream' received packet 1/3 (21 bytes).
[12:00:05] [initial] group 'activation-stream' received packet 1/3 (25 bytes).
...
[12:00:07] Leaving group 'loss-stream' to exercise leave_group().
[12:00:08] Waiting for 2 additional packet(s) on 1 remaining group(s).
[12:00:08] [post-leave] group 'activation-stream' received packet 1/2 (25 bytes).
[12:00:10] Confirming no packets arrive on 'loss-stream' after leave_group().
[12:00:10] No payloads observed for 'loss-stream' post-leave. Demo complete.
```

Source:

```
[12:00:01] Creating multicast group 'loss-stream'...
[12:00:02] Metadata for 'loss-stream' written to /tmp/multi-group-demo/loss-stream.json.
[12:00:02] Creating multicast group 'activation-stream'...
[12:00:03] Waiting for receiver readiness marker /tmp/multi-group-demo/loss-stream.ready...
[12:00:05] Broadcasting 3 packet(s) per group (pre-leave).
[12:00:07] Waiting for receiver to leave group 'loss-stream' (marker: /tmp/.../loss-stream.left)...
[12:00:08] Receiver left 'loss-stream'. Broadcasting 2 more packet(s) per group.
```

You can change the group names or packet counts via CLI flags; the log pattern
stays the same (initial packets on both groups, receiver leaves one, only the
remaining group sees the post-leave traffic).

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

### Validating the Compose run

1. Watch the combined logs while `docker compose up` runs. You should see the
   same sequence as the manual run (source creates two groups, receiver leaves
   one, post-leave packets only hit the other group).
2. Ensure both containers exit with code `0` (Docker prints “Exited (0)” when
   each completes). Any non-zero exit indicates a timeout or unexpected payload.
3. Optional: inspect the shared state directory (bind-mounted as `tmp/`). It
   should contain `loss-stream.json`, `loss-stream.ready`, `loss-stream.left`,
   etc., proving the handshake completed.
4. Re-run with `docker compose logs -f receiver` to confirm the receiver reports
   “No payloads observed for 'loss-stream' post-leave,” which is the key
   assertion that `leave_group` worked.

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
