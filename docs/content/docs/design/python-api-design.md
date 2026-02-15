---
title: "Python API Design"
description: "Implementation notes for how nextmini_py is embedded into the Rust dataplane."
---

`nextmini_py` is a thin wrapper around the Rust dataplane. The key point is that there is no second implementation of routing, flow policy, or transport. The Python API simply creates and configures the same node actors you would run with the binary entrypoint, then exposes a narrow API surface for app code.

## Runtime bootstrap

The `Dataplane` constructor reads the TOML file, builds a `LocalConfig`, and forces `enable_local_interface = false` so the node runs in user-space mode suitable for Python embedding. It then creates a `Conductor`, obtains the built `ProcessorHandle` and `ControllerInterfaceHandle`, and uses an internal Tokio runtime for all background work.

From there, `Dataplane::new` performs two explicit wiring steps before returning:

1. It creates a `PythonInterfaceHandle` with the configured channel capacity and backpressure policy.
2. It attaches that handle to the processor via `connect_python_interface` and to the controller interface via `attach_python_interface`.

After that, `Conductor::run` is spawned in the same process. Startup handshake, server startup, controller event handling, and scheduler wiring run as background tasks, so Python code can begin interacting with a live runtime immediately.

## Where packets enter and exit

Python traffic has two paths. For sending, both synchronous and asynchronous payload APIs end up as `Packet` objects and call the processor with `process_packet_blocking`.

For receiving, `register_receiver_from_node` and `register_receiver_for_group` compute a `FlowId` and register a receiver slot in `PythonInterfaceHandle`. When a packet targets that flow and resolves to the local node, `Processor::send_packet` calls `python_interface.deliver` first; if a listener exists, payload bytes are stripped and delivered directly as `PayloadDelivery`. If no listener is ready, normal forwarding continues to user-space sender/local socket.

That boundary is channel based, so `channel_backpressure` controls whether full queues block or drop at that edge.

## How a receive method works

`PacketReceiver` wraps a shared `mpsc::Receiver<PayloadDelivery>`.
`register_receiver...` creates that receiver and returns it to Python.
`recv` uses the Python runtime loop and can block with an optional timeout.
`recv_async` returns an awaitable and uses the same channel path.

`send_to_node` builds a TCP-style packet from a `PacketView`, derives source/destination ports and IPs from the local config, and injects it into the same processor path used by normal Rust traffic.

## How control operations flow

Group and multicast calls (`create_group`, `join_group`, `leave_group`, `set_group_routes`) are RPC messages sent through `controller.send`. The controller responds over websocket with events such as `GroupCreated`, `GroupRoutesInstalled`, `LocalMemberJoined`, and `TopologyReady`.

`PythonInterfaceHandle` receives those events, but `Dataplane::wait_for_event_matching` does more than just block on the next event. It first checks an in-memory `event_stash` for an already-arrived event and, if none matches, polls the socket-facing stream and stashes unmatched events. This is what makes timeout-aware waiting methods stable under bursts.

`leave_group` also emits a synthetic `LocalMemberLeft` event to avoid waiting on delayed control-plane echo.

## Lossless integration

Lossless methods also run inside existing runtime state. `Dataplane` captures the shared `LosslessRuntimeHandle` from `Conductor` and delegates sender/receiver setup to it.

`send_data` validates the request (`receiver_ids`, `chunk_size`, non-empty buffer) and starts a sender session.
`send_data` and `receive_data` compute the same session id format from `(group_id, source_node_id)`; the sender uses its local node id as source, while the receiver passes explicit `source_node_id`.

`receive_data` and `receive_data_async` register receive requests, preallocate a sink buffer when requested, and keep that buffer in a local registry keyed by session id until explicitly consumed.

`lossless_wait` and `lossless_wait_async` poll runtime completion and then stop the session so handle state is cleaned up.

`get_data_buffer(session_id, consume=True)` returns reconstructed bytes through the same immutable `PacketView` type used by normal send paths.

## What to remember

This design keeps Python as a front end, not a separate implementation:

- no separate route table is maintained in Python
- no separate session protocol is implemented in Python
- no separate controller client logic in Python

All of those stay in the Rust dataplane, with `nextmini_py` only binding method calls and delivery callbacks to Python objects.
