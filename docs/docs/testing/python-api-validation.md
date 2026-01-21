# Python API validation

This page collects a few concrete checks for validating `nextmini_py` against the current dataplane/controller codebase.

## What to validate

- **Unicast delivery**: `send_to_node` → `register_receiver_from_node` → `recv()`
- **Multicast control-plane**: `create_group` / `join_group` / `set_group_routes`
- **Lossless transfers**: `send_data` / `receive_data` + `lossless_wait` + `get_data_buffer`
- **Membership churn**: join/leave triggers route rebuilds and local delivery changes

## Prerequisites

- CPython 3.13
- `maturin`
- Docker (for the end-to-end multicast harness)

## 1) Run the Python API unit tests

Point PyO3 at your Python 3.13 interpreter and run the `nextmini_py` dev tests:

```bash
PYO3_PYTHON=/path/to/python3.13 \
cargo nextest run -p nextmini_py --no-default-features --features dev-tests
```

## 2) Run the end-to-end multicast harness in Docker

The `examples/multicast-docker` scenario boots Postgres, the controller, and multiple dataplane nodes, then exercises:

- multicast group creation and route installation
- lossless transfer setup (`receive_data`)
- lossless delivery (`send_data` + `lossless_wait`)

Run it from the repo root:

```bash
cd examples/multicast-docker
docker compose up
```

Useful inspection commands:

```bash
docker compose logs -f controller
docker compose logs -f source receiver_a receiver_b
docker compose exec postgres psql -U pgusr -d nextmini -c "select * from groups;"
```

## 3) Validate join/leave churn

During (or after) the harness, force membership updates and confirm the controller pushes `InstallGroupRoutes` updates:

1. On a receiver container, call `join_group(group_id)` and wait with `wait_for_local_membership(group_id, timeout_ms=...)`.
2. Call `leave_group(group_id)` and confirm subsequent routes drop local delivery for that node.

If you are writing a new churn reproducer, use the Python event waiters (`group_is_ready`, `wait_for_group_routes`, `wait_for_local_membership`) instead of polling Postgres directly.

