---
title: "Testing Multicast and Membership Churn"
description: "Validates multicast delivery and membership churn behavior with the dockerized multicast test harness."
---

This guide validates end-to-end multicast behavior through `nextmini_py` using the dockerized harness in `examples/multicast-docker`.

## Scope

This run verifies:

- Group lifecycle (`create_group`, `group_is_ready`, `join_group`, `leave_group`).
- Route installation and readiness checks (`wait_for_topology_ready`, `wait_for_group_routes`).
- Payload delivery from source to multiple receivers.
- Membership churn handling after one receiver leaves.

## Prerequisites

- Docker and Docker Compose.
- `maturin` available inside the containers (handled by the harness scripts).
- From repo root: `examples/multicast-docker/` exists and contains `docker-compose.yml`.

## Harness inputs (what is currently wired)

The current harness uses these arguments and environment values:

- `GROUP_LABEL` (environment) → forwarded as `--group-label`.
- `RECEIVER_IDS` (environment) → forwarded as `--receiver-ids`.
- `SOURCE_NODE_ID` (environment) → forwarded as `--source-node-id`.
- `GROUP_TIMEOUT` (environment) → default wait timeout used for group creation, topology readiness, and receiver coordination.
- `RECEIVE_TIMEOUT_MS` (environment) → forwarded as `--receive-timeout-ms`.
- `ARTIFACT_DIR` (environment) → forwarded as `--artifact-dir` (defaults to `/artifacts`).
- `CLEAN_SHARED_DIRS` (environment, default `1`) → controls whether source clears `artifacts/` and shared tensors before each run.
- `SKIP_BUILD` / `NEXTMINI_PY_WHEEL` (environment) → choose between compiling in-container and using a provided wheel.
- `--chunk-size` (CLI) controls transfer chunk size.
- `--tensor-path` (CLI), or implicit `--generate-tensor` on source (the default path is `/workspace/tensors/tensor-auto-1g.pt`).
- `--sink-path` (CLI) overrides per-receiver payload output file.
- `--fec` (CLI, also accepts `FEC` env: `off` / `on`) is a runtime hint only; dataplane behavior is controlled by controller/runtime config.
- `--payload-count`, `--expected-bytes` (CLI) are optional reporting hints for logs and throughput calculations; receiver registration no longer depends on them.

## Run the harness

From the repository root:

```bash
cd examples/multicast-docker
docker compose up --build
```

If you want the source to skip cleaning prior artifacts:

```bash
CLEAN_SHARED_DIRS=0 docker compose up --build
```

## Validate behavior

1. Tail logs while services run:

```bash
docker compose logs -f controller source receiver_a receiver_b
```

2. Confirm the source logs show:

- group creation acknowledged (`group_is_ready` returned group ID/IP),
- multicast routes installed,
- payload/session send started.

3. Confirm each receiver logs:

- wait for `receiver-ready-<node_id>.json` under `artifacts/` (when using the docker harness),
- joined membership,
- routes installed locally,
- payload/session receive completion.

4. Inspect produced artifacts:

```bash
ls -lah artifacts/
cat artifacts/group-info.json
```

Expected outputs include receiver payload files (`receiver-*.bin`) and group metadata.

## Membership churn check

After the initial successful run, simulate one member leaving and rerun:

1. Stop `receiver_b`.
2. Re-run the source and `receiver_a`.
3. Verify only `receiver_a` receives payloads and controller logs show updated route pushes.

## Optional DB sanity checks

```bash
docker compose exec postgres psql -U pgusr -d nextmini -c "select * from groups;"
docker compose exec postgres psql -U pgusr -d nextmini -c "select * from group_members order by group_id, member_node_id;"
```

These queries confirm membership changes were persisted.
