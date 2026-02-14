---
title: "Python API Validation (Multicast + Membership Churn)"
description: ""
---


This guide validates end-to-end multicast behavior through `nextmini_py` using the dockerized harness in `examples/multicast-docker`.

## Scope

This run verifies:

- Group lifecycle (`create_group`, `group_is_ready`, `join_group`, `leave_group`).
- Route installation waiters (`wait_for_group_routes`, `wait_for_local_membership`).
- Payload delivery from source to multiple receivers.
- Membership churn handling after one receiver leaves.

## Prerequisites

- Docker and Docker Compose.
- `maturin` available inside the containers (handled by the harness scripts).
- From repo root: `examples/multicast-docker/` exists and contains `docker-compose.yml`.

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
