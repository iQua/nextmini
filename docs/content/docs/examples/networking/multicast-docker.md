---
title: "Multicast Docker Harness"
description: "Runs a source and two receivers with lossless multicast, artifact capture, and checksum-based verification."
---

The `examples/multicast-docker` folder is a reproducible multicast harness built around `nextmini_py`. It launches Postgres, controller, one source node, and two receiver nodes in a dedicated Docker network.

This page focuses on running the harness. For lifecycle semantics and packet-level behavior, see [Example: Multicast Flow Lifecycle](/docs/examples/networking/multicast-flow) and [Multicast integration guide](/docs/devel/multicast).

The source process creates a group, installs a star DAG from source to all receivers, waits for receiver readiness files, and sends a tensor payload. Each receiver joins the group, receives the payload losslessly, and writes a local artifact (`receiver-<node>.bin`). Shared metadata and outputs are written under `examples/multicast-docker/artifacts`.

## Run

From the repository root:

```bash
cd examples/multicast-docker
docker compose up --build
```

The default run uses:

- source node id `1`
- receiver node ids `2,3`
- group label `demo-multicast`
- automatic tensor generation when no `--tensor-path` is provided

## Verification

Watch runtime logs:

```bash
cd examples/multicast-docker
docker compose logs -f controller source receiver_a receiver_b
```

Then run the verification script from the repository root:

```bash
uv run examples/multicast-docker/scripts/verify_received_files.py
```

A passing run prints `VERIFICATION PASSED: All receivers got identical data`.

You can also inspect generated artifacts:

```bash
cd examples/multicast-docker
ls -lah artifacts
cat artifacts/group-info.json
cat artifacts/tensor-metadata.json
```

## Cleanup

```bash
cd examples/multicast-docker
docker compose down --volumes
```

Remove generated receiver payloads and metadata if you want a fully clean workspace:

```bash
cd examples/multicast-docker
rm -f artifacts/receiver-*.bin artifacts/group-info.json artifacts/tensor-metadata.json artifacts/receiver-ready-*.json
```
