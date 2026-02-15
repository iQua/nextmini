---
title: "HTTP Throughput with curl"
description: "Runs an HTTP download from a client to a server through a multi-hop Nextmini dataplane."
---

This example shows how to send real traffic across a path of Nextmini dataplane nodes using a familiar `curl` transfer.

The container layout is intentionally simple: one external client, one external server, and three internal dataplane nodes by default. The client sends requests through a SOCKS5 proxy on the first dataplane node (`172.16.8.5:8081`) and the server responds from `/large_test.dat` on port `8080`.

The route is explicitly defined so the path is deterministic. In this default setup, the configured path is:

`1 -> 2 -> 3 -> 4 -> 5`

where `1` is the external client, `2..4` are the dataplane nodes, and `5` is the external server.

## Run this example

From the repository root, open a shell in `examples/curl`:

```bash
cd examples/curl
```

Build and launch everything in one terminal:

```bash
docker compose build
docker compose up
```

`docker compose build` must run before the first launch so the local `nextmini_datapath`, `nextmini_external_client`, and `nextmini_external_server` images are current.

When startup is complete, `external_client` prints metrics from its `curl` run after a short warm-up.

If you see an address-space conflict from Docker networking, clean up stale networks and volumes first:

```bash
docker system prune -a
```

## What happens under the hood

The runtime behavior is driven by two generated files plus one static configuration:

- `docker-compose.yml`: defines the client, server, controller, Postgres, and each dataplane node service.
- `controller-config.toml`: supplies the explicit route as `route = [1, 2, 3, ...]`.
- `src/client.sh`: waits briefly, then runs:
  - a SOCKS5 proxied `curl` call,
  - performance output via `-w`,
  - an optional throughput conversion to Gbps.

The default composition has one client, one server, and three dataplane nodes. Every run follows this fixed startup shape:

1. Controller starts and opens the WebSocket endpoint.
2. Each dataplane node boots after its predecessor to avoid connection churn.
3. The client starts after the chain is available.
4. The server exposes a large file locally and stays serving it.

## Change the path length (recommended)

The example includes a helper script to regenerate the topology for different hop counts:

```bash
python nodes.py --nodes 6
```

You can also use the short form:

```bash
python nodes.py -n 6
```

This command updates three files in place:

- `docker-compose.yml`: adds nodes `node2` to `node7` for `--nodes 6` (node2 starts at `172.16.8.5`, node7 becomes `172.16.8.10`, server moves to `172.16.8.11`).
- `controller-config.toml`: updates `full_mesh_config.n_nodes` and the `route` line so the route becomes `1, 2, 3, 4, 5, 6, 7, 8`.
- `src/client.sh`: updates the target server URL to match the new external server IP.

Rebuild and relaunch after changing the topology:

```bash
docker compose build
docker compose up
```

The minimum supported value is `--nodes 1`. The helper uses fixed IPv4 space (`172.16.8.0/24`), so very large values will collide with subnet limits.

## Reproducible checks

You can verify the active command path before starting:

```bash
rg '^route = ' controller-config.toml
```

You can inspect logs by service:

```bash
docker compose logs -f external_client
docker compose logs -f node2
docker compose logs -f external_server
```

You should see a successful download at least once in the client log, with an HTTP status and timing lines like:

```text
--- Performance Metrics (Full Download) ---
HTTP Code: 200
Total Time: 0.00s
Download Speed: 1234567.89 bytes/sec
Content Length Downloaded: <bytes>
--- End Metrics ---
```

## Tear down

When you are done, stop the example:

```bash
docker compose down
```

If you started it in the foreground, `Ctrl+C` in that terminal is also valid. This keeps the generated files in place so you can reproduce a new launch with the same shape.
