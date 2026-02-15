---
title: "Splice Throughput Example"
description: "Run a proxied client-server TCP stream through local Nextmini dataplane nodes and observe splice throughput."
---

This example places an external client and external server on opposite sides of a three-node Nextmini dataplane path. The default route in `examples/splice-test/controller-config.toml` is `[1, 2, 3, 4, 5]`, where node `1` is the external client and node `5` is the external server.

The client (`examples/splice-test/src/client.rs`) performs a SOCKS5 handshake to `172.16.8.5:8081`, then continuously sends data to the server at `172.16.8.8:8080`. The server (`examples/splice-test/src/server.rs`) reports receive throughput.

From the repository root:

```bash
cd examples/splice-test
docker compose build
docker compose up
```

In another terminal, watch client and server logs:

```bash
cd examples/splice-test
docker compose logs -f external_client
docker compose logs -f external_server
```

Successful startup is indicated by lines such as `SOCKS5 tunnel established successfully` in `external_client` logs and `Throughput:` in `external_server` logs.

If you want a different number of dataplane nodes, regenerate before rebuilding:

```bash
cd examples/splice-test
python nodes.py -n 5
rg "^route = " controller-config.toml
rg "target_ip" src/client.rs
docker compose build
docker compose up
```

Stop containers when finished:

```bash
docker compose down
```
