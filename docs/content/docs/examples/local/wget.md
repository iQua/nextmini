---
title: "Wget Over SOCKS5 Example"
description: "Download a file through a local Nextmini multi-hop path using proxychains and wget."
---

This example runs an external client, three Nextmini dataplane nodes, and an external server in one Docker network. The configured routes in `examples/wget/controller-config.toml` are `[1, 2, 3, 4, 5]` and `[5, 4, 3, 2, 1]`. The client script repeatedly fetches `http://172.16.8.8:8080/file.txt` through SOCKS5 proxy `172.16.8.5:8081`.

From the repository root:

```bash
cd examples/wget
docker compose -f docker-compose.yaml build
docker compose -f docker-compose.yaml up
```

In another terminal, verify the client sees successful downloads:

```bash
cd examples/wget
docker compose -f docker-compose.yaml logs -f external_client
```

A healthy run shows `Download completed. File content: Hello from Nextmini!`.

You can also verify the server is serving the file and receiving requests:

```bash
docker compose -f docker-compose.yaml logs -f external_server
```

Look for `GET /file.txt HTTP/1.1" 200`.

To verify the SOCKS proxy settings baked into the client image:

```bash
docker exec external_client cat /etc/proxychains.conf
```

Stop the example:

```bash
docker compose -f docker-compose.yaml down
```
