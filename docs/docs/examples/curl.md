# Curl Through SOCKS5

`examples/curl` runs `curl` through a SOCKS5 proxy to an HTTP server, forwarding the stream hop-by-hop through Nextmini.

Before running, read: [Proxy flows (MAX mode + SOCKS5)](../design/proxy-flows.md).

## Run

```bash
cd examples/curl
docker compose up --build
```

## Change hop count

```bash
cd examples/curl
python nodes.py -n 3
```

## What to look for

- Dataplane logs: `tcp_max: Connection accepted ...` and `connector: Spliced connection for flow ...`
- The edge hop that connects to the final server: `Connected to <ip:port> without max header.`
