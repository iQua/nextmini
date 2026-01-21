# SOCKS5 proxy examples (curl / wget / splice-test)

These examples forward traffic between **external endpoints** through Nextmini using a SOCKS5 ingress and the MAX transport. They are useful for validating proxy flows and for high-throughput stream splicing.

Before running, read the design overview: [Proxy flows (MAX mode + SOCKS5)](../design/proxy-flows.md).

## curl (single machine)

Runs `curl` through a SOCKS5 proxy to an HTTP server.

```bash
cd examples/curl
docker compose up --build
```

If you want to change the number of internal hops, regenerate the configs first:

```bash
cd examples/curl
python nodes.py -n 3
```

## wget (single machine)

Uses `proxychains` + `wget` through SOCKS5 to download a file from an HTTP server.

```bash
cd examples/wget
docker compose up --build
```

## splice-test (high-throughput splicing)

Splices a long-lived TCP stream through multiple hops and reports throughput.

```bash
cd examples/splice-test
docker compose up --build
```

To customize hop count, regenerate configs:

```bash
cd examples/splice-test
python nodes.py -n 3
```

## What to look for in logs

- Dataplane nodes: `tcp_max: Connection accepted ...` and `connector: Spliced connection for flow ...`
- The edge hop that connects to the final server: `Connected to <ip:port> without max header.`

If routing looks wrong (for example, the edge hop connects to the external server immediately), check `controller-config.toml` for the intended route and ensure external endpoints are in the `external_base_addr` range.

