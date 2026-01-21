# splice-test (SOCKS5 + MAX)

`examples/splice-test` splices a long-lived TCP stream through multiple hops and reports throughput.

Before running, read: [Proxy flows (MAX mode + SOCKS5)](../design/proxy-flows.md).

## Run

```bash
cd examples/splice-test
docker compose up --build
```

## Change hop count

```bash
cd examples/splice-test
python nodes.py -n 3
```

## What to look for

- Dataplane logs: `tcp_max: Connection accepted ...` and `connector: Spliced connection for flow ...`
