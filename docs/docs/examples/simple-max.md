# MAX mode (internal)

`examples/simple-max` demonstrates **MAX mode forwarding** without SOCKS5 ingress.

In MAX mode, a dataplane node forwards outbound traffic through a connection-on-demand TCP “MAX” stream (instead of the normal packet pipeline). This can improve throughput for long-lived streams.

For SOCKS5 ingress (proxying external TCP through the topology), see: [SOCKS5 proxy examples](proxy.md).

## Run

```bash
cd examples/simple-max
docker compose up --build
```

## What to look for

- Node 1 is configured with `operating_mode = "max"` in the controller config.
- Logs often include `tcp_max` / `connector` messages indicating that traffic is being forwarded through MAX streams.

Follow logs:

```bash
cd examples/simple-max
docker compose logs -f node1 node2 node3
```

## Cleanup

```bash
cd examples/simple-max
docker compose down -v
```

