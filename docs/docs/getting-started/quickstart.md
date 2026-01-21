# Quickstart (Docker)

This is the fastest way to run Nextmini and validate that TUN + routing work end-to-end.

## Prerequisites

- A Docker runtime (Docker Engine on Linux, or Docker Desktop on macOS/Windows)

Note: some examples (e.g. namespace scaling or `network_mode: host` deployments) require a Linux host.

## Run

```bash
cd examples/simple
docker compose up --build
```

Then follow the tutorial to send traffic through the emulated network:

- [Simple `iperf3` example](../examples/simple.md)

## Stop

```bash
cd examples/simple
docker compose down
```

