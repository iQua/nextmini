# Quickstart (Docker)

This is the fastest way to run Nextmini and validate that TUN + routing work end-to-end.

## Prerequisites

- A Docker runtime
- A Linux host is the easiest option. On macOS/Windows, run in a Linux VM.

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

