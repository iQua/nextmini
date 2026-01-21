# Examples overview

All runnable scenarios live under `examples/`. This documentation site is the canonical place for their instructions (we avoid Markdown READMEs inside `examples/`).

## Quick start

Most examples follow the same pattern:

```bash
cd examples/<name>
docker compose up --build
```

Then inspect logs with:

```bash
docker compose logs -f
```

## Getting started

- **Simple `iperf3`**: [Running Nextmini in Docker Containers](simple.md)
- **Topology + routes**: [Defining the Network Topology and Routes](routes.md)
- **Namespace mode** (many nodes on one host): [Namespace](namespace.md)

## Python API and multicast

- **Python API quickstart**: [Python API quickstart (PyTorch-friendly)](pytorch_python_api.md)
- **Multicast lifecycle**: [Example: Multicast Flow Lifecycle](multicast-flow.md)
- **End-to-end multicast harness**: [Multicast Docker Example](multicast-docker.md)
- **LP multicast tree selection**: [LP Multicast Tree Selection](lp.md) and [Toy Demo](lp-toy.md)
- **RL example (trainer/worker over `nextmini_py`)**: [RL Training on GSM8K](rl.md)

## Proxy / MAX transport

These examples exercise SOCKS5 + MAX forwarding (see [Proxy flows](../design/proxy-flows.md)):

- [SOCKS5 proxy examples (splice-test / curl)](proxy.md)

## Deployments

- **Bare metal**: [Bare Metal Deployment](bare-metal.md)
- **Local controller + Fly.io nodes (experimental)**: [Local Server + Fly.io](localserver-flyio.md)
- **Public network without swarm (experimental)**: [Public network deployment](public-network.md)
