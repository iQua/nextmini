# Examples overview

All runnable scenarios live under `examples/`. This site is the canonical place for their instructions (we avoid Markdown READMEs inside `examples/`).

## Quick start

Most examples follow the same pattern:

```bash
cd examples/<name>
docker compose up --build
```

## Example layers

### 1) TUN (overlay) examples

Run unmodified applications (ping/iperf3/PyTorch) over a virtual network interface.

- [Simple (iperf3)](simple.md)
- [Topology & Routes](routes.md)
- [PyTorch (DDP)](pytorch.md)

### 2) User-space flow examples

Exercise controller-managed flows that are generated and consumed inside the dataplane process (SmolTCP TCP or lossless sessions).

- [Controller-managed flows (lossless or SmolTCP)](simple-flow.md)
- [ns-flow (flow scaling)](ns-flow.md)

### 3) Proxy flows (MAX mode)

Forward TCP streams through the topology using connection-on-demand and (optionally) SOCKS5 ingress.

- [MAX mode (internal)](simple-max.md)
- [SOCKS5 Proxy (splice-test / curl)](proxy.md)

### 4) Python API + multicast examples

Embed the dataplane in Python and drive multicast and lossless transfers directly in-process.

- [Python API quickstart](pytorch_python_api.md)
- [Multicast flow lifecycle](multicast-flow.md)
- [Multicast Docker](multicast-docker.md)
- [LP toy demo](lp-toy.md) (includes probing + route installation)
- [LP multicast tree selection](lp.md)
- [RL Training (GSM8K)](rl.md)

### 5) Deployment

- [Single host (Docker Compose)](single-host.md)
- [Bare metal](bare-metal.md)
- [Multi-node (manual, no swarm)](public-network.md)
- [Multi-node (Docker Swarm)](simple-swarm.md)
- [Batch sync (SSH + rsync)](batch-sync.md)
- [Local Controller + Fly.io (experimental)](localserver-flyio.md)
