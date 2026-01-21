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

### 2) Namespace (Linux)

Run many dataplane nodes as Linux network namespaces on a single host.

- [Namespace scaling](namespace.md)
- [ns-flow](ns-flow.md)

### 3) User-space flows

Exercise controller-managed flows that are generated and consumed inside the dataplane process.

- [Lossless flows](simple-flow.md)
- [SmolTCP flows](smoltcp-flows.md)

### 4) Proxy flows

Forward TCP streams through the topology using SOCKS5 ingress and the MAX transport.

- [splice-test](splice-test.md)
- [curl](curl.md)

### 5) Python API examples

- [RL Training (GSM8K)](rl.md)
- [LP toy demo](lp-toy.md)

### 6) Deployment

- [Single host (Docker Compose)](single-host.md)
- [Bare metal](bare-metal.md)
- [Manual deployment](public-network.md)
- [Multi-node (Docker Swarm)](simple-swarm.md)
- [Batch sync (SSH + rsync)](batch-sync.md)
- [Local Controller + Fly.io (experimental)](localserver-flyio.md)
