# Hybrid deployment: local controller + Fly.io nodes (experimental)

This deployment runs the controller + Postgres locally while deploying dataplane nodes on Fly.io.

The scripts live in `examples/localserver-flyio/`.

## 1) Start controller + Postgres locally

```bash
cd examples/localserver-flyio
uv run start-controller.py
```

The script starts `docker compose up -d --build` and prints a `ws://<public-ip>:3000` address for Fly.io nodes to connect to.

Useful commands:

```bash
cd examples/localserver-flyio
docker compose logs -f controller
docker compose down
```

## 2) Deploy Fly.io dataplane nodes

```bash
cd examples/localserver-flyio
uv run deploy-flyio.py --public-ip <YOUR_PUBLIC_IP> --nodes 2
```

Verify a node:

```bash
flyctl status -a nextmini-node-1
flyctl logs -a nextmini-node-1 -n
```

## 3) Ring all-reduce tests (optional)

The ring launcher lives in `examples/localserver-flyio/ring-emu/`:

```bash
cd examples/localserver-flyio/ring-emu
uv run run_with_cleanup.py --help
```

## Cleanup

Local controller:

```bash
cd examples/localserver-flyio
docker compose down
```

Fly.io apps:

```bash
cd examples/localserver-flyio
uv run cleanup.py --help
```
