# Fly.io deployment (experimental)

The repository includes `examples/flyio`, which contains scripts and manifests for deploying a controller and dataplane nodes to Fly.io.

This workflow changes frequently (and depends on Fly.io platform details), so treat it as **experimental**. Prefer the Docker-based examples for a known-good baseline, and use `examples/flyio` as a starting point if you already deploy other services on Fly.io.

## What to check before using it

- Controller connectivity (the controller must be reachable by dataplane nodes over WebSocket).
- Database connectivity (Postgres must be reachable by the controller).
- Whether your deployment requires IPv6 support (Fly internal networking may be IPv6-first depending on your setup).

## Entry points in the repo

- `examples/flyio/deploy.sh`
- `examples/flyio/fly.*.toml`
- `examples/flyio/Dockerfile.*`

