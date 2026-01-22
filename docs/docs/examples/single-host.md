# Single Host Deployment (Docker Compose)

Most Nextmini examples run the same way: a local Docker Compose stack boots **Postgres**, the **controller**, and a set of **dataplane nodes** on one machine.

## Start

From any example directory that contains a `docker-compose.yml`:

```bash
cd examples/<example>
docker compose up --build
```

## Inspect

Common commands:

```bash
cd examples/<example>
docker compose logs -f
docker compose ps
docker compose exec node1 /bin/bash
```

## Stop

```bash
cd examples/<example>
docker compose down -v
```

## Notes

- TUN-based examples require a privileged container (or `CAP_NET_ADMIN`) so the dataplane can create and configure a TUN device.
- If you hit Docker subnet overlap errors, change the example’s compose subnet (or remove unused Docker networks on your machine).

