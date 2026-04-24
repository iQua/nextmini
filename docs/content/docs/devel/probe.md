---
title: "Link Probing"
description: "Measure link bandwidth between dataplane nodes using the probe tool."
---

The probe tool triggers a bandwidth measurement between two dataplane nodes by inserting a row into PostgreSQL. The controller picks up the request, dispatches probe traffic through the sender, and the receiver reports throughput back.

## How it works

1. A row is inserted into the `probe_requests` table.
2. A PostgreSQL trigger fires `pg_notify('probe_requested', ...)`.
3. The controller receives the notification and sends a `ProbeLink` message to the sender node over WebSocket.
4. The sender builds \~1000 probe packets (1360 B payload, 1400 B total with IP+TCP headers) and pushes them to the receiver over the existing TCP link.
5. The receiver detects probe packets by a reserved flow ID (`127.0.0.1 → 127.0.0.2`), tracks first-to-last packet arrival time, and computes throughput.
6. The result is reported back to the controller and persisted in `probe_results`.

## Prerequisites

- Controller and dataplane nodes are running and connected.
- PostgreSQL is reachable from the machine running the tool.
- Python 3 with `psycopg2-binary`:

```bash
pip install psycopg2-binary
```

## Usage

The tool lives at `tools/probe/probe_link.py`. Run it from the repository root:

```bash
# Probe from node 2 to node 1 (default ~1.36 MB payload)
python tools/probe/probe_link.py --from 2 --to 1

# Larger probe (5 MB)
python tools/probe/probe_link.py --from 3 --to 1 --bytes 5000000

# Non-default DB port (e.g. when a local postgres conflicts with docker)
python tools/probe/probe_link.py --from 2 --to 1 --db-port 15432

# Insert request without waiting for the result
python tools/probe/probe_link.py --from 2 --to 1 --no-wait
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--from` | required | Sender node ID |
| `--to` | required | Receiver node ID |
| `--bytes` | 1360000 | Total probe payload (\~1000 packets at 1360 B each) |
| `--timeout` | 30 | Seconds to wait for a result |
| `--no-wait` | off | Insert the request and exit immediately |
| `--db-host` | localhost | PostgreSQL host |
| `--db-port` | 5432 | PostgreSQL port |
| `--db-name` | nextmini | Database name |
| `--db-user` | pgusr | Database user |
| `--db-pass` | pgpwrd | Database password |

## Inserting a probe manually

You can also insert a probe request directly with SQL:

```sql
INSERT INTO probe_requests (from_node_id, to_node_id, probe_bytes)
VALUES (2, 1, 1360000);
```

Then query the result:

```sql
SELECT * FROM probe_results ORDER BY created_at DESC LIMIT 5;
```

## Limitations

- Probing requires an established topology connection between the two dataplane nodes. The connection may have been initiated by either endpoint, so both directions can be measured once the topology is ready.
