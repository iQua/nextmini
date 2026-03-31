# Link Probe Tool

Trigger a bandwidth probe between two dataplane nodes and retrieve the result.

## How it works

1. The script inserts a row into the `probe_requests` table in PostgreSQL.
2. The controller receives a PG NOTIFY, sends a `ProbeLink` message to the sender node.
3. The sender node pushes ~1000 probe packets (1360 B payload each, 1400 B total) to the receiver via the existing TCP link.
4. The receiver measures arrival time of the first through last packet and computes throughput.
5. The result is reported back to the controller and persisted in `probe_results`.
6. The script polls `probe_results` until the row appears.

## Prerequisites

- Controller and dataplane nodes are running and connected.
- PostgreSQL is reachable from this machine.
- `psycopg2-binary` is installed: `pip install psycopg2-binary`

## Usage

```bash
# Basic: probe from node 2 to node 1
python tools/probe/probe_link.py --from 2 --to 1

# Custom payload size (5 MB)
python tools/probe/probe_link.py --from 3 --to 1 --bytes 5000000

# Non-default DB port (e.g. when local postgres conflicts with docker)
python tools/probe/probe_link.py --from 2 --to 1 --db-port 15432

# Insert request without waiting for result
python tools/probe/probe_link.py --from 2 --to 1 --no-wait
```

## Limitations

- Probing only works when the **sender** node initiated the TCP connection to the
  receiver (i.e. the sender received an `AddNode` from the controller). In the
  simple 3-node example, node 1 is always the listener, so probes **to** node 1
  work (e.g. `--from 2 --to 1`) but probes **from** node 1 do not.
- If the probe's final packet is lost, the `active_probes` entry on the receiver
  is never cleaned up (minor memory leak over many failed probes).

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--from` | (required) | Sender node ID |
| `--to` | (required) | Receiver node ID |
| `--bytes` | 1360000 | Total probe payload (~1000 packets at 1360 B each) |
| `--timeout` | 30 | Seconds to wait for result |
| `--no-wait` | false | Insert request and exit immediately |
| `--db-host` | localhost | PostgreSQL host |
| `--db-port` | 5432 | PostgreSQL port |
| `--db-name` | nextmini | Database name |
| `--db-user` | pgusr | Database user |
| `--db-pass` | pgpwrd | Database password |
