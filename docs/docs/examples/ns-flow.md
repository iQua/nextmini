# ns-flow (namespace flow scaling)

`examples/ns-flow` runs a high-node-count scenario on a single machine using Linux namespaces. It is useful for stress-testing controller flow installation and “flow finished” bookkeeping.

## Quick start

```bash
sudo ./examples/ns-flow/run.sh
```

`run.sh` waits for the controller to seed routes and flows in Postgres before starting the dataplane.

## Regenerate configs (optional)

```bash
python3 examples/ns-flow/generate.py --n-nodes 136 --n-flows 180 --src 1 --dst 136
```

## Verify completion

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_finished) AS finished FROM flows;"
```

## Cleanup

```bash
sudo ./examples/ns-flow/cleanup.sh
```

