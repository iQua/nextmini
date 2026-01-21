# Toy Demo (LP multicast pipeline)

This is a minimal 3-node deployment demonstrating the multicast LP pipeline:

1. Probe link capacities (inserts `is_probe = true` flows into Postgres)
2. Solve an LP for an optimal multicast tree
3. Install routes via `set_group_routes()`
4. Send a lossless multicast from node 1 → nodes 2 and 3

## Run

```bash
cd examples/lp/toy
docker compose up --build
```

## Inspect probes (optional)

```bash
cd examples/lp/toy
docker compose exec postgres psql -U pgusr -d nextmini -c "\
  SELECT COUNT(*) AS probes, COUNT(*) FILTER (WHERE is_finished) AS finished \
  FROM flows WHERE is_probe = TRUE;"
```

## Expected output

- Source prints selected edges (e.g. `[(1, 2), (1, 3)]`)
- Receivers print a delivered payload (e.g. `b"hello-nextmini"`)
