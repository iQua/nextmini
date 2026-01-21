# Lossless flows (controller-managed)

`examples/simple-flow` runs **controller-managed flows** using the lossless session engine (`flow_transport = "lossless_unicast"`). Each flow delivers an exact byte count, optionally paced by `flow_rate`.

This example is useful for validating:

- lossless sessions driven by controller `[[flows]]`
- flow completion bookkeeping in Postgres
- pacing behavior (token buckets) for fixed-size transfers

See also: [User-space flows](../design/user-space-flows.md) and [Lossless Session Configuration](../design/lossless_config.md).

## Run

```bash
cd examples/simple-flow
docker compose up --build
```

## Inspect flows

```bash
cd examples/simple-flow
docker compose exec postgres psql -U pgusr -d nextmini -c "\
  SELECT id, src_node_id, dst_node_id, is_finished, start_time, finish_time \
  FROM flows ORDER BY id;"
```

## Tune the scenario

Edit `examples/simple-flow/controller-config.toml`:

- `flow_transport = "lossless_unicast"` (controller default)
- `flow_spec.flow_len` and `flow_spec.flow_rate`
- `[[routes]]` and `[[link_rates]]` to change paths and shaping

## Cleanup

```bash
cd examples/simple-flow
docker compose down -v
```

