# Controller-managed flows (lossless or SmolTCP)

`examples/simple-flow` runs **controller-managed user-space flows**: the dataplane generates and consumes the flows internally (no application traffic required).

You choose the transport in `controller-config.toml`:

- `flow_transport = "tcp"` → SmolTCP user-space TCP flows
- `flow_transport = "lossless_unicast"` → lossless sessions (deliver exactly `flow_len` bytes)

This example is useful for validating:

- controller-driven `[[flows]]` execution (SmolTCP TCP or lossless)
- flow completion bookkeeping in Postgres
- weighted scheduling (`scheduler_type = "wrr"` + `flow_weight`)

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
  SELECT id, src_node_id, dst_node_id, flow_weight, is_finished, start_time, finish_time \
  FROM flows ORDER BY id;"
```

## Tune the scenario

Edit `examples/simple-flow/controller-config.toml`:

- `flow_transport = "tcp"` (SmolTCP) vs `"lossless_unicast"` (lossless)
- `scheduler_type = "fifo"` vs `"wrr"` (weighted scheduling)
- `flow_spec.flow_len` and `flow_spec.flow_rate`
- `flow_spec.flow_weight` (only used when `scheduler_type = "wrr"`)
- `[[routes]]` and `[[link_rates]]` to change paths and shaping

## Cleanup

```bash
cd examples/simple-flow
docker compose down -v
```

