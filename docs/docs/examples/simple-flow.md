# Lossless flows (controller-managed)

`examples/simple-flow` runs **controller-managed user-space flows** using the lossless session engine (`flow_transport = "lossless_unicast"`). Each flow delivers an exact byte count, optionally paced by `flow_rate`.

To run the SmolTCP (user-space TCP) variant using the same scenario directory, see: [SmolTCP flows](smoltcp-flows.md).

This example is useful for validating:

- lossless sessions driven by controller `[[flows]]`
- flow completion bookkeeping in Postgres
- weighted scheduling (`scheduler_type = "wrr"` + `flow_weight`)

See also: [Lossless flows (design)](../design/lossless-flows.md).

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

- `flow_transport = "lossless_unicast"`
- `scheduler_type = "fifo"` vs `"wrr"` (weighted scheduling)
- `flow_spec.flow_len` and `flow_spec.flow_rate`
- `flow_spec.flow_weight` (only used when `scheduler_type = "wrr"`)
- `[[routes]]` and `[[link_rates]]` to change paths and shaping

## Cleanup

```bash
cd examples/simple-flow
docker compose down -v
```

