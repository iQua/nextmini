# SmolTCP flows (WRR)

`examples/smoltcp-test` runs **controller-managed user-space flows** using the SmolTCP TCP engine. No application traffic is required: the dataplane generates and consumes the flows internally.

This example is useful for validating:

- `[[flows]]` installation from `controller-config.toml`
- Weighted scheduling (`scheduler_type = "wrr"` + `flow_weight`)
- Link shaping via `[[link_rates]]`

See also: [User-space flows](../design/user-space-flows.md).

## Run

```bash
cd examples/smoltcp-test
docker compose up --build
```

## Inspect flows

```bash
cd examples/smoltcp-test
docker compose exec postgres psql -U pgusr -d nextmini -c "\
  SELECT id, src_node_id, dst_node_id, flow_weight, is_finished, start_time, finish_time \
  FROM flows ORDER BY id;"
```

## Tune the scenario

Edit `examples/smoltcp-test/controller-config.toml`:

- `scheduler_type = "wrr"` vs `"fifo"`
- `flow_spec.flow_weight` to change relative shares
- `[[link_rates]]` to change the per-link token bucket rate

## Cleanup

```bash
cd examples/smoltcp-test
docker compose down -v
```

