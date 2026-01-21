# SmolTCP flows (controller-managed)

This page uses the `examples/simple-flow` scenario, but runs it with SmolTCP user-space TCP flows by setting `flow_transport = "tcp"`.

## Run

1) Edit `examples/simple-flow/controller-config.toml`:

- Set `flow_transport = "tcp"`
- Define your `[[flows]]` (use `flow_len`, optional `flow_rate`, and optional `flow_weight`)

2) Start the scenario:

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

## See also

- Design: [SmolTCP flows](../design/smoltcp-flows.md)
- Lossless variant of the same scenario: [Lossless flows](simple-flow.md)
