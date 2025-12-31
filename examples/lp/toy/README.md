# Toy Demo

Minimal 3-node deployment demonstrating the LP multicast pipeline:

1. Probe link capacities
2. Solve LP for optimal multicast tree
3. Install routes via `set_group_routes()`
4. Send lossless multicast from node 1 → nodes 2, 3

## Run

```bash
cd examples/lp/toy
docker compose up --build
```

## Expected Output

- Source: `edges=[(1, 2), (1, 3)]` or similar
- Receivers: `payload=b'hello-nextmini'`
