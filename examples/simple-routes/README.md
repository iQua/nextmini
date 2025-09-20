# Simple Routes Example

Quick start:

```bash
cd examples/simple-routes
docker compose build
docker compose up
```

To stop the stack:

```bash
docker compose down
```


See more details about source-selected routing in [ROUTING.md](./ROUTING.md).

## Route configuration formats

- Sequence (single path):
  ```toml
  [[routes]]
  route = [1, 2, 3, 4]
  ```
- DAG (multiple branches for one src→dst):
  ```toml
  [[routes]]
  route = [[1, 2], [2, 4], [1, 3], [3, 4]]
  ```
  The controller infers `src_node_id` as the node with outgoing but no incoming edges, and `dst_node_id` as the node with incoming but no outgoing edges.

Example snippet from this folder’s `controller-config.toml`:
```toml
protocol = "tcp"

[[routes]]
route = [[1, 2], [2, 4], [1, 3], [3, 4]]

[[routes]]
route = [[1, 4]]

[[routes]]
route = [[4, 1]]
```



