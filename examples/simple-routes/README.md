# Defining routes between a source and a destination: a simple example

To build and run this example:

```bash
cd examples/simple-routes
docker compose build
docker compose up
```

To run an `iperf3` server in `node4`:

```bash
docker exec -it node4 /bin/bash
```

```bash
node4:/var/nextmini# iperf3 -s
```

And then similarly, run the `iperf3` client in `node1` to connect to `node4`, with its Nextmini IP address `10.0.0.4`:

```bash
docker exec -it node1 /bin/bash
```

```bash
node1:/var/nextmini# iperf3 -c 10.0.0.4
```

To stop the Docker containers:

```bash
docker compose down
```

## Defining the routes with two alternative formats

- Sequence (single path):

  ```toml
  [[routes]]
  route = [1, 2, 3, 4]
  ```

- Directed Acyclic Graph (multiple branches for one source → one destination):

  ```toml
  [[routes]]
  route = [[1, 2], [2, 4], [1, 3], [3, 4]]
  ```

The controller infers `src_node_id` as the node with outgoing but no incoming edges, and `dst_node_id` as the node with incoming but no outgoing edges.

Example `controller-config.toml`:

```toml
protocol = "tcp"

[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }

[[routes]]
route = [[1, 2], [2, 4], [1, 3], [3, 4]]

[[routes]]
route = [[4, 1]]
```
