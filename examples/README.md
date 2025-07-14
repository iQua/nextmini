# Running the Examples

Nextmini has multiple introductory examples, which can be run by the following commands:

```bash
cd examples/<folder-name>; docker compose up --build
```

where `<folder-name`> can be either one of the following:

```text
simple
simple-flow
simple-routes
simple-scheduler
smoltcp-test
simple-max
splice-test
```

# Configuration Files

## Dataplane Configuration

`config.toml` is the configuration file for dataplane.

## Feature Configuration

There are two features that can be enabled: `sequential` and `concurrent`.

- `sequential`: The sequential feature guarantees that no packets are reordered throughout the entire path, by processing packets consistently using one of the packet processors.

- `concurrent`: The concurrent feature allows packets to be processed in parallel by multiple packet processors, therefore packets may be reordered. They are put back in order before being sent to the TUN interface. This feature is useful for high throughput applications, when a flow can be split into multiple paths over the network.

## Controller Configuration

`controller-config.toml` is the configuration file for controller, where you can specify the flows, routes, link rates, and other parameters like scheduler type and protocol used to connect between nodes.

To define routes, you can first choose the topology, where we define two types: `full_mesh` and `ring`, which include default routes. You can also define your own routes explicitly. If you define multiple routes between two nodes, the system will choose one of them randomly to achieve load balancing.

For link rates, you can also specify the link rates between two nodes, which will be used to determine the maximum bandwidth available for that link.

Note that the rate and bucket size are specified in bits per second and bytes, respectively. You need to adjust these values accordingly for setup.

```toml
[[link_rates]]
src_node_id = 1
dst_node_id = 2
rate = 1000_000_000
bucket_size = 3000

[[link_rates]]
src_node_id = 2
dst_node_id = 1
rate = 100_000_000
bucket_size = 1800
```


For flows, you only need to specify the source and destination node IDs and the flow specific parameters like `flow_len`, `flow_rate`, and `flow_weight`.

# Example of test:

## splice-test

To run the splice-test example, you can use the following command:

```bash
cd examples/splice-test; docker compose build; docker compose up
```

You can see the config:

```toml
protocol = "tcp"

[topology]
n_nodes = 3

[[routes]]
# node 0 represents the external client
# node 4 represents the internal server
route = [0, 1, 2, 3, 4]
```
and the docker-compose yaml file:

```yaml
  external_client:
    container_name: external_client
    hostname: external_client
    image: nextmini_external_client
    build:
      context: ./
      dockerfile: ./src/Dockerfile.client
    networks:
      network:
        ipv4_address: 172.16.8.4
    stdin_open: true
    privileged: true
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep 5 && /var/nextmini/src"

  node1:
    container_name: node1
    hostname: node1
    image: nextmini_datapath
    build:
      context: ../../
      dockerfile: ./dataplane/Dockerfile
    networks:
      network:
        ipv4_address: 172.16.8.5
    stdin_open: true
    privileged: true
    # environment:
    #     - RUST_LOG=debug
    volumes:
      - ./config.toml:/var/nextmini/config.toml
      - ../../tools/:/var/nextmini/tools
    depends_on:
      - controller
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep 7 && /var/nextmini/nextmini ws://controller:3000"

  node2:
    container_name: node2
    hostname: node2
    image: nextmini_datapath
    build:
      context: ../../
      dockerfile: ./dataplane/Dockerfile
    networks:
      network:
        ipv4_address: 172.16.8.6
    stdin_open: true
    privileged: true
    # environment:
    #     - RUST_LOG=debug
    volumes:
      - ./config.toml:/var/nextmini/config.toml
      - ../../tools/:/var/nextmini/tools
    depends_on:
      - controller
      - node1
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep 8 && /var/nextmini/nextmini ws://controller:3000"

  node3:
    container_name: node3
    hostname: node3
    image: nextmini_datapath
    build:
      context: ../../
      dockerfile: ./dataplane/Dockerfile
    networks:
      network:
        ipv4_address: 172.16.8.7
    stdin_open: true
    privileged: true
    # environment:
    #     - RUST_LOG=debug
    volumes:
      - ./config.toml:/var/nextmini/config.toml
      - ../../tools/:/var/nextmini/tools
    depends_on:
      - controller
      - node2
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep 9 && /var/nextmini/nextmini ws://controller:3000"

  external_server:
    container_name: external_server
    hostname: external_server
    image: nextmini_external_server
    build:
      context: ./
      dockerfile: ./src/Dockerfile.server
    networks:
      network:
        ipv4_address: 172.16.8.8
    stdin_open: true
    privileged: true
    ports:
      - "8080:8080"
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep 10 && /var/nextmini/src"
```

As you can see the `external_client` and `external_server` are the external client and server, which will connect to the internal nodes. The internal nodes are `node1`, `node2`, and `node3`, which will run the Nextmini datapath.

The real IP address of `external_client` is set to be `172.16.8.4`, as the default `external_base_addr` now is `172.16.8.4`, therefore, the `node_id` for `external_client` would be `0`.

```rust
  // binds the external client/server address to the node ID
  subnet if subnet == (external_base & netmask) => (ip_addr - external_base) as NodeId,
```

For convenience, the IP addr of `external_base_addr` is set to `172.16.8.8` as the last node in the route, which is `node_id = 4` in the example.

Simply put, these two IP addresses(the IP addr of external client and server) can be configured arbitrarily when ·external_base_addr· is properly set up (that is, when the prefix is the same), as long as they don't conflict with the `node_id` that the controller allocates to the nextmini dataplane node.
