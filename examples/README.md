# Run the examples

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

For routes, firstly, you can choose the topology, where we define two types: `full_mesh` and `ring`. Then, if you have multiple routes between two nodes, the system will choose one of them randomly to achieve load balancing.

For link rates, you can also specify the link rates between two nodes, which will be used to determine the maximum bandwidth available for that link.

For flows, you only need to specify the source and destination node IDs and the flow specific parameters like `flow_len`, `flow_rate`, and `flow_weight`.
