# Running the Examples

Nextmini includes several introductory examples. To run an example, navigate to its folder and use Docker Compose:

```bash
cd examples/<folder-name>;
docker compose up --build
```

Replace `<folder-name>` with one of the following:

- simple
- simple-flow
- simple-routes
- simple-scheduler
- smoltcp-test
- simple-max
- splice-test

# Configuration Files

## Dataplane Configuration

The `config.toml` file configures the dataplane node.

## Feature Configuration

You can enable one of two features: `sequential` or `concurrent`.

- `sequential`: Ensures no packet reordering by processing packets using a single processor throughout the path.

- `concurrent`: Allows parallel processing by multiple processors, which may reorder packets. Packets are reordered before delivery to the TUN interface. This is ideal for high-throughput scenarios where flows can be split across multiple paths.

## Controller Configuration

The `controller-config.toml` file configures the controller. Here, you can specify flows, routes, link rates, scheduler type, protocol, and more.

For routes, select a topology like `full_mesh` or `ring` for default routes, or define custom routes. Multiple routes between nodes enable random load balancing.

For link rates, define maximum bandwidth between nodes (in bits per second) and bucket size (in bytes). Example:

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

### Run the splice-test example

To run the splice-test example, you can use the following command:

```bash
cd examples/splice-test; docker compose build; docker compose up
```

### Logs

After about 15 seconds, logs from the external client and server will show throughput and total data sent/received. Example output (test runs indefinitely without a set duration):

```text
external_client  | Send Throughput: 73.21 Gbps, Total sent: 717.91 GB.
external_server  | Throughput: 70.14 Gbps, Total received: 726.66 GB.
external_client  | Send Throughput: 70.22 Gbps, Total sent: 726.68 GB.
external_server  | Throughput: 78.01 Gbps, Total received: 736.42 GB.
external_client  | Send Throughput: 78.02 Gbps, Total sent: 736.44 GB.
external_server  | Throughput: 72.68 Gbps, Total received: 745.50 GB.
external_client  | Send Throughput: 72.53 Gbps, Total sent: 745.50 GB.
external_server  | Throughput: 73.45 Gbps, Total received: 754.68 GB.
```

### What is in the config?

The config file with comments removed looks like this:

```toml
protocol = "tcp"

[topology]
n_nodes = 3

[[routes]]
# node 1 represents the external client
# node 5 represents the internal server
route = [1, 2, 3, 4, 5]
```

Without the commented lines, the config will look like this:

```toml
protocol = "tcp"

# Three nodes in the topology
[[nodes]]
[topology]
n_nodes = 3

# if you wish to test ping and iperf, you can uncomment the following lines
[[nodes]]
node_id = 2
operating_mode = "max"

[[nodes]]
node_id = 4
operating_mode = "max"

[[routes]]
# node 1 represents the external client
# node 5 represents the internal server
route = [1, 2, 3, 4, 5]

# if you wish to test ping and iperf, you can uncomment the following lines
[[routes]]
route = [2, 3, 4]

[[routes]]
route = [4, 3, 2]
```

The additional routes support testing ping and iperf between node 2` and `node 4`.

If without commented lines, firstly use the `docker exec -it node4 /bin/bash`.

Then `iperf3 -c 10.0.0.2`.

Below is the output of the iperf command from `node 4` to `node 2`:

```text
Connecting to host 10.0.0.2, port 5201
[  5] local 10.0.0.4 port 48696 connected to 10.0.0.2 port 5201
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec   400 MBytes  3.35 Gbits/sec    0   3.69 MBytes
[  5]   1.00-2.00   sec   410 MBytes  3.44 Gbits/sec    0   3.69 MBytes
... (truncated)
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-10.00  sec  3.93 GBytes  3.38 Gbits/sec    0    sender
[  5]   0.00-10.01  sec  3.93 GBytes  3.37 Gbits/sec         receiver
```

To test ping from node 2 to node 4:

Enter node2 container: `docker exec -it node2 /bin/bash`

Then run: `ping 10.0.0.4`

Example output:

```text
PING 10.0.0.4 (10.0.0.4) 56(84) bytes of data.
64 bytes from 10.0.0.4: icmp_seq=1 ttl=64 time=1.30 ms
64 bytes from 10.0.0.4: icmp_seq=2 ttl=64 time=1.60 ms
```

### What is in the docker-compose file?

The docker-compose.yaml defines services like `external_client` and `external_server` for external endpoints, connected via SOCKS5. Internal Nextmini dataplane nodes (e.g., `node2`, `node3`, `node4`) run the datapath.

Example IP assignments:
- `external_client`: 172.16.8.4
- `node2`: 172.16.8.5
- `node3`: 172.16.8.6
- `node4`: 172.16.8.7
- `external_server`: 172.16.8.8

The external client/server IPs can be customized, but ensure they match the prefix of `external_base_addr` (default: 172.16.8.3) for proper node_id computation. In this setup, external_client gets node_id=1 (172.16.8.4 - 172.16.8.3 = 1), and external_server gets node_id=5 (172.16.8.8 - 172.16.8.3 = 5).

```rust
  // binds the external client/server address to the node ID
  subnet if subnet == (external_base & netmask) => (ip_addr - external_base) as NodeId,
```

The above code is used to bind the external client/server address to a node ID. Notice that this node ID can't conflict with the node IDs that the controller 
allocates to the Nextmini dataplane nodes, which are `2`, `3`, and `4` in this example. Thus the real IP address of `external_client/server` should not 
conflict with the IP addresses of the internal nodes.

This binds external addresses to node IDs without conflicting with internal nodes (e.g., 2, 3, 4). External IPs must share the same prefix as `external_base_addr` and avoid conflicts with controller-assigned IDs requested from Nextmini dataplane node.
