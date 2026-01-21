# Running Nextmini in Docker Containers

## A Simple Example Running `iperf3`

To run Nextmini on a single machine over multiple containers, you will need a Docker runtime for running Docker. On macOS, [OrbStack](https://orbstack.dev/) is strongly recommended as it offers a simpler and more performant setup than Docker Desktop.

Once the Docker runtime is up and running, use the following commands in the `examples/simple` directory:

```bash
docker compose build && docker compose up
```

This will automatically start three Nextmini dataplane nodes and the Nextmini controller as a server. The three Nextmini dataplane nodes are connected with each other in a *full mesh* topology, running the shortest path protocol. These dataplane nodes will connect to the Nextmini controller via the docker network, and connect to one another as required by the *full mesh* topology.

In case you run into the following error:

```bash
[+] Running 1/1
 ✘ Network simple_network  Error                                                             0.0s
failed to create network simple_network: Error response from daemon: invalid pool request: Pool overlaps with other one on this address space
```

Run the following command to remove all stopped containers, all networks not used by at least one container, all images without at least one container associated to them, as well as all build cache. This will provide you with a clean slate before you run `docker compose build && docker compose up` again:

```bash
docker system prune -a
```

We can attach to `node1` container with the following command, in a different terminal:

```bash
docker exec -it node1 /bin/bash
```

Nextmini leverages TUN/TAP interfaces to facilitate internode communication. To check the available Nextmini interfaces on node1, simply type:

```bash
ifconfig
```

This will display all available network interfaces, including those created by Nextmini, shown as `utun`:

```
utun: flags=4305<UP,POINTOPOINT,RUNNING,NOARP,MULTICAST>  mtu 1400
        inet 10.0.0.1  netmask 255.255.0.0  destination 10.0.0.1
        inet6 fe80::9173:2d8a:c3bb:4649  prefixlen 64  scopeid 0x20<link>
        unspec 00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00  txqueuelen 500  (UNSPEC)
        RX packets 0  bytes 0 (0.0 B)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 4  bytes 192 (192.0 B)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
```

We can see the local TUN interface, and it is assigned an IP address of `10.0.0.1`. By default, Nextmini uses the subnet `10.0.0.0/16` for the TUN/overlay network. Node IPs are derived from `node_id` (for example, node 1 is `10.0.0.1`, node 2 is `10.0.0.2`).

To confirm that connection is successful, we can ping `node2` and `node3` by their Nextmini IP addresses:

```bash
ping 10.0.0.2
ping 10.0.0.3
```

if successful, outputs similar to the following should be displayed

```bash
node1:/var/nextmini# ping 10.0.0.2
PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
64 bytes from 10.0.0.2: icmp_seq=1 ttl=64 time=12.4 ms
64 bytes from 10.0.0.2: icmp_seq=2 ttl=64 time=0.585 ms
```

The Nextmini docker image comes pre-installed with the `iperf3` tool for network bandwidth testing. We can test the bandwidth between `node1` and `node2` by starting an `iperf3` server on `node1` with

```bash
iperf3 -s
```

Open a new terminal and attach to `node2` with

```bash
docker exec -it node2 /bin/bash
```

On `node2`, connect an `iperf3` client to node1 with

```bash
iperf3 -c 10.0.0.1
```

Wait for `iperf3` to complete running.

To shutdown the Docker containers, use the command:

```
docker compose down
```

Alternatively, press `Control + C` in the terminal where `docker compose up` is running.
