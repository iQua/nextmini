# Example: Curl Command

To run this example which only uses the single machine,

enter the follwowing commands:
```bash
cd examples/curl

docker compose build; docker compose up
```

Then wait for 30s to see the output of the curl command.

### Logs

```text
controller       | 2025-07-21T22:08:06.163972Z  INFO controller: Sending AddNodeAddress messages to 3 nodes.
external_server  | Launching simple HTTP server on port 8080...
external_client  | Starting simple curl client via SOCKS5 proxy 172.16.8.5:8081 -> http://172.16.8.8:8080
external_client  | [22:08:30] Sending request...
node2            | 2025-07-21T22:08:30.905961Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.4:47108.
node2            | 2025-07-21T22:08:30.906191Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.4:47108.
node3            | 2025-07-21T22:08:30.906360Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.5:40094.
node4            | 2025-07-21T22:08:30.906718Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.6:49448.
node2            | 2025-07-21T22:08:30.906440Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:8081 with TCP max.
node3            | 2025-07-21T22:08:30.906488Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.5:40094.
node4            | 2025-07-21T22:08:30.906886Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:49448.
node3            | 2025-07-21T22:08:30.906651Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.7:8081 with TCP max.
node4            | 2025-07-21T22:08:30.906973Z  INFO nextmini::node::connector: No remote address found for node id: 5, redirecting to external server: 172.16.8.8:8080
node4            | 2025-07-21T22:08:30.907241Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.8:8080 without max header.
external_server  | 172.16.8.7 - - [21/Jul/2025 22:08:30] "GET / HTTP/1.1" 200 -
node2            | 2025-07-21T22:08:30.909542Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369094082803602189975552 to 172.16.8.6:8081 (upstream: 79 bytes, downstream: 342 bytes).
node3            | 2025-07-21T22:08:30.909610Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369094082803602189975552 to 172.16.8.7:8081 (upstream: 79 bytes, downstream: 342 bytes).
node4            | 2025-07-21T22:08:30.909755Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369094082803602189975552 to 172.16.8.8:8080 (upstream: 79 bytes, downstream: 342 bytes).
```
