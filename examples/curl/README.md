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
node2            | 2025-07-21T23:16:07.824213Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.4:34510.
node2            | 2025-07-21T23:16:07.824377Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.4:34510.
node2            | 2025-07-21T23:16:07.824508Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:8081 with TCP max.
node3            | 2025-07-21T23:16:07.824525Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.5:55480.
node3            | 2025-07-21T23:16:07.824541Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.5:55480.
node3            | 2025-07-21T23:16:07.824770Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.7:8081 with TCP max.
node4            | 2025-07-21T23:16:07.824802Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.6:56694.
node4            | 2025-07-21T23:16:07.824823Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:56694.
node4            | 2025-07-21T23:16:07.824851Z  INFO nextmini::node::connector: No remote address found for node id: 5, redirecting to external server: 172.16.8.8:8080
node4            | 2025-07-21T23:16:07.824950Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.8:8080 without max header.
external_server  | 172.16.8.7 - - [21/Jul/2025 23:16:07] "GET / HTTP/1.1" 200 -
external_client  | <!DOCTYPE HTML>
external_client  | <html lang="en">
external_client  | <head>
external_client  | <meta charset="utf-8">
external_client  | <title>Directory listing for /</title>
external_client  | </head>
external_client  | <body>
external_client  | <h1>Directory listing for /</h1>
external_client  | <hr>
external_client  | <ul>
node3            | 2025-07-21T23:16:07.827195Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369090536781845589131264 to 172.16.8.7:8081 (upstream: 79 bytes, downstream: 342 bytes).
node4            | 2025-07-21T23:16:07.827333Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369090536781845589131264 to 172.16.8.8:8080 (upstream: 79 bytes, downstream: 342 bytes).
node2            | 2025-07-21T23:16:07.827144Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369090536781845589131264 to 172.16.8.6:8081 (upstream: 79 bytes, downstream: 342 bytes).
external_client  | </ul>
external_client  | <hr>
external_client  | </body>
external_client  | </html>
100   187  100   187    0     0  65134      0 --:--:-- --:--:-- --:--:-- 93500
external_client  |
external_client  |
external_client  | --- Performance Metrics ---
external_client  | HTTP Code: 200
external_client  | Total Time: 0.002871s
external_client  | DNS Lookup: 0.000028s
external_client  | TCP Connect: 0.000663s
external_client  | TLS Handshake: 0.000000s
external_client  | Pre-transfer: 0.000961s
external_client  | Redirect: 0.000000s
external_client  | Start Transfer: 0.002801s
external_client  | Download Speed: 65134 bytes/sec
external_client  | Upload Speed: 0 bytes/sec
external_client  | Content Length: 187 bytes
external_client  | Request Size: 79 bytes
external_client  | --- End Metrics ---
external_client  |
```
