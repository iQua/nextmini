Test record:

The following log shows a client successfully downloading a file from an external server through the Nextmini network.

```text
external_server  | Launching simple HTTP server on port 8080 with file.txt available...
external_client  | Starting wget client via proxychains SOCKS5 172.16.8.5:8081 -> http://172.16.8.8:8080/file.txt
external_client  | [02:52:32] Downloading http://172.16.8.8:8080/file.txt to /downloads/file.txt
external_client  | [proxychains] config file found: /etc/proxychains.conf
external_client  | [proxychains] preloading /usr/lib/libproxychains4.so
external_client  | [proxychains] DLL init: proxychains-ng 4.17
node2            | 2025-07-25T02:52:32.120307Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.4:50798.
node2            | 2025-07-25T02:52:32.120772Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.4:50798.
node2            | 2025-07-25T02:52:32.121542Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:8081 with TCP max.
node3            | 2025-07-25T02:52:32.121333Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.5:44586.
node3            | 2025-07-25T02:52:32.121986Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.5:44586.
node3            | 2025-07-25T02:52:32.122668Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.7:8081 with TCP max.
node4            | 2025-07-25T02:52:32.122830Z  INFO nextmini::node::network::tcp_max: Connection accepted from 172.16.8.6:44528.
node4            | 2025-07-25T02:52:32.123526Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.6:44528.
node4            | 2025-07-25T02:52:32.123852Z  INFO nextmini::node::connector: No remote address found for node id: 5, redirecting to external server: 172.16.8.8:8080
node4            | 2025-07-25T02:52:32.124553Z  INFO nextmini::node::network::tcp_max: Connected to 172.16.8.8:8080 without max header.
external_server  | 172.16.8.7 - - [25/Jul/2025 02:52:32] "GET /file.txt HTTP/1.1" 200 -
node3            | 2025-07-25T02:52:32.129896Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369095121446266252296192 to 172.16.8.7:8081 (upstream: 138 bytes, downstream: 207 bytes).
node2            | 2025-07-25T02:52:32.129595Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369095121446266252296192 to 172.16.8.6:8081 (upstream: 138 bytes, downstream: 207 bytes).
node4            | 2025-07-25T02:52:32.130266Z  INFO nextmini::node::connector: Spliced connection for flow 228710454654182369095121446266252296192 to 172.16.8.8:8080 (upstream: 138 bytes, downstream: 207 bytes).
external_client  | [02:52:32] Download completed. File content: Hello from Nextmini!
```
