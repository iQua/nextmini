# Proxy Flows (MAX mode + SOCKS5)

Nextmini can forward traffic between **external endpoints** through an emulated topology by acting as a SOCKS5 proxy. This is built on the “MAX” transport: connection-on-demand TCP streams between dataplane nodes plus stream splicing at relay hops.

This page explains how proxy flows work, how they relate to `OperatingMode::Max`, and what configuration matters.

## What a proxy flow is

In a proxy deployment:

1. An **external client** connects to a Nextmini dataplane node’s MAX server port and speaks **SOCKS5**.
2. The client requests a TCP **CONNECT** to an **external server** (`ip:port`).
3. Nextmini routes that stream hop-by-hop through the topology and finally connects to the external server.
4. Each relay hop splices bytes between the inbound and outbound TCP streams (zero-copy on Linux).

The application traffic stays as TCP end-to-end; Nextmini does not terminate TLS or interpret application protocols.

## MAX transport building blocks

The MAX stack is implemented in the dataplane:

- `dataplane/src/node/network/tcp_max.rs`: `TcpMaxServer` and `TcpMaxClient`
- `dataplane/src/node/connector.rs`: per-flow connection-on-demand and stream splicing

`TcpMaxServer` distinguishes two protocols by the first byte:

- `0x05`: SOCKS5 request (typically from an external client)
- `0x06`: Nextmini “TCP MAX” header (used between dataplane nodes)

## How routing works for proxy flows

Proxy flows still use the controller-installed routing table. The key detail is **node ID mapping** for external endpoints:

- External IPs are mapped to node IDs using `external_base_addr` sent by the controller at startup.
- Example (Docker defaults): if `external_base_addr = 172.16.8.3` and the client is `172.16.8.4`, then `node_id = 1`.

This lets you include external endpoints in routes just like internal dataplane nodes (for example, `1 → 2 → 3 → 4 → 5` where 1 and 5 are external client/server).

## SOCKS5 support and limitations

The SOCKS5 implementation is intentionally minimal:

- **Supported**: IPv4 `CONNECT`, “no authentication”
- **Not supported**: domain-name targets (`ATYP=0x03`), IPv6 (`ATYP=0x04`), `BIND`, `UDP ASSOCIATE`, authentication methods

If you need domain-name resolution, resolve the target hostname on the client side and pass an IPv4 address to the proxy.

## MAX mode vs proxy flows

`OperatingMode::Max` affects how a dataplane node forwards **TUN-originated** traffic:

- `normal`: packets are forwarded through the regular packet pipeline
- `max`: outbound packets are forwarded through the MAX connector (connection-on-demand per flow)

Proxy flows always use the MAX server + connector path because they start as inbound MAX/SOCKS5 connections. You can still set nodes to `operating_mode = "max"` to maximize throughput for regular (TUN) application traffic in the same deployment.

## Examples

The following examples exercise proxy flows:

- [`examples/splice-test`](../examples/splice-test.md): high-throughput stream splicing through multiple hops
- [`examples/curl`](../examples/curl.md): curl through SOCKS5 to an HTTP server

## Troubleshooting

- Set `RUST_LOG=info` (or `debug`) and watch for:
  - `tcp_max: Connection accepted ...`
  - `connector: Spliced connection for flow ...`
- If you see redirects to the external server too early, confirm your controller routes include the intended internal hops.
- If external node IDs look wrong, confirm your external endpoint IPs share the same prefix as `external_base_addr` in the controller config.
