# Transport Configuration

This page covers transport protocol settings. For a complete list of all configuration options, see the [Configuration Reference](config-reference.md).

## Transport Protocol Configuration

The dataplane supports multiple transport protocols for inter-node communication. The default protocol is **TCP**.

### Runtime Configuration

All tuning happens through the standard dataplane configuration (`config.toml` or CLI flags):

```toml
# Transport protocol: tcp (default) | udp | quic
protocol = "tcp"

# When using QUIC, configure congestion control (bbr | cubic)
# quic_congestion_control = "bbr"
```

To use QUIC instead of TCP:

```toml
protocol = "quic"
quic_congestion_control = "bbr"
```

Use `--protocol quic --quic-congestion-control cubic` on the CLI to switch to QUIC with CUBIC congestion control.

### Operational Notes

- In controller-managed deployments, `protocol` is treated as **controller-owned**: the controller's startup config
  overwrites the local `protocol` setting after a node connects. Local overrides are most useful for standalone/dev
  runs before the controller handshake.
- The server keeps QUIC keep-alives enabled so idle tunnels stay established.
- The `mtu` field in the dataplane `LocalConfig` controls the user-space interface (SmolTCP/TUN) MTU and related packet sizing.
- Endpoints log connection failures and retry automatically (up to 10 attempts) before aborting. Review the dataplane logs for the remote address and failure reason if a node cannot join.

## Python API payloads

The Python bindings always send a single TCP payload per `send_to_node` call. There are no `python_fragmentation_*` fields anymore; TCP segmentation is handled by the OS.

Lossless transfers (`send_data` / `receive_data`) chunk the buffer using the lossless runtime config (`[lossless_runtime_config].default_chunk_size`). Chunking is an application-level setting and is not automatically derived from `mtu`.
