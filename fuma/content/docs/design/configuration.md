# Transport Configuration

This page covers transport protocol settings. For a complete list of all configuration options, see the [Configuration Reference](config-reference).

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

Use `--protocol udp` to run dataplane links over UDP.

### Operational Notes

- The server keeps QUIC keep-alives enabled so idle tunnels stay established.
- MTU handling relies on the `mtu` field in `LocalConfig`; adjust it there if the path requires a smaller packet size.
- Endpoints log connection failures and retry automatically (up to 10 attempts) before aborting. Review the dataplane logs for the remote address and failure reason if a node cannot join.

## Python API payloads

The Python bindings always send a single TCP frame per `send_to_node` call. There are no `python_fragmentation_*` fields anymore; the OS networking stack handles any link-layer segmentation automatically, and the dataplane clamps lossless chunk sizes to respect the configured MTU. That keeps configuration simple—set `mtu` once, and both the Rust dataplane and the Python bindings inherit the same envelope.
