## QUIC Transport Basics

The dataplane currently exposes a single QUIC transport mode that sends all TUN traffic over one reliable stream.

### Runtime Configuration

All tuning happens through the standard dataplane configuration (`config.toml` or CLI flags):

```
protocol = "quic"

# QUIC congestion control algorithm (bbr | cubic)
quic_congestion_control = "bbr"
```

Use `--quic-congestion-control cubic` on the CLI to switch away from the default BBR controller.

### Operational Notes

- The server keeps QUIC keep-alives enabled so idle tunnels stay established.
- MTU handling relies on the `mtu` field in `LocalConfig`; adjust it there if the path requires a smaller packet size.
- Endpoints log connection failures and retry automatically (up to 10 attempts) before aborting. Review the dataplane logs for the remote address and failure reason if a node cannot join.

## Python API payloads

The Python bindings always send a single TCP frame per `send_to_node` call. There are no `python_fragmentation_*` fields anymore; the OS networking stack handles any link-layer segmentation automatically, and the dataplane clamps reliable chunk sizes to respect the configured MTU. That keeps configuration simple—set `mtu` once, and both the Rust dataplane and the Python bindings inherit the same envelope.
