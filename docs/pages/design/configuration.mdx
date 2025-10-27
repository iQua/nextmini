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
