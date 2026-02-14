---
title: "Transport Configuration"
description: "Describes transport protocol selection and related inter-node communication knobs."
---

The dataplane supports multiple transport protocols for inter-node communication. The default protocol is **TCP**.

### Runtime Configuration

All tuning happens through the standard dataplane configuration (`config.toml` or CLI flags):

#### Configuration file

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

#### CLI Flags

Use `--protocol quic --quic-congestion-control cubic` on the CLI to switch to QUIC with CUBIC congestion control, and use `--protocol udp` to run dataplane links over UDP.

### Operational Notes

The server keeps QUIC keep-alives enabled so idle tunnels remain established during quiet periods. MTU behavior is controlled by `LocalConfig.mtu`, so reduce that value when paths require tighter framing. On connection failures, endpoints retry automatically up to 10 times before aborting; if a node does not join, inspect dataplane logs for the remote address and failure reason.

The Python bindings always send a single TCP frame per `send_to_node` call; the OS networking stack handles any link-layer segmentation automatically, and the dataplane clamps lossless chunk sizes to respect the configured MTU. That keeps configuration simple — set `mtu` once, and both the Rust dataplane and the Python bindings inherit the same envelope.
