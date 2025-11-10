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

## Python API fragmentation knobs

Large buffers injected via the Python bindings are controlled through a small set of `python_fragmentation_*` fields on `LocalConfig` (or their CLI equivalents):

```toml
python_fragmentation_enabled = true
python_fragmentation_max_message_bytes = 65536
python_fragmentation_reassembly_window_bytes = 262144
python_fragmentation_fragment_timeout_ms = 1000
python_fragmentation_trace_flow_events = true
```

* `python_fragmentation_enabled` gates the PyPayloadSeg header/fragmentation feature. When `false`, the legacy single-packet behavior stays in effect.
* `python_fragmentation_max_message_bytes` caps the logical size of any Python buffer; keep it at or below `mtu - 64` to minimize fragment counts.
* `python_fragmentation_reassembly_window_bytes` and `python_fragmentation_fragment_timeout_ms` bound the per-flow memory/time spent waiting for fragments on the receiving side.
* `python_fragmentation_trace_flow_events` toggles structured tracing + controller events whenever fragments are dropped or time out. When enabled, the dataplane also emits periodic `PythonFragmentMetrics` snapshots so the controller can log aggregate counters even if tracing is muted.

When `python_fragmentation_trace_flow_events` is true the dataplane also emits `DataplaneToController::PythonFragmentEvents`
messages. Each entry includes the `flow_id`, optional `message_id`, a `detail` string, and a `kind` value (`invalid_header`,
`assembler_drop`, `window_overflow`, or `timeout`). These surface in the controller logs so operators can alert on repeated
fragmentation failures per node.

Set these values directly in node configs (or pass `--python-fragmentation-*` overrides) before enabling the feature flag in production. Controller configs can mirror the same fields if you need central visibility.
