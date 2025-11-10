# Python Fragmentation Smoke Test

Use this runbook when validating the PyPayloadSeg pipeline end to end. It exercises the Python bindings, dataplane sender/downstream assembler, and the new controller telemetry without requiring the full docker-compose harness.

## Prerequisites

- `nextmini_py` installed in the active virtualenv (`maturin develop --release -m python-api/Cargo.toml`).
- Node config with fragmentation enabled:
  ```toml
  python_fragmentation_enabled = true
  python_fragmentation_max_message_bytes = 65536
  python_fragmentation_reassembly_window_bytes = 262144
  python_fragmentation_fragment_timeout_ms = 1000
  python_fragmentation_trace_flow_events = true
  ```
- Destination node running the dataplane with the config above (can be the same host if you start two processes).
- **CPython 3.13 dev headers**. The smoke script imports `nextmini_py`, which links against libpython; without the header/package the build step fails before the test runs.

## Running the script

1. Start the dataplane process with the config described above.
2. In another shell, run:
   ```bash
   docs/testing/scripts/python_fragmentation_smoke.py \
     --config /abs/path/node-config.toml \
     --dst-node-id 2 \
     --recv-from-node 1 \
     --payload-bytes 2097152
   ```
   * `--recv-from-node` is optional. When set, the script registers a payload-only receiver so it can verify the reconstructed buffer length.
   * Adjust `--payload-bytes` to stress larger messages; the default is 2 MiB.

## Expected output

You should see:
- `[*] Booting dataplane…` followed by `[+] send_to_node completed…` within a second.
- When `--recv-from-node` is used, `[+] Successfully received … bytes.`
- Controller logs containing `PythonFragmentEvents` only when you intentionally provoke errors (e.g., drop fragments or exceed the window). In a clean run, there should be **no** events emitted.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Script errors with `nextmini_py is not installed` | `maturin develop`/`pip install` never ran in the current virtualenv. | Activate the env and reinstall the wheel. |
| `ld: symbol(s) not found for architecture arm64` during `cargo test -p nextmini_py` | Host is missing CPython dev headers (macOS: `brew install python@3.13`; Linux: `apt install python3.13-dev`). | Install the headers and rebuild `nextmini_py`. |
| Script times out waiting for payload | Destination dataplane not running, wrong `dst-node-id`, or fragmentation disabled (sender rejects oversized payloads). | Ensure both nodes are up and sharing the same config, then retry. |
| Controller logs repeated `window_overflow` events | Python sender exceeds the configured `max_message_bytes` or the receiver can’t drain in time. | Lower the payload size or raise `reassembly_window_bytes`. |

## Next steps

- Record the output of a successful run (send/receive logs + controller telemetry) in the ticket or PR that introduces fragmentation.
- Once CI hosts ship the CPython 3.13 dev toolchain we can wire this script into an automated `uv run docs/testing/scripts/python_fragmentation_smoke.py …` target.
