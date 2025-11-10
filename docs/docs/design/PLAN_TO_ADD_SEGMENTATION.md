# Python payload segmentation roll-out (status)

This page previously hosted the working plan for adding fragmentation to the Python dataplane API. The feature has
shipped; use this short status note to orient yourself and follow the links to the canonical design docs.

## Current status

- ✅ `nextmini_py` automatically fragments `FrozenBuffer` payloads when `python_fragmentation_enabled = true`.
- ✅ `PythonInterfaceHandle` reassembles fragments for payload-only receivers, exports per-flow telemetry, and notifies
  the controller about drops/timeouts when tracing is enabled.
- ✅ Controller + docs expose the `python_fragmentation_*` knobs so operators can tune budgets and alerting.
- ☐ CI automation still depends on hosts with CPython 3.13 dev headers before we can wire the smoke test into a regular
  job. Until then, run the script manually when touching fragmentation code.

## Roll-out checklist

1. Set the `python_fragmentation_*` fields in the node config (or equivalent CLI flags). The defaults documented in
   [`python_payload_fragmentation.md`](python_payload_fragmentation.md) work for most workloads.
2. Ensure both the controller and dataplane binaries are rebuilt from the current branch so they understand the new
   messages.
3. Install or develop the latest `nextmini_py` wheel (`python-api/` crate) on each Python worker.
4. Update Python receivers to pass `payload_only=True` when registering flows so they receive `PayloadDelivery`
   metadata, including fragment counts and message ids.
5. Tail controller logs for `PythonFragmentEvents` during the first deployment. Adjust the `max_message_bytes` and
   `reassembly_window_bytes` knobs if you observe repeated `window_overflow` or `timeout` events.
6. Keep `docs/testing/scripts/python_fragmentation_smoke.py` handy as a regression test whenever dataplane or binding
   changes touch this area.

## Reference material

- [Python dataplane API](python-api.md) – end-user view of the bindings, including code examples and API signatures.
- [Python payload fragmentation](python_payload_fragmentation.md) – header layout, assembler design, telemetry.
- [PyTorch + Nextmini Python API quickstart](../examples/pytorch_python_api.md) – copy/paste snippets for training jobs.
- [Python fragmentation smoke test](../testing/python_fragmentation_smoke.md) – end-to-end validation guide.
