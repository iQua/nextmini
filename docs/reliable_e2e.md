Reliable Multicast E2E (rich, opt-in)

Quick start
- Build/install Python bindings (CPython 3.13):
  - `pip install maturin`
  - `maturin develop --release -m python-api/Cargo.toml`
- Enable E2E tests: `export ENABLE_RELIABLE_E2E=1`
- Run examples tests: `uv venv && source .venv/bin/activate && uv pip install -r examples/reliable_multicast/pyproject.toml && pytest -q examples/reliable_multicast/tests`

Scenarios (dry-run until engines wired)
- Dry run: returns sender/receiver session IDs and renders Panels/Syntax of calls and config.
- Ack variants: `all`, `k:N`, `frac:P` — validates wrapper input handling.
- Validation errors: invalid ack policy, zero chunk size, missing file.

Live mode (after PR2/PR3)
- Flip to live by ensuring engines emit completion signals and stats.
- Assertions: bytes/chunks sent/received, resends within bounds, optional checksum equality.
- Extend with loss/pacing scenarios (tc netem or in-process drops); keep rich logging.

Deterministic waits
- If the wrapper exposes `Dataplane.reliable_wait(session_id, timeout_ms=None)`, tests can await completion deterministically.
- The harness attempts to call `reliable_wait` when available and logs results.

Notes
- Rich logging utilities in `examples/reliable_multicast/utils/logging.py`.
- Scenario runner in `examples/reliable_multicast/e2e_reliable_multicast.py` prints the exact wrapper calls for traceability.

Controller DB reset (dev vs prod)
- By default, the controller resets DB schema at startup (dev-friendly).
- To disable reset (e.g., persistent envs): `export CONTROLLER_RESET_DB=0` before launching controller.
