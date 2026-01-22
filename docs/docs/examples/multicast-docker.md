# Multicast Docker Example (Python API + lossless sessions)

This example boots a complete Nextmini stack (Postgres, controller, and dataplane nodes) in Docker and exercises multicast end-to-end via the Python API:

- `create_group` / `join_group`
- `set_group_routes` + route installation
- large lossless transfer using `send_data` / `receive_data` + `lossless_wait`

The Python driver lives in `examples/multicast-docker/scripts/multicast_node.py`.

## How this differs from `examples/rl`

- This is a focused smoke test for multicast group lifecycle + a lossless transfer (source + receivers).
- `examples/rl` is an application-style trainer/worker workload with heavier dependencies (Transformers/datasets) and a longer-running loop.
- If you are debugging multicast plumbing, start here; if you are validating the RL training workflow, use `examples/rl`.

## Run

```bash
cd examples/multicast-docker
docker compose up --build
```

By default, each container builds `nextmini_py` in-place (via `maturin develop`) and then runs the Python driver.

Note: startup order matters — receivers must connect to the controller before the source calls `create_group` (otherwise you may see `Unable to build route key for flow`). The driver waits for topology readiness and for multicast routes before starting `receive_data`.

## Faster startup (optional)

If you already have a Linux wheel build for `nextmini_py` (under `target/wheels/` in the repo), you can skip rebuilding
the extension module inside each container.

From the repo root (on a Linux host / environment):

```bash
maturin build --release -m python-api/Cargo.toml -F python-extension
```

Then run the stack with `SKIP_BUILD=1` so containers install the wheel instead of calling `maturin develop`:

```bash
cd examples/multicast-docker
SKIP_BUILD=1 docker compose up --build
```

## Inspecting the run

- Logs:

  ```bash
  docker compose logs -f controller
  docker compose logs -f source receiver_a receiver_b
  ```

- DB state:

  ```bash
  docker compose exec postgres psql -U pgusr -d nextmini -c "select * from groups;"
  docker compose exec postgres psql -U pgusr -d nextmini -c "select * from group_members;"
  ```

- Output artifacts: `examples/multicast-docker/artifacts/`

## Common knobs

Environment variables (set via `docker compose run -e ...` or your shell):

- `GROUP_LABEL`: label passed to `create_group`
- `RECEIVER_IDS`: comma-separated receiver node IDs (default `2,3`)
- `RECEIVE_TIMEOUT_MS`: receiver wait timeout

CLI flags (passed to `multicast_node.py`):

- `--chunk-size`: lossless chunk size (default 8500)

For full details, read `examples/multicast-docker/scripts/run_multicast_node.sh` and `examples/multicast-docker/scripts/multicast_node.py`.
