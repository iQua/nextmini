# Multicast Docker Example (Python API + lossless sessions)

This example boots a complete Nextmini stack (Postgres, controller, and dataplane nodes) in Docker and exercises multicast end-to-end via the Python API:

- `create_group` / `join_group`
- `set_group_routes` + route installation
- large lossless transfer using `send_data` / `receive_data` + `lossless_wait`

The Python driver lives in `examples/multicast-docker/scripts/multicast_node.py`.

## Run

```bash
cd examples/multicast-docker
docker compose up --build
```

By default, each container builds `nextmini_py` in-place (via `maturin develop`) and then runs the Python driver.

## Faster startup (optional)

If you already have a local wheel build, you can prebuild it once and let Docker reuse it:

```bash
cd python-api
maturin build --release -m python-api/Cargo.toml
cd ../examples/multicast-docker
docker compose up --build
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

Set environment overrides via `docker compose run -e ...` or your shell:

- `GROUP_LABEL`: label passed to `create_group`
- `RECEIVER_IDS`: comma-separated receiver node IDs (default `2,3`)
- `CHUNK_SIZE`: lossless chunk size (default 8500)
- `RECEIVE_TIMEOUT_MS`: receiver wait timeout

For full details, read `examples/multicast-docker/scripts/run_multicast_node.sh` and `examples/multicast-docker/scripts/multicast_node.py`.

