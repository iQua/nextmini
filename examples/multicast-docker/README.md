# Multicast Docker Example

This example boots a full Nextmini stack (Postgres, controller, and three dataplane
nodes) inside Docker to exercise the multicast APIs end-to-end. Each dataplane node
runs in-process via `nextmini_py` and drives the control-plane by issuing
`CreateGroup`/`JoinGroup` messages before sending or receiving payloads.

`nextmini_py` now exposes helpers that stream controller events into Python:

- `group_is_ready()` – wait for the `GroupCreated` ack after issuing `CreateGroup`.

The source container calls `group_is_ready()` immediately after `CreateGroup`
to learn the assigned `group_id`/`group_ip` without querying Postgres, while
receivers use the membership/route helpers to know when the dataplane is ready
to deliver multicast packets locally.

## Layout

- `controller-config.toml` – controller configuration with a three-node full-mesh and
  a dedicated multicast pool.
- `configs/` – per-node dataplane configs for the source (node ID 1) and two receivers
  (node IDs 2 and 3).
- `scripts/multicast_node.py` – Python driver that wraps `nextmini_py` to create/join
  multicast groups, stream payloads to a multicast IP, and assert delivery.
- `scripts/run_multicast_node.sh` – helper that builds the `nextmini_py` wheel inside
  the container (via `maturin develop`) and launches the Python driver.
- `docker-compose.yml` – wires the services together on a dedicated bridge network.
- `artifacts/group-info.json` – shared metadata surface with the controller-assigned
  group id/IP so receivers can learn the multicast address out-of-band. Lossless session
  IDs are now adopted automatically once manifests arrive, so no additional coordination
  is required.

## Running the Example

### Option 1: Pre-build the Python wheel (recommended for faster startup)

Note: This has only been tested on arbutus. Needs to change the name and version for compatibility with other platforms.

```bash
cd python-api
# Build the wheel with the python-extension feature enabled
maturin build --release -F python-extension
cd ../examples/multicast-docker
docker compose build
docker compose up
```

The pre-built wheel will be shared across all containers, significantly speeding up startup.

### Default: Build inside containers (recommended for portability)

```bash
cd examples/multicast-docker
docker compose up
```

By default `SKIP_BUILD=0`, so each container builds the `nextmini_py` extension in-place using `maturin develop -F python-extension`. This works across machines (including macOS hosts) without prebuilding wheels.
The first run will take a few minutes as the wheel is compiled.

Every run now streams a tensor end-to-end: the source synthesizes (or loads) a tensor, installs a simple multicast DAG via `set_group_routes`, waits for both receivers to register their receive sessions (signaled through `artifacts/receiver-ready-<node_id>.json`), multicasts the data chunk-by-chunk, and shuts down once the reconstructed outputs land in `artifacts/`. Lossless session IDs are negotiated inside the dataplane, and the receivers register via `receive_data` before blocking on `lossless_wait`, then drain the payload via `get_data_buffer`.

Key environment overrides (set via `docker compose run -e ...` or exported before
`docker compose up`):

- `GROUP_LABEL` – label used when creating the multicast group (default `demo-multicast`).
- `RECEIVER_IDS` – comma-separated receiver node IDs the source waits for (default `2,3`).
- `SOURCE_NODE_ID` – source node ID used by receivers when registering.
- `GROUP_TIMEOUT`, `RECEIVE_TIMEOUT_MS` – tweak the various waits when running on slower machines or remote builders.
- `TENSOR_PATH` – optional path (inside the repo) to a tensor/binary blob that should be
  multicast chunk-by-chunk. When omitted, the source auto-generates a ≈1 GB tensor at
  `/workspace/tensors/tensor-auto-1g.pt` before every run.
- `EXPECTED_BYTES` – total byte count for the tensor; defaults to the auto-generated file
  size when `TENSOR_PATH` is not provided.
- `CHUNK_SIZE` – payload slice size (defaults to 8500 bytes; lossless senders automatically clamp this to fit the dataplane MTU so you never have to tune fragmentation manually).
- `FEC` – set to `on` to enable FEC-backed lossless sessions (default `off`).
- `FEC_SYMBOLS_PER_BLOCK` – optional sender override for FEC `symbols_per_block` when `FEC=on`.
- `FEC_SYMBOL_SIZE` – optional sender override for FEC `symbol_size` when `FEC=on`.
- `SINK_PATH_A` / `SINK_PATH_B` – optional override for where each receiver writes the
  reconstructed tensor under `/artifacts`.
- `ARTIFACT_DIR` – shared volume for tensors and metadata.
- `CLEAN_SHARED_DIRS` – when `1` (default) the source container empties the shared
  `artifacts/` directory before each run and, if no custom `--tensor-path` is provided,
  also clears the tensor staging directory. Set to `0` to keep prior outputs.
- `TENSOR_STAGE_DIR` – override for the tensor staging directory used during cleanup
  (defaults to `/workspace/tensors`).

## Streaming Large Tensors

By default the source container auto-generates a ≈1 GB PyTorch tensor
(`/workspace/tensors/tensor-auto-1g.pt`) before it starts the dataplane, records its size
in `/artifacts/tensor-metadata.json`, and then streams it chunk-by-chunk to the multicast
group. All you need is:

```bash
cd examples/multicast-docker
docker compose build
docker compose up
```

Each run downloads/install PyTorch (via `run_multicast_node.sh`), synthesizes the tensor,
and then pushes it using `CHUNK_SIZE` (defaults to 8500 bytes; payloads larger than the MTU are automatically fragmented by the dataplane). Receivers automatically
load the metadata, reconstruct the
stream under `/artifacts/receiver-<node_id>.bin`, and verify the byte counts. Adjust the
following knobs if needed:

- Provide a custom tensor via `TENSOR_PATH` (and optionally `EXPECTED_BYTES`) to skip
  auto-generation.
- Increase/decrease `CHUNK_SIZE` to tune throughput.
- Inspect `/artifacts/tensor-metadata.json` for the last tensor path/size broadcast to
  receivers.

The repository already tracks empty `artifacts/` and `tensors/` directories, so you can
run `docker compose up` without creating them manually. The source container will clear
their contents automatically before each run (unless `CLEAN_SHARED_DIRS=0`), so you get
a fresh slate without manual cleanup. Delete the directories entirely only if you want to
recreate the bind mounts from scratch.

## Testing & Verification

1. Pre-build the wheel via `cd python-api && maturin build --release`, or set `SKIP_BUILD=0`
   to compile in-container.
2. From `examples/multicast-docker`, run `docker compose up` (the tracked `artifacts/`
   and `tensors/` directories will be reused automatically and cleaned before each run).
3. Tail the `source` and receiver logs (`docker compose logs -f source receiver_a receiver_b`)
   to confirm group readiness events and chunk counters.
4. After the run, inspect `artifacts/tensor-metadata.json` and `artifacts/receiver-*.bin`
   to validate byte counts.

## Inspecting the Run

- Controller logs: `docker compose logs -f controller`
- Source logs: `docker compose logs -f source`
- Receiver logs: `docker compose logs -f receiver_a`, `receiver_b`
- DB state: `docker compose exec postgres psql -U pgusr -d nextmini -c "select * from groups;"`,
  `select * from group_members;`

Each receiver prints the number of bytes received per multicast payload; the run fails
early if a timeout occurs while waiting for the group, membership propagation, or actual
traffic delivery.
