# Multicast Docker Example

This example boots a full Nextmini stack (Postgres, controller, and three dataplane
nodes) inside Docker to exercise the multicast APIs end-to-end. Each dataplane node
runs in-process via `nextmini_py` and drives the control-plane by issuing
`CreateGroup`/`JoinGroup` messages before sending or receiving payloads.

`nextmini_py` now exposes helpers that stream controller events into Python:

- `group_is_ready()` – wait for the `GroupCreated` ack after issuing `CreateGroup`.
- `wait_for_local_membership()` and `wait_for_routes_installed()` – receivers block
  until the controller installs their local delivery entries before opening sockets.

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

## Running the Example

### Option 1: Pre-build the Python wheel (recommended for faster startup)

Note: This has only been tested on arbutus. Needs to change the name and version for compatibility with other platforms.

```bash
cd python-api
maturin build --release
cd ../examples/multicast-docker
docker compose build
docker compose up
```

The pre-built wheel will be shared across all containers, significantly speeding up startup.

### Option 2: Build inside containers on any machines including MacOS.

```bash
cd examples/multicast-docker
SKIP_BUILD=0 docker compose up
```

This will build the `nextmini_py` extension inside each container using `maturin develop`.
The first run will take a few minutes as the wheel is compiled.

Once the source sees the required number of subscribers (defaults to 2) that are both
joined and ready, it finishes transmitting. Both receivers stream the configured payload
count and every container exits cleanly.

Key environment overrides (set via `docker compose run -e ...` or exported before
`docker compose up`):

- `GROUP_LABEL` – label used when creating the multicast group (default `demo-multicast`).
- `PAYLOAD_COUNT` / `PAYLOAD_SIZE` / `PAYLOAD_SLEEP_MS` – tune the source workload.
- `EXPECTED_SUBSCRIBERS` – number of receivers that must be both joined and ready before the source starts sending (defaults to 2).
- `RECEIVER_EXPECTED` – number of payloads each receiver waits for.
- `GROUP_TIMEOUT`, `MEMBER_TIMEOUT`, `RECEIVE_TIMEOUT_MS` – tweak the various waits when
  running on slower machines or remote builders.
- `TENSOR_PATH` – optional path (inside the repo) to a tensor/binary blob that should be
  multicast chunk-by-chunk. When set, also provide `EXPECTED_BYTES` (total byte count) and
  optionally adjust `CHUNK_SIZE` (defaults to 6144 bytes).
- `VERIFY_CHECKSUM` – set to `1` to have the source emit, and receivers verify, a
  SHA-256 checksum stored at `CHECKSUM_PATH` (defaults to `/artifacts/<group>.sha256`).
- `SINK_PATH_A` / `SINK_PATH_B` – optional override for where each receiver writes the
  reconstructed tensor under `/artifacts`.

## Streaming Large Tensors

To push the multicast flow with a 1 GB tensor:

1. Generate the tensor once (PyTorch example shown below) and keep it under `tensors/`.
2. Create `examples/multicast-docker/artifacts/` so Docker can mount it at `/artifacts`.
3. Export `TENSOR_PATH`, `EXPECTED_BYTES`, and (optionally) `VERIFY_CHECKSUM=1` before
   `docker compose up`.

```bash
# From the repo root
python - <<'PY'
import pathlib, torch
torch.manual_seed(42)
tensor = torch.randn(256, 1024, 1024, dtype=torch.float32).contiguous().cpu()
path = pathlib.Path("tensors") / "tensor-1g.pt"
path.parent.mkdir(exist_ok=True)
torch.save(tensor, path)
PY

cd examples/multicast-docker
export TENSOR_PATH=/workspace/tensors/tensor-1g.pt
export EXPECTED_BYTES=$(python - <<'PY'
import os
print(os.path.getsize("../tensors/tensor-1g.pt"))
PY
)
export VERIFY_CHECKSUM=1
export CHECKSUM_PATH=/artifacts/tensor-1g.sha256
docker compose up
```

The source streams the tensor in `CHUNK_SIZE`-sized slices, writes/updates the checksum
file under `/artifacts`, and each receiver rebuilds the stream under
`/artifacts/receiver-<node_id>.bin` while verifying the digest. Tune `PAYLOAD_SLEEP_MS`
and `CHUNK_SIZE` to trade throughput for CPU utilization.

## Inspecting the Run

- Controller logs: `docker compose logs -f controller`
- Source logs: `docker compose logs -f source`
- Receiver logs: `docker compose logs -f receiver_a`, `receiver_b`
- DB state: `docker compose exec postgres psql -U pgusr -d nextmini -c "select * from groups;"`,
  `select * from group_members;`

Each receiver prints the number of bytes received per multicast payload; the run fails
early if a timeout occurs while waiting for the group, membership propagation, or actual
traffic delivery.
