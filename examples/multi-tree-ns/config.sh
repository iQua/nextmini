usage() {
  cat <<'EOF'
Usage:
  ./examples/multi-tree-ns/run.sh \
    --mode plain|fec \
    --source 1 \
    --receivers 4,5 \
    --tree 1-2,2-4,2-5 \
    [--tree 1-3,3-4,3-5] \
    --file /abs/path/to/input.bin
EOF
}

require_cmd() {
  if [[ "$1" == */* ]]; then
    [[ -x "$1" ]] || {
      echo "Missing required executable: $1" >&2
      exit 1
    }
    return 0
  fi
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

pick_free_port() {
  "$gen_python" - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

build_controller_if_needed() {
  [[ -x "$controller_bin" ]] && return 0
  if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1090
    . "$HOME/.cargo/env"
  fi
  require_cmd cargo
  (cd "$root_dir" && CARGO_TARGET_DIR="$controller_target_dir" cargo build -p controller --release)
  [[ -x "$controller_bin" ]] || {
    echo "Controller binary not found after build: $controller_bin" >&2
    exit 1
  }
}

prepare_work_dir() {
  rm -rf "$work_dir"
  mkdir -p "$work_dir/logs"
}

generate_runtime_files() {
  "$gen_python" - "$work_dir" "$mode" "$source_node" "$receivers_csv" "$input_file" "$bridge_ip" "$controller_port" "$postgres_port" "${tree_specs[@]}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

work_dir = Path(sys.argv[1])
mode = sys.argv[2]
source = int(sys.argv[3])
receiver_ids = sorted({int(part) for part in sys.argv[4].split(",") if part.strip()})
input_file = str(Path(sys.argv[5]).resolve())
bridge_ip = sys.argv[6]
controller_port = int(sys.argv[7])
postgres_port = int(sys.argv[8])
tree_specs = sys.argv[9:]


def parse_tree(spec: str) -> list[list[int]]:
    edges = []
    for part in spec.split(","):
        if not part:
            continue
        src, dst = part.split("-", 1)
        edges.append([int(src), int(dst)])
    if not edges:
        raise SystemExit(f"empty tree spec: {spec!r}")
    return edges


trees = [parse_tree(spec) for spec in tree_specs]
if mode == "plain" and len(trees) != 1:
    raise SystemExit("plain mode requires exactly one --tree")
if mode == "fec" and len(trees) < 2:
    raise SystemExit("fec mode requires at least two --tree values")

all_ids = {source, *receiver_ids}
tree_nodes: set[int] = set()
undirected_topology: set[tuple[int, int]] = set()
for tree in trees:
    for a, b in tree:
        if a <= 0 or b <= 0:
            raise SystemExit("node ids must be positive")
        all_ids.add(a)
        all_ids.add(b)
        tree_nodes.add(a)
        tree_nodes.add(b)
        undirected_topology.add(tuple(sorted((a, b))))

n_nodes = max(all_ids)
tree_ids = list(range(len(trees)))
relay_ids = sorted(tree_nodes - {source} - set(receiver_ids))

roles = {}
for node_id in range(1, n_nodes + 1):
    if node_id == source:
        roles[str(node_id)] = "source"
    elif node_id in receiver_ids:
        roles[str(node_id)] = "receiver"
    elif node_id in relay_ids:
        roles[str(node_id)] = "relay"
    else:
        roles[str(node_id)] = "idle"

scenario = {
    "mode": mode,
    "receiver_ids": receiver_ids,
    "input_file": input_file,
    "n_nodes": n_nodes,
    "trees": [{"tree_id": tree_id, "edges": tree} for tree_id, tree in enumerate(trees)],
    "roles": roles,
}
(work_dir / "scenario.json").write_text(json.dumps(scenario))

controller_config = [
    f"port = {controller_port}",
    'protocol = "tcp"',
    "",
    "[topology]",
    f"n_nodes = {n_nodes}",
    f"edges = {json.dumps([list(edge) for edge in sorted(undirected_topology)])}",
    "",
    "[routing]",
    'protocol = "shortest_path"',
    "",
    "[db]",
    'host = "127.0.0.1"',
    f'port = "{postgres_port}"',
    'user = "pgusr"',
    'password = "pgpwrd"',
    'database = "nextmini"',
    "",
]
(work_dir / "config.toml").write_text("\n".join(controller_config))

tree_ids_toml = ", ".join(str(tree_id) for tree_id in tree_ids) if mode == "fec" else "0"
fec_enabled = "true" if mode == "fec" else "false"
for node_id in range(1, n_nodes + 1):
    node_cfg = f'''controller_addr = "ws://{bridge_ip}:{controller_port}"
private_network_interface = "eth0"
public_network_interface = "eth0"
private_network_name = "multi_tree_ns"
node_id = {node_id}
num_tun_queues = 1
num_packet_processors = 1
channel_capacity = 3000
queue_capacity = 4000
feature = "sequential"
channel_backpressure = true
controller_connect_timeout_ms = 30000

[lossless_runtime_config]
default_block_size = 8500
fec_enabled = {fec_enabled}
fec_default_tree_ids = [{tree_ids_toml}]
fec_default_symbols_per_block = 32
'''
    (work_dir / f"node-{node_id}.toml").write_text(node_cfg)
PY
}

load_n_nodes() {
  n_nodes="$("$gen_python" -c 'import json, sys; print(json.load(open(sys.argv[1]))["n_nodes"])' "$work_dir/scenario.json")"
}
