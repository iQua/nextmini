#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any


SOURCE_NODE_ID = 1
IMPORTED_TOPOLOGY: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate config and payload files for the namespace lossless example."
    )
    parser.add_argument("--case-name", required=True)
    parser.add_argument("--mode", choices=("plain", "fec"), required=True)
    parser.add_argument(
        "--fec-scheme",
        choices=("raptorq", "mettle"),
        default="raptorq",
        help="FEC backend for generated FEC manifests.",
    )
    parser.add_argument("--receivers", type=int, required=True)
    parser.add_argument("--trees", type=int, required=True)
    parser.add_argument(
        "--solution-json",
        type=pathlib.Path,
        help="Use the tree family and receiver set from a solver solution.json.",
    )
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--symbols-per-block", type=int, required=True)
    parser.add_argument("--mettle-coded-rate-num", type=int, default=1)
    parser.add_argument("--mettle-coded-rate-den", type=int, default=1)
    parser.add_argument("--payload-size", type=int, required=True)
    parser.add_argument(
        "--synthetic-payload",
        action="store_true",
        help="Generate deterministic payload bytes inside the dataplane instead of writing payload.bin.",
    )
    parser.add_argument("--packet-processors", type=int, default=1)
    parser.add_argument("--channel-capacity", type=int, default=2048)
    parser.add_argument("--queue-capacity", type=int, default=2048)
    parser.add_argument(
        "--receive-timeout-ms",
        type=int,
        default=120_000,
        help="Transfer completion timeout for source/receiver sessions.",
    )
    parser.add_argument(
        "--peer-report-timeout-ms",
        type=int,
        default=15_000,
        help="Sender timeout for receiver feedback after SourceDone.",
    )
    parser.add_argument(
        "--controller-addr",
        default="ws://127.0.0.1:3000",
        help="Host-local controller address for dataplane startup.",
    )
    parser.add_argument(
        "--db-host",
        default="127.0.0.1",
        help="Postgres host as seen by the host-local controller.",
    )
    parser.add_argument("--out-dir", type=pathlib.Path, required=True)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.receivers <= 0:
        raise SystemExit("--receivers must be positive.")
    if args.trees <= 0:
        raise SystemExit("--trees must be positive.")
    if args.block_size <= 0:
        raise SystemExit("--block-size must be positive.")
    if args.symbols_per_block <= 0:
        raise SystemExit("--symbols-per-block must be positive.")
    if args.mettle_coded_rate_den <= 0:
        raise SystemExit("--mettle-coded-rate-den must be positive.")
    if args.mettle_coded_rate_num < args.mettle_coded_rate_den:
        raise SystemExit("--mettle-coded-rate-num must be >= --mettle-coded-rate-den.")
    if args.payload_size <= 0:
        raise SystemExit("--payload-size must be positive.")
    if args.packet_processors <= 0:
        raise SystemExit("--packet-processors must be positive.")
    if args.channel_capacity <= 0:
        raise SystemExit("--channel-capacity must be positive.")
    if args.queue_capacity <= 0:
        raise SystemExit("--queue-capacity must be positive.")
    if args.receive_timeout_ms <= 0:
        raise SystemExit("--receive-timeout-ms must be positive.")
    if args.peer_report_timeout_ms <= 0:
        raise SystemExit("--peer-report-timeout-ms must be positive.")
    if args.mode == "plain" and args.trees != 1:
        raise SystemExit("plain mode currently supports exactly one tree in this harness.")
    if args.solution_json is not None:
        if args.mode != "fec":
            raise SystemExit("--solution-json is only supported for FEC runs.")
        if not args.solution_json.exists():
            raise SystemExit(f"--solution-json not found: {args.solution_json}")


def load_imported_topology(path: pathlib.Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    scenario = data.get("scenario") or {}
    source_id = int(scenario.get("source", SOURCE_NODE_ID))
    if source_id != SOURCE_NODE_ID:
        raise SystemExit(
            f"namespace harness expects solver source {SOURCE_NODE_ID}, got {source_id}"
        )

    receivers = [int(node_id) for node_id in scenario.get("receivers", [])]
    if not receivers:
        raise SystemExit(f"solution has no scenario.receivers: {path}")

    raw_trees = data.get("trees") or []
    trees: list[tuple[int, list[tuple[int, int]]]] = []
    for raw_tree in raw_trees:
        tree_id = int(raw_tree["tree_id"])
        edges = [(int(src), int(dst)) for src, dst in raw_tree["edges"]]
        trees.append((tree_id, edges))

    if not trees:
        raise SystemExit(f"solution has no trees: {path}")

    scenario_edges = [
        (int(edge["src"]), int(edge["dst"])) for edge in scenario.get("edges", [])
    ]
    if not scenario_edges:
        scenario_edges = sorted({edge for _, edges in trees for edge in edges})

    ordered_nodes = [int(node_id) for node_id in data.get("ordered_nodes", [])]
    if not ordered_nodes:
        endpoints = {SOURCE_NODE_ID, *receivers}
        endpoints.update(src for src, _ in scenario_edges)
        endpoints.update(dst for _, dst in scenario_edges)
        ordered_nodes = sorted(endpoints)

    return {
        "receivers": receivers,
        "trees": trees,
        "topology_edges": sorted(set(scenario_edges)),
        "n_nodes": max(ordered_nodes),
    }

def relay_pairs(tree_count: int) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    next_node_id = SOURCE_NODE_ID + 1

    for _ in range(tree_count):
        pairs.append((next_node_id, next_node_id + 1))
        next_node_id += 2

    return pairs

def receiver_ids(tree_count: int, receiver_count: int) -> list[int]:
    if IMPORTED_TOPOLOGY is not None:
        return list(IMPORTED_TOPOLOGY["receivers"])
    start = SOURCE_NODE_ID + (tree_count * 2) + 1
    return list(range(start, start + receiver_count))

def topology_edges(tree_count: int, receiver_count: int) -> list[tuple[int, int]]:
    if IMPORTED_TOPOLOGY is not None:
        return list(IMPORTED_TOPOLOGY["topology_edges"])
    receivers = receiver_ids(tree_count, receiver_count)
    edges: list[tuple[int, int]] = []

    for relay_a_id, relay_b_id in relay_pairs(tree_count):
        edges.append((SOURCE_NODE_ID, relay_a_id))
        edges.append((relay_a_id, relay_b_id))
        edges.extend((relay_b_id, receiver_id) for receiver_id in receivers)

    return edges


def tree_edges(tree_count: int, receiver_count: int) -> list[tuple[int, list[tuple[int, int]]]]:
    if IMPORTED_TOPOLOGY is not None:
        return list(IMPORTED_TOPOLOGY["trees"])
    receivers = receiver_ids(tree_count, receiver_count)
    trees: list[tuple[int, list[tuple[int, int]]]] = []

    for tree_id, (relay_a_id, relay_b_id) in enumerate(relay_pairs(tree_count)):
        edges = [(SOURCE_NODE_ID, relay_a_id), (relay_a_id, relay_b_id)] + [
            (relay_b_id, receiver_id) for receiver_id in receivers
        ]
        trees.append((tree_id, edges))

    return trees


def total_nodes(tree_count: int, receiver_count: int) -> int:
    if IMPORTED_TOPOLOGY is not None:
        return int(IMPORTED_TOPOLOGY["n_nodes"])
    return 1 + (tree_count * 2) + receiver_count


def render_controller_config(args: argparse.Namespace) -> str:
    edges = ",\n    ".join(
        f"[{src}, {dst}]" for src, dst in topology_edges(args.trees, args.receivers)
    )
    return f"""# Generated by examples/ns-lossless/generate.py
protocol = "tcp"

[routing]
protocol = "shortest_path"

[topology]
n_nodes = {total_nodes(args.trees, args.receivers)}
edges = [
    {edges}
]

[db]
user = "pgusr"
password = "pgpwrd"
host = "{args.db_host}"
database = "nextmini"
port = "5432"
"""


def render_tree_blocks(args: argparse.Namespace) -> str:
    blocks: list[str] = []
    for tree_id, edges in tree_edges(args.trees, args.receivers):
        edge_lines = ", ".join(f"[{src}, {dst}]" for src, dst in edges)
        blocks.append(
            "\n".join(
                [
                    "[[integration_test.trees]]",
                    f"tree_id = {tree_id}",
                    f"edges = [{edge_lines}]",
                ]
            )
        )
    return "\n\n".join(blocks)


def render_dataplane_config(args: argparse.Namespace, payload_path: pathlib.Path, artifact_dir: pathlib.Path) -> str:
    receivers = ", ".join(
        str(node_id) for node_id in receiver_ids(args.trees, args.receivers)
    )
    tree_ids = ", ".join(str(tree_id) for tree_id, _ in tree_edges(args.trees, args.receivers))
    fec_enabled = "true" if args.mode == "fec" else "false"
    fec_scheme = args.fec_scheme if args.mode == "fec" else "raptorq"

    return f"""# Generated by examples/ns-lossless/generate.py
private_network_interface = "eth0"
private_network_name = "net1"
n_nodes = {total_nodes(args.trees, args.receivers)}
num_tun_queues = 1
num_packet_processors = {args.packet_processors}
channel_capacity = {args.channel_capacity}
channel_backpressure = true
queue_capacity = {args.queue_capacity}
feature = "sequential"
enable_local_interface = false
controller_addr = "{args.controller_addr}"
metrics_collection_interval = 3600
controller_connect_timeout_ms = 30000
interval_between_spawn = 1
child_start_delay_ms = 0
auto_add_forward_rules = true

[lossless_runtime_config]
default_block_size = {args.block_size}
ready_grace_ms = 3000
peer_report_timeout_ms = {args.peer_report_timeout_ms}
fec_enabled = {fec_enabled}
fec_default_symbols_per_block = {args.symbols_per_block}
fec_default_scheme = "{fec_scheme}"
fec_default_tree_ids = [{tree_ids}]
mettle_default_coded_rate_num = {args.mettle_coded_rate_num}
mettle_default_coded_rate_den = {args.mettle_coded_rate_den}

[integration_test]
enabled = true
case_name = "{args.case_name}"
group_label = "ns-it-{args.case_name}"
source_node_id = {SOURCE_NODE_ID}
receiver_ids = [{receivers}]
artifact_dir = "{artifact_dir}"
payload_path = "{payload_path}"
synthetic_payload = {str(args.synthetic_payload).lower()}
payload_size = {args.payload_size}
group_timeout_ms = 30000
receive_timeout_ms = {args.receive_timeout_ms}
poll_interval_ms = 200
src_port = 45000
dst_port = 46000
block_size = {args.block_size}

{render_tree_blocks(args)}
"""


def write_payload(path: pathlib.Path, size: int) -> None:
    payload = bytes((index * 31 + 7) % 256 for index in range(size))
    path.write_bytes(payload)


def main() -> None:
    global IMPORTED_TOPOLOGY

    args = parse_args()
    validate_args(args)
    if args.solution_json is not None:
        IMPORTED_TOPOLOGY = load_imported_topology(args.solution_json)

    out_dir = args.out_dir.resolve()
    artifact_dir = out_dir / "artifacts"
    payload_path = out_dir / "payload.bin"
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    if not args.synthetic_payload:
        write_payload(payload_path, args.payload_size)
    (out_dir / "controller-config.toml").write_text(
        render_controller_config(args), encoding="utf-8"
    )
    (out_dir / "dataplane-config.toml").write_text(
        render_dataplane_config(args, payload_path, artifact_dir), encoding="utf-8"
    )

    print(f"Wrote {out_dir / 'controller-config.toml'}")
    print(f"Wrote {out_dir / 'dataplane-config.toml'}")
    if args.synthetic_payload:
        print(f"Using synthetic payload size {args.payload_size}")
    else:
        print(f"Wrote {payload_path}")


if __name__ == "__main__":
    main()
