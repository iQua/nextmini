#!/usr/bin/env python3
"""Toy demo: convert toy LP paths -> multicast DAG -> install into Nextmini -> send data.

This avoids the full RL pipeline and focuses on:
1) `convert_to_multicast_trees()` extraction
2) `nextmini_py.Dataplane.set_group_routes()` end-to-end wiring
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import nextmini_py as nm

from examples.lp.tree_conversion import convert_to_multicast_trees, paths_to_edges
from examples.lp.solver import build_graph_from_controller_config
from examples.lp import mFlow


def load_toml(path: str) -> dict:
    try:
        import tomllib  # py3.11+
    except ImportError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]

    with open(path, "rb") as f:
        return tomllib.load(f)


def wait_for_file(path: Path, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for file: {path}")


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def pick_best_tree(
    tree_candidates: list[tuple[list[list[int]], float]],
    requested_dests: list[int],
) -> tuple[list[list[int]], float]:
    """Pick a tree that covers as many requested destinations as possible, then max throughput."""
    requested = set(requested_dests)

    def score(item: tuple[list[list[int]], float]) -> tuple[int, float]:
        paths, tput = item
        covered = {p[-1] for p in paths if p}
        return (len(covered & requested), tput)

    return max(tree_candidates, key=score)


def run_source(args: argparse.Namespace) -> None:
    shared = Path(args.shared_dir)
    group_info_path = shared / "group.json"

    dp = nm.Dataplane(args.config)
    src_node_id = dp.node_id

    receiver_ids = [int(x) for x in args.receiver_ids.split(",") if x.strip()]
    payload = args.payload.encode("utf-8")

    # Create group (owner is this node).
    dp.create_group(args.label)
    group_id, group_ip, _ = dp.group_is_ready(timeout_ms=30_000)
    print(f"[src] group created: id={group_id} ip={group_ip} src={src_node_id}", flush=True)

    write_json(
        group_info_path,
        {
            "group_id": group_id,
            "group_ip": group_ip,
            "src_node_id": src_node_id,
            "receiver_ids": receiver_ids,
            "expected_bytes": len(payload),
        },
    )

    # Wait for receivers to join (signaled via files).
    for rid in receiver_ids:
        wait_for_file(shared / f"joined_{rid}", timeout_s=600)
        print(f"[src] receiver {rid} joined", flush=True)

    # Give the controller a moment to persist membership rows before we override routes.
    time.sleep(0.5)

    # Compute a real mFlow LP solution, then convert → edges.
    graph = build_graph_from_controller_config(args.controller_config)
    variables, sol = mFlow.solve(graph, [src_node_id], {src_node_id: receiver_ids})

    sources, session_trees = convert_to_multicast_trees(variables, sol)
    idx = sources.index(src_node_id)
    best_paths, throughput = pick_best_tree(session_trees[idx], receiver_ids)
    edges = paths_to_edges(best_paths)

    print(f"[src] conversion solver=mflow throughput={throughput} edges={edges}", flush=True)

    # Install override DAG into controller.
    dp.set_group_routes(group_id, edges)
    time.sleep(0.3)  # give controller time to push InstallGroupRoutes

    # Send payload via reliable multicast.
    builder = nm.PacketBuilder(size=len(payload))
    builder.write(payload)
    view = builder.freeze()

    sid = dp.send_data(group_ip, receiver_ids, view, chunk_size=args.chunk_size)
    ok = dp.reliable_wait(sid, timeout_ms=60_000)
    print(f"[src] send done ok={ok} sid={sid}", flush=True)


def run_receiver(args: argparse.Namespace) -> None:
    shared = Path(args.shared_dir)
    group_info_path = shared / "group.json"

    dp = nm.Dataplane(args.config)
    node_id = dp.node_id

    wait_for_file(group_info_path, timeout_s=600)
    info = json.loads(group_info_path.read_text())
    group_id = int(info["group_id"])
    group_ip = str(info["group_ip"])
    src_node_id = int(info["src_node_id"])
    expected_bytes = int(info["expected_bytes"])

    dp.join_group(group_id)
    (shared / f"joined_{node_id}").write_text("1")
    print(f"[dst {node_id}] joined group {group_id} ({group_ip})", flush=True)

    # Wait until routes include local delivery (InstallGroupRoutes received).
    ready = dp.wait_for_local_membership(group_id, timeout_ms=60_000)
    print(f"[dst {node_id}] local membership ready={ready}", flush=True)

    sid = dp.receive_data(group_ip, src_node_id, expected_bytes=expected_bytes, chunk_size=args.chunk_size)
    ok = dp.reliable_wait(sid, timeout_ms=60_000)
    view = dp.get_data_buffer(sid)
    payload = bytes(view.read())
    print(f"[dst {node_id}] recv ok={ok} sid={sid} payload={payload!r}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["source", "receiver"], required=True)
    parser.add_argument("--config", required=True, help="Path to node config TOML")
    parser.add_argument("--shared-dir", default=os.environ.get("TOY_SHARED_DIR", "/shared"))
    parser.add_argument(
        "--controller-config",
        default=os.environ.get("TOY_CONTROLLER_CONFIG", "examples/lp/toy/controller-config.toml"),
        help="Topology source for --solver=mflow (controller-config.toml)",
    )
    parser.add_argument("--label", default="toy-lp-group")
    parser.add_argument("--receiver-ids", default="2,3")
    parser.add_argument("--payload", default="hello-nextmini")
    parser.add_argument("--chunk-size", type=int, default=8500)
    args = parser.parse_args()

    if args.role == "source":
        run_source(args)
    else:
        run_receiver(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


