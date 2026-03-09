#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import nextmini_py as nm

TIMEOUT_S = 120


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["source", "receiver", "relay", "idle"], required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def wait_for_file(path: Path, timeout_s: int = TIMEOUT_S) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {path}")


def wait_for_ready(work_dir: Path, receiver_ids: list[int], timeout_s: int = TIMEOUT_S) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        missing = [rid for rid in receiver_ids if not (work_dir / f"ready-{rid}.json").exists()]
        if not missing:
            return
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for ready files: {missing}")


def role_hold() -> None:
    while True:
        time.sleep(60)


def run_receiver(dp: nm.Dataplane, work_dir: Path) -> int:
    wait_for_file(work_dir / "group.json")
    group = load_json(work_dir / "group.json")

    group_id = int(group["group_id"])
    source_node_id = int(group["source_node_id"])

    dp.join_group(group_id)
    if not dp.wait_for_local_membership(group_id, timeout_ms=TIMEOUT_S * 1000):
        raise TimeoutError(f"timed out waiting for local membership: {group_id}")
    sid = dp.receive_data(group_id, source_node_id)
    (work_dir / f"ready-{dp.node_id}.json").write_text(json.dumps({"node_id": dp.node_id}))

    if not dp.lossless_wait(sid, timeout_ms=TIMEOUT_S * 1000):
        raise RuntimeError(f"receiver session did not complete: {sid}")

    payload = bytes(dp.get_data_buffer(sid).read())
    (work_dir / f"out-{dp.node_id}.bin").write_bytes(payload)
    return 0


def run_source(dp: nm.Dataplane, scenario: dict, work_dir: Path) -> int:
    dp.create_group("multi-tree-ns")
    group = dp.group_is_ready(timeout_ms=TIMEOUT_S * 1000)
    if not group:
        raise TimeoutError("timed out waiting for group assignment")

    group_id, group_ip, source_node_id = group
    (work_dir / "group.json").write_text(
        json.dumps(
            {
                "group_id": group_id,
                "source_node_id": source_node_id,
            }
        )
    )

    trees = [
        (int(tree["tree_id"]), [(int(a), int(b)) for a, b in tree["edges"]])
        for tree in scenario["trees"]
    ]
    if scenario["mode"] == "fec":
        dp.set_group_routes_multi(group_id, trees)
    else:
        dp.set_group_routes(group_id, trees[0][1])

    if not dp.wait_for_group_routes(
        group_id,
        source_node_id,
        min_routes=len(trees),
        timeout_ms=TIMEOUT_S * 1000,
    ):
        raise TimeoutError("timed out waiting for group routes")

    receiver_ids = scenario["receiver_ids"]
    wait_for_ready(work_dir, receiver_ids)

    payload = Path(scenario["input_file"]).read_bytes()
    sid = dp.send_data(group_id, group_ip, receiver_ids, nm.PacketView(payload), block_size=8500)
    if not dp.lossless_wait(sid, timeout_ms=TIMEOUT_S * 1000):
        raise RuntimeError(f"source session did not complete: {sid}")
    return 0


def main() -> int:
    args = parse_args()
    scenario = load_json(args.work_dir / "scenario.json")

    dp = nm.Dataplane(str(args.config))
    if not dp.wait_for_topology_ready(timeout_ms=TIMEOUT_S * 1000):
        raise TimeoutError("timed out waiting for topology ready")

    if args.role == "source":
        return run_source(dp, scenario, args.work_dir)
    if args.role == "receiver":
        return run_receiver(dp, args.work_dir)

    role_hold()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise
