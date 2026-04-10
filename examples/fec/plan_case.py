#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import time
from pathlib import Path
from string import Template

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent
RELAYS_PER_TREE = 2
UNITS = {"gib": 1024**3, "gb": 1000**3, "mib": 1024**2, "mb": 1000**2, "kib": 1024, "kb": 1000, "b": 1}
CONTROLLER_TEMPLATE = Template((ROOT / "templates" / "controller-config.toml").read_text())
NODE_TEMPLATE = Template((ROOT / "templates" / "node.toml").read_text())


def parse_size(value: str) -> int:
    raw = value.strip().lower().replace("_", "")
    for suffix, scale in UNITS.items():
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)].strip()) * scale)
    return int(raw)


def emit(name: str, value: object) -> None:
    print(f"{name}={shlex.quote(str(value))}")


def emit_many(values: dict[str, object]) -> None:
    for name, value in values.items():
        emit(name, value)


def compute_edges(source_id: int, receiver_ids: list[int], relay_ids: list[int], tree_ids: list[int]) -> list[tuple[int, int]]:
    edges = set()
    for index, _tree_id in enumerate(tree_ids):
        start = index * RELAYS_PER_TREE
        relays = relay_ids[start : start + RELAYS_PER_TREE]
        edges.add((source_id, relays[0]))
        edges.update(zip(relays, relays[1:]))
        edges.update((relays[-1], node_id) for node_id in receiver_ids)
    return sorted(edges)


def write_payload(path: Path, size: int) -> None:
    pattern = bytes((index * 31 + 7) % 256 for index in range(65_536))
    with path.open("wb") as handle:
        remaining = size
        while remaining > 0:
            chunk = pattern[: min(remaining, len(pattern))]
            handle.write(chunk)
            remaining -= len(chunk)


def plan_case(cfg: dict, case_name: str) -> None:
    defaults = cfg.get("defaults", {})
    case = next(case for case in cfg["cases"] if case["name"] == case_name)
    nodes_by_id = {int(node["node_id"]): dict(node) for node in cfg["nodes"]}
    for node in nodes_by_id.values():
        node.setdefault("user", defaults.get("user", "ubuntu"))
        node.setdefault("port", 22)
        node.setdefault("identity_file", defaults.get("identity_file"))
        node.setdefault("network_interface", defaults.get("network_interface", "ens3"))

    trainer = next(node for node in nodes_by_id.values() if node["role"] == "trainer")
    source_id = int(trainer["node_id"])
    receiver_ids = [int(node_id) for node_id in case["receiver_ids"]]
    relay_ids = [int(node_id) for node_id in case.get("relay_ids", [])]
    tree_ids = [int(tree_id) for tree_id in case["tree_ids"]]
    explicit_edges = [tuple(e) for e in case["edges"]] if "edges" in case else None
    if explicit_edges is not None:
        edge_node_ids = {nid for edge in explicit_edges for nid in edge}
        active_node_ids = [source_id, *receiver_ids, *[nid for nid in relay_ids if nid in edge_node_ids]]
    else:
        active_node_ids = [source_id, *receiver_ids, *relay_ids[: len(tree_ids) * RELAYS_PER_TREE]]

    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{case_name}-{time.time_ns() % 1_000_000:06d}"
    out_root = Path(cfg.get("paths", {}).get("local_output_root", "/tmp/nextmini-fec")).expanduser()
    run_dir = out_root / run_id
    logs_dir = run_dir / "logs"
    run_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(exist_ok=True)
    payload_size = parse_size(str(case.get("payload_size", defaults.get("payload_size", "100MiB"))))
    payload_path = run_dir / "payload.bin"
    write_payload(payload_path, payload_size)
    remote_root = str(cfg.get("paths", {}).get("remote_run_root", "~/fec-runs")).strip().removeprefix("~/")
    controller_host = cfg["controller"]["host"]
    controller_cfg = run_dir / "controller-config.toml"
    edges = explicit_edges if explicit_edges is not None else compute_edges(source_id, receiver_ids, relay_ids, tree_ids)
    controller_cfg.write_text(
        CONTROLLER_TEMPLATE.substitute(
            n_nodes=len(dict.fromkeys(active_node_ids)),
            edges=", ".join(f"[{src}, {dst}]" for src, dst in edges),
        )
    )

    emit_many(
        {
            "RUN_ID": run_id,
            "CASE_NAME": case_name,
            "BLOCK_SIZE": int(case["block_size"]),
            "FEC_MODE": "on" if case["mode"] in {"fec", "mettle"} else "off",
            "SOURCE_NODE_ID": source_id,
            "RECEIVER_IDS": " ".join(str(node_id) for node_id in receiver_ids),
            "ACTIVE_NODE_IDS": " ".join(str(node_id) for node_id in dict.fromkeys(active_node_ids)),
            "STARTUP_NODE_IDS": " ".join(
                str(node_id)
                for node_id in dict.fromkeys(case.get("startup_order", active_node_ids))
            ),
            "RUN_DIR": run_dir,
            "LOGS_DIR": logs_dir,
            "PAYLOAD_PATH": payload_path,
            "PAYLOAD_SIZE_BYTES": payload_size,
            "CONTROLLER_CONFIG_PATH": controller_cfg,
            "REMOTE_RUN_DIR": f"{remote_root}/{run_id}",
            "CONTROLLER_HOST": controller_host,
            "CONTROLLER_USER": cfg["controller"]["user"],
            "CONTROLLER_KEY": str(Path(cfg["controller"].get("identity_file", "")).expanduser()) if cfg["controller"].get("identity_file") else "",
            "CONTROLLER_SSH_PORT": int(cfg["controller"].get("port", 22)),
            "CONTROLLER_IMAGE": cfg["images"]["controller"],
            "NODE_IMAGE": cfg["images"]["node"],
        }
    )

    for node_id in dict.fromkeys(active_node_ids):
        node = nodes_by_id[node_id]
        node_cfg = run_dir / f"node-{node_id}.toml"
        node_cfg.write_text(
            NODE_TEMPLATE.substitute(
                controller_addr=f"ws://{controller_host}:3000",
                host=node["host"],
                network_interface=node["network_interface"],
                node_id=node_id,
                block_size=int(case["block_size"]),
                ready_grace_ms=int(case.get("ready_grace_ms", defaults.get("ready_grace_ms", 1500))),
                peer_report_timeout_ms=int(case.get("peer_report_timeout_ms", defaults.get("peer_report_timeout_ms", 5000))),
                fec_enabled=str(case["mode"] in {"fec", "mettle"}).lower(),
                mettle_enabled=str(case["mode"] == "mettle").lower(),
                tree_ids=", ".join(str(tree_id) for tree_id in tree_ids),
                symbols_per_block=int(case["symbols_per_block"]),
                mettle_coded_rate_numerator=int(case.get("mettle_coded_rate_numerator", defaults.get("mettle_coded_rate_numerator", 21))),
                mettle_coded_rate_denominator=int(case.get("mettle_coded_rate_denominator", defaults.get("mettle_coded_rate_denominator", 20))),
                channel_capacity=int(case.get("channel_capacity", defaults.get("channel_capacity", 1000))),
            )
        )
        emit_many(
            {
                f"NODE_ROLE_{node_id}": node["role"],
                f"NODE_HOST_{node_id}": node["host"],
                f"NODE_USER_{node_id}": node["user"],
                f"NODE_KEY_{node_id}": str(Path(node["identity_file"]).expanduser()) if node.get("identity_file") else "",
                f"NODE_PORT_{node_id}": int(node["port"]),
                f"NODE_CONFIG_{node_id}": node_cfg,
            }
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit shell variables for one FEC example case.")
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.example.toml")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--case")
    args = parser.parse_args()
    cfg = tomllib.loads(args.inventory.read_text())
    if args.list_cases:
        for case in cfg["cases"]:
            print(case["name"])
        return 0

    plan_case(cfg, args.case)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
