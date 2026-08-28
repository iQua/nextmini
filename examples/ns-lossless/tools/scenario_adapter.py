#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import tomllib
from typing import Any


EDGE_OPTIONAL_FIELDS = ("delay_ms", "jitter_ms", "loss_pct")
NODE_CAP_DIRECTIONS = ("egress", "ingress")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a replayed probe or compact synthetic graph into solve_multitree.py input."
    )
    parser.add_argument("input", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument(
        "--inventory-output",
        type=pathlib.Path,
        help="Also write a minimal inventory for solve_multitree.py.",
    )
    parser.add_argument(
        "--solver-backend",
        choices=("gurobi", "scipy"),
        default="gurobi",
        help="Use edge_based/Gurobi, or the path_based/SciPy fallback.",
    )
    parser.add_argument("--max-tree-hops", type=int, default=3)
    return parser.parse_args()


def load_input(path: pathlib.Path) -> dict[str, Any]:
    if path.suffix.lower() == ".toml":
        return tomllib.loads(path.read_text(encoding="utf-8"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("scenario input must be a JSON/TOML object")
    return payload


def normalized_edge(
    raw_edge: dict[str, Any],
    *,
    default_bw: float | None,
    defaults: dict[str, Any],
) -> dict[str, int | float]:
    try:
        src = int(raw_edge["src"])
        dst = int(raw_edge["dst"])
    except KeyError as exc:
        raise ValueError(f"edge is missing {exc.args[0]}: {raw_edge}") from exc

    raw_bw = raw_edge.get("bw", default_bw)
    if raw_bw is None:
        raise ValueError(f"edge {src}->{dst} has no bw and no default bandwidth")
    bw = float(raw_bw)
    if bw <= 0.0:
        raise ValueError(f"edge {src}->{dst} must have positive bw")

    edge: dict[str, int | float] = {"src": src, "dst": dst, "bw": bw}
    for field in EDGE_OPTIONAL_FIELDS:
        raw_value = raw_edge.get(field, defaults.get(field))
        if raw_value is None:
            continue
        value = float(raw_value)
        if value < 0.0:
            raise ValueError(f"edge {src}->{dst} has negative {field}")
        edge[field] = value
    return edge


def receiver_relays_enabled(spec: dict[str, Any]) -> bool:
    enabled = spec.get("receiver_relays", True)
    if not isinstance(enabled, bool):
        raise ValueError("scenario.receiver_relays must be a boolean")
    return enabled


def receiver_relay_capacity(
    spec: dict[str, Any],
    receiver: int,
    edges: list[dict[str, int | float]],
) -> float:
    configured = spec.get("receiver_relay_bw")
    if isinstance(configured, dict):
        configured = configured.get(str(receiver), configured.get(receiver))
    if configured is not None:
        capacity = float(configured)
    else:
        raw_node_caps = spec.get("node_caps") or {}
        if not isinstance(raw_node_caps, dict):
            raise ValueError("scenario.node_caps must be an object")
        raw_ingress = raw_node_caps.get("ingress", {})
        if not isinstance(raw_ingress, dict):
            raise ValueError("scenario.node_caps.ingress must be an object")
        raw_capacity = raw_ingress.get(str(receiver), raw_ingress.get(receiver))
        if raw_capacity is not None:
            capacity = float(raw_capacity)
        else:
            default_capacity = spec.get("downlink_bw", spec.get("default_bw"))
            if default_capacity is not None:
                capacity = float(default_capacity)
            else:
                incoming = [
                    float(edge["bw"])
                    for edge in edges
                    if int(edge["dst"]) == receiver
                ]
                if not incoming:
                    raise ValueError(
                        f"receiver-relay edge capacity for receiver {receiver} "
                        "requires receiver_relay_bw, an ingress node cap, "
                        "downlink_bw, default_bw, or an existing incoming edge"
                    )
                capacity = max(incoming)
    if capacity <= 0.0:
        raise ValueError(
            f"receiver-relay edge capacity for receiver {receiver} must be positive"
        )
    return capacity


def add_receiver_relay_edges(
    spec: dict[str, Any],
    receivers: list[int],
    edges: list[dict[str, int | float]],
) -> list[dict[str, int | float]]:
    if not receiver_relays_enabled(spec) or len(receivers) < 2:
        return edges

    defaults = dict(spec.get("heterogeneity", {}))
    existing = {edge_pair(edge) for edge in edges}
    expanded = list(edges)
    for src in receivers:
        for dst in receivers:
            if src == dst or (src, dst) in existing:
                continue
            expanded.append(
                normalized_edge(
                    {"src": src, "dst": dst},
                    default_bw=receiver_relay_capacity(spec, dst, edges),
                    defaults=defaults,
                )
            )
    return expanded


def expand_compact_edges(spec: dict[str, Any]) -> list[dict[str, int | float]]:
    source = int(spec.get("source", 1))
    receivers = [int(node_id) for node_id in spec.get("receivers", [])]
    receiver_set = set(receivers)
    relays = [
        int(node_id)
        for node_id in spec.get("forwarding_nodes", [])
        if int(node_id) not in receiver_set and int(node_id) != source
    ]
    defaults = dict(spec.get("heterogeneity", {}))

    if spec.get("edges"):
        default_bw = spec.get("default_bw")
        return [
            normalized_edge(edge, default_bw=default_bw, defaults=defaults)
            for edge in spec["edges"]
        ]

    if not relays:
        raise ValueError("compact scenarios require forwarding_nodes")
    if not receivers:
        raise ValueError("compact scenarios require receivers")

    uplink_bw = spec.get("uplink_bw", spec.get("default_bw"))
    downlink_bw = spec.get("downlink_bw", spec.get("default_bw"))
    raw_uplinks = spec.get("uplinks") or [{"dst": relay} for relay in relays]
    raw_downlinks = spec.get("downlinks") or [
        {"src": relay, "dst": receiver}
        for relay in relays
        for receiver in receivers
    ]

    edges = []
    for raw_edge in raw_uplinks:
        edge = {"src": source, **raw_edge}
        edges.append(normalized_edge(edge, default_bw=uplink_bw, defaults=defaults))
    for raw_edge in raw_downlinks:
        edges.append(
            normalized_edge(raw_edge, default_bw=downlink_bw, defaults=defaults)
        )
    return edges


def edge_pair(raw_edge: Any) -> tuple[int, int]:
    if isinstance(raw_edge, dict):
        return int(raw_edge["src"]), int(raw_edge["dst"])
    if not isinstance(raw_edge, (list, tuple)) or len(raw_edge) != 2:
        raise ValueError(f"expected edge [src, dst], got {raw_edge!r}")
    return int(raw_edge[0]), int(raw_edge[1])


def normalize_node_caps(
    raw_node_caps: dict[str, Any],
    edges: list[dict[str, int | float]],
    known_nodes: set[int],
) -> dict[str, dict[str, float]]:
    if not isinstance(raw_node_caps, dict):
        raise ValueError("scenario.node_caps must be an object")

    edge_pairs = {edge_pair(edge) for edge in edges}
    node_caps: dict[str, dict[str, float]] = {}
    for direction in NODE_CAP_DIRECTIONS:
        raw_section = raw_node_caps.get(direction, {})
        if not isinstance(raw_section, dict):
            raise ValueError(f"scenario.node_caps.{direction} must be an object")
        section: dict[str, float] = {}
        for raw_node, raw_bw in raw_section.items():
            node = int(raw_node)
            if node not in known_nodes:
                raise ValueError(
                    f"scenario.node_caps.{direction} references unknown node {node}"
                )
            bw = float(raw_bw)
            if bw <= 0.0:
                raise ValueError(
                    f"scenario.node_caps.{direction}[{node}] must be positive"
                )
            controlled = [
                edge
                for edge in edge_pairs
                if edge[0 if direction == "egress" else 1] == node
            ]
            if not controlled:
                raise ValueError(
                    f"scenario.node_caps.{direction}[{node}] controls no edges"
                )
            section[str(node)] = bw
        node_caps[direction] = section
    return node_caps


def adapt_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw_scenario = payload.get("scenario", payload)
    if not isinstance(raw_scenario, dict):
        raise ValueError("scenario must be an object")

    source = int(raw_scenario.get("source", 1))
    receivers = [int(node_id) for node_id in raw_scenario.get("receivers", [])]
    configured_forwarding_nodes = [
        int(node_id) for node_id in raw_scenario.get("forwarding_nodes", [])
    ]
    if not receivers:
        raise ValueError("scenario.receivers must not be empty")

    receiver_relays = receiver_relays_enabled(raw_scenario)
    receiver_set = set(receivers)
    pure_forwarding_nodes = [
        node_id
        for node_id in configured_forwarding_nodes
        if node_id not in receiver_set and node_id != source
    ]
    forwarding_nodes = list(
        dict.fromkeys(
            [*receivers, *pure_forwarding_nodes]
            if receiver_relays
            else pure_forwarding_nodes
        )
    )

    edges = add_receiver_relay_edges(
        raw_scenario, receivers, expand_compact_edges(raw_scenario)
    )
    scenario: dict[str, Any] = {
        "name": str(raw_scenario.get("name", payload.get("name", "closed-loop-scenario"))),
        "source": source,
        "receivers": receivers,
        "forwarding_nodes": forwarding_nodes,
        "receiver_relays": receiver_relays,
        "edges": edges,
    }
    raw_node_caps = raw_scenario.get("node_caps")
    if raw_node_caps is not None:
        known_nodes = {
            source,
            *receivers,
            *forwarding_nodes,
            *(node for edge in edges for node in edge_pair(edge)),
        }
        scenario["node_caps"] = normalize_node_caps(
            raw_node_caps, edges, known_nodes
        )
    return {"scenario": scenario}


def render_inventory(*, backend: str, max_tree_hops: int) -> str:
    if max_tree_hops <= 0:
        raise ValueError("max_tree_hops must be positive")
    variant = "edge_based" if backend == "gurobi" else "path_based"
    return f"""# Generated by examples/ns-lossless/tools/scenario_adapter.py
# The paper setting is edge_based/Gurobi. path_based/SciPy is the local fallback.
[solver]
execution = "local"
variant = "{variant}"
backend = "{backend}"
objective = "max_min"
seed = 0
tol = 1e-8
max_cg_iters = 500
max_tree_hops = {max_tree_hops}
max_tree_hops_auto_fallback = true
max_tree_hops_fallback_unbounded = false
"""


def main() -> None:
    args = parse_args()
    payload = adapt_payload(load_input(args.input))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if args.inventory_output is not None:
        args.inventory_output.parent.mkdir(parents=True, exist_ok=True)
        args.inventory_output.write_text(
            render_inventory(
                backend=args.solver_backend,
                max_tree_hops=args.max_tree_hops,
            ),
            encoding="utf-8",
        )

    print(args.output)


if __name__ == "__main__":
    main()
