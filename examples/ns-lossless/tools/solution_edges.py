#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import pathlib
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit tc-ready edge conditions from a solve_multitree solution."
    )
    parser.add_argument("solution", type=pathlib.Path)
    parser.add_argument(
        "--profile",
        choices=("solution-edge-rates", "solution-two-edge-rates"),
        required=True,
    )
    parser.add_argument("--min-rate-mbit", type=float, default=0.0)
    parser.add_argument("--node-budgets-output", type=pathlib.Path)
    parser.add_argument("--node-budget-edges-output", type=pathlib.Path)
    return parser.parse_args()


def finite_nonnegative(edge: tuple[int, int], field: str, raw_value: Any) -> float:
    value = float(raw_value)
    if value < 0.0:
        raise ValueError(f"solution edge {edge[0]}->{edge[1]} has negative {field}")
    return value


def select_edges(
    solution: dict[str, Any], profile: str, min_rate_mbit: float
) -> list[dict[str, int | float | str]]:
    if min_rate_mbit < 0.0:
        raise ValueError("min_rate_mbit must be non-negative")

    tree_edges = collections.Counter(
        (int(src), int(dst))
        for tree in solution.get("trees", [])
        for src, dst in tree.get("edges", [])
    )
    scenario_edges = {
        (int(edge["src"]), int(edge["dst"])): edge
        for edge in solution.get("scenario", {}).get("edges", [])
    }

    if profile == "solution-two-edge-rates":
        selected = [
            edge
            for edge, _ in sorted(
                (
                    (edge, use_count)
                    for edge, use_count in tree_edges.items()
                    if edge[0] != 1 and edge in scenario_edges
                ),
                key=lambda item: (-item[1], float(scenario_edges[item[0]]["bw"])),
            )[:2]
        ]
    else:
        selected = [
            (int(edge["src"]), int(edge["dst"]))
            for edge in solution.get("scenario", {}).get("edges", [])
            if (int(edge["src"]), int(edge["dst"])) in tree_edges
        ]

    rows = []
    for src, dst in selected:
        raw_edge = scenario_edges[(src, dst)]
        bw_mbit = max(float(raw_edge["bw"]), min_rate_mbit)
        if bw_mbit <= 0.0:
            raise ValueError(f"solution edge {src}->{dst} has non-positive bw")
        edge_key = (src, dst)
        delay_ms = finite_nonnegative(edge_key, "delay_ms", raw_edge.get("delay_ms", 0.0))
        jitter_ms = finite_nonnegative(
            edge_key, "jitter_ms", raw_edge.get("jitter_ms", 0.0)
        )
        loss_pct = finite_nonnegative(edge_key, "loss_pct", raw_edge.get("loss_pct", 0.0))
        rows.append(
            {
                "dev": f"veth{dst - 1}a",
                "src_ip": f"172.16.8.{src + 1}",
                "dst_ip": f"172.16.8.{dst + 1}",
                "rate_kbit": max(1, int(round(bw_mbit * 1000.0))),
                "src": src,
                "dst": dst,
                "delay_ms": delay_ms,
                "jitter_ms": jitter_ms,
                "loss_pct": loss_pct,
            }
        )
    return rows


def select_node_budget_plan(
    solution: dict[str, Any],
) -> tuple[list[dict[str, int | str]], list[dict[str, int | float | str]]]:
    scenario = solution.get("scenario", {})
    tree_edges = {
        (int(src), int(dst))
        for tree in solution.get("trees", [])
        for src, dst in tree.get("edges", [])
    }
    scenario_edges = {
        (int(edge["src"]), int(edge["dst"])): edge
        for edge in scenario.get("edges", [])
    }
    raw_node_caps = scenario.get("node_caps") or {}
    if not isinstance(raw_node_caps, dict):
        raise ValueError("scenario.node_caps must be an object")
    raw_budgets = []
    for direction in ("egress", "ingress"):
        section = raw_node_caps.get(direction, {})
        if not isinstance(section, dict):
            raise ValueError(f"scenario.node_caps.{direction} must be an object")
        for raw_node, raw_bw in sorted(section.items(), key=lambda item: int(item[0])):
            node = int(raw_node)
            bw = float(raw_bw)
            if bw <= 0.0:
                raise ValueError(
                    f"scenario.node_caps.{direction}[{node}] must be positive"
                )
            edges = sorted(
                edge
                for edge in scenario_edges
                if edge[0 if direction == "egress" else 1] == node
            )
            if not edges:
                raise ValueError(
                    f"scenario.node_caps.{direction}[{node}] controls no edges"
                )
            raw_budgets.append(
                {
                    "budget_id": f"node-{node}-{direction}",
                    "name": f"node {node} {direction}",
                    "node": node,
                    "direction": direction,
                    "bw": bw,
                    "edges": edges,
                }
            )
    ingress_nodes = {
        int(node) for node in raw_node_caps.get("ingress", {})
    }
    ingress_controlled = {
        edge for edge in scenario_edges if edge[1] in ingress_nodes
    }
    budgets = []
    leaves = []
    for index, budget in enumerate(raw_budgets):
        budget_id = str(budget.get("budget_id", f"b{index}"))
        direction = str(budget["direction"])
        if direction not in {"egress", "ingress"}:
            raise ValueError(f"node budget {budget_id} has invalid direction {direction}")
        node = int(budget["node"])
        rate_kbit = max(1, int(round(float(budget["bw"]) * 1000.0)))
        budgets.append(
            {
                "budget_id": budget_id,
                "direction": direction,
                "node": node,
                "dev": f"veth{node - 1}{'b' if direction == 'egress' else 'a'}",
                "rate_kbit": rate_kbit,
                "name": str(budget["name"]),
            }
        )
        budget_leaves = []
        for raw_edge in budget["edges"]:
            src, dst = int(raw_edge[0]), int(raw_edge[1])
            edge = scenario_edges.get((src, dst))
            if edge is None:
                raise ValueError(
                    f"node budget {budget_id} references missing runtime edge {src}->{dst}"
                )
            if (src, dst) not in tree_edges:
                continue
            apply_conditions = direction == "ingress" or (src, dst) not in ingress_controlled
            edge_rate_kbit = max(1, int(round(float(edge["bw"]) * 1000.0)))
            budget_leaves.append(
                {
                    "budget_id": budget_id,
                    "src_ip": f"172.16.8.{src + 1}",
                    "dst_ip": f"172.16.8.{dst + 1}",
                    "edge_rate_kbit": edge_rate_kbit,
                    "src": src,
                    "dst": dst,
                    "delay_ms": finite_nonnegative(
                        (src, dst), "delay_ms", edge.get("delay_ms", 0.0)
                    )
                    if apply_conditions
                    else 0.0,
                    "jitter_ms": finite_nonnegative(
                        (src, dst), "jitter_ms", edge.get("jitter_ms", 0.0)
                    )
                    if apply_conditions
                    else 0.0,
                    "loss_pct": finite_nonnegative(
                        (src, dst), "loss_pct", edge.get("loss_pct", 0.0)
                    )
                    if apply_conditions
                    else 0.0,
                }
            )
        if budget_leaves:
            fair_share_kbit = max(1, rate_kbit // len(budget_leaves))
            for leaf in budget_leaves:
                edge_rate_kbit = int(leaf.pop("edge_rate_kbit"))
                leaf["guaranteed_kbit"] = min(edge_rate_kbit, fair_share_kbit)
                leaf["ceil_kbit"] = min(edge_rate_kbit, rate_kbit)
        leaves.extend(budget_leaves)
    return budgets, leaves


def write_node_budget_plan(
    solution: dict[str, Any], budget_path: pathlib.Path, edge_path: pathlib.Path
) -> None:
    budgets, leaves = select_node_budget_plan(solution)
    budget_path.parent.mkdir(parents=True, exist_ok=True)
    edge_path.parent.mkdir(parents=True, exist_ok=True)
    budget_path.write_text(
        "".join(
            "\t".join(
                (
                    str(row["budget_id"]),
                    str(row["direction"]),
                    str(row["node"]),
                    str(row["dev"]),
                    str(row["rate_kbit"]),
                    str(row["name"]),
                )
            )
            + "\n"
            for row in budgets
        ),
        encoding="utf-8",
    )
    edge_path.write_text(
        "".join(
            "\t".join(
                (
                    str(row["budget_id"]),
                    str(row["src_ip"]),
                    str(row["dst_ip"]),
                    str(row["guaranteed_kbit"]),
                    str(row["ceil_kbit"]),
                    str(row["src"]),
                    str(row["dst"]),
                    format_number(float(row["delay_ms"])),
                    format_number(float(row["jitter_ms"])),
                    format_number(float(row["loss_pct"])),
                )
            )
            + "\n"
            for row in leaves
        ),
        encoding="utf-8",
    )


def format_number(value: float) -> str:
    return f"{value:.9f}".rstrip("0").rstrip(".") or "0"


def main() -> None:
    args = parse_args()
    solution = json.loads(args.solution.read_text(encoding="utf-8"))
    if (args.node_budgets_output is None) != (args.node_budget_edges_output is None):
        raise ValueError(
            "--node-budgets-output and --node-budget-edges-output must be used together"
        )
    if args.node_budgets_output is not None:
        assert args.node_budget_edges_output is not None
        write_node_budget_plan(
            solution, args.node_budgets_output, args.node_budget_edges_output
        )
    for row in select_edges(solution, args.profile, args.min_rate_mbit):
        print(
            "\t".join(
                (
                    str(row["dev"]),
                    str(row["src_ip"]),
                    str(row["dst_ip"]),
                    str(row["rate_kbit"]),
                    str(row["src"]),
                    str(row["dst"]),
                    format_number(float(row["delay_ms"])),
                    format_number(float(row["jitter_ms"])),
                    format_number(float(row["loss_pct"])),
                )
            )
        )


if __name__ == "__main__":
    main()
