#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import pathlib
from typing import Any

from ledger import update_ledger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check saturated-edge tree incidence for laminarity."
    )
    parser.add_argument("solution", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--ledger", type=pathlib.Path)
    parser.add_argument("--tolerance", type=float, default=1e-8)
    return parser.parse_args()


def edge_tuple(raw_edge: Any) -> tuple[int, int]:
    if isinstance(raw_edge, dict):
        raw_edge = raw_edge["edge"]
    return int(raw_edge[0]), int(raw_edge[1])


def saturated_edges(
    solution: dict[str, Any], tolerance: float
) -> list[tuple[int, int]]:
    summaries = solution.get("tree_edge_usage") or []
    if summaries:
        candidates = []
        for summary in summaries:
            usage = float(summary.get("usage", 0.0))
            capacity = float(
                summary.get("induced_budget", summary.get("capacity", 0.0))
            )
            threshold = tolerance * max(1.0, abs(capacity))
            if usage > tolerance and capacity - usage <= threshold:
                candidates.append(edge_tuple(summary))
        return sorted(set(candidates))

    capacities = {
        (int(edge["src"]), int(edge["dst"])): float(edge["bw"])
        for edge in solution.get("scenario", {}).get("edges", [])
    }
    usage: dict[tuple[int, int], float] = collections.defaultdict(float)
    for tree in solution.get("trees", []):
        weight = float(tree.get("weight", 0.0))
        for raw_edge in tree.get("edges", []):
            usage[edge_tuple(raw_edge)] += weight

    candidates = []
    for edge, used in usage.items():
        capacity = capacities.get(edge)
        if capacity is None:
            continue
        threshold = tolerance * max(1.0, abs(capacity))
        if used > tolerance and capacity - used <= threshold:
            candidates.append(edge)
    return sorted(candidates)


def classify_incidence(
    incidence: frozenset[int],
    all_trees: frozenset[int],
    root_groups: set[frozenset[int]],
) -> str:
    if incidence == all_trees:
        return "all-trees"
    if len(incidence) == 1:
        return "singleton"
    if incidence in root_groups:
        return "root-group"
    return "other"


def analyze(solution: dict[str, Any], tolerance: float = 1e-8) -> dict[str, Any]:
    trees = solution.get("trees", [])
    tree_edges = {
        int(tree["tree_id"]): {edge_tuple(edge) for edge in tree.get("edges", [])}
        for tree in trees
    }
    all_trees = frozenset(tree_edges)
    source = int(solution.get("scenario", {}).get("source", 1))

    roots_by_tree = {
        tree_id: tuple(sorted(dst for src, dst in edges if src == source))
        for tree_id, edges in tree_edges.items()
    }
    grouped_roots: dict[tuple[int, ...], set[int]] = collections.defaultdict(set)
    for tree_id, root_signature in roots_by_tree.items():
        grouped_roots[root_signature].add(tree_id)
    root_groups = {frozenset(tree_ids) for tree_ids in grouped_roots.values()}

    saturated = saturated_edges(solution, tolerance)
    entries = []
    incidences = []
    for edge in saturated:
        incidence = frozenset(
            tree_id for tree_id, edges in tree_edges.items() if edge in edges
        )
        if not incidence:
            continue
        incidences.append((edge, incidence))
        entries.append(
            {
                "edge": list(edge),
                "tree_ids": sorted(incidence),
                "classification": classify_incidence(
                    incidence, all_trees, root_groups
                ),
            }
        )

    violations = []
    for index, (left_edge, left) in enumerate(incidences):
        for right_edge, right in incidences[index + 1 :]:
            if left.isdisjoint(right) or left <= right or right <= left:
                continue
            violations.append(
                {
                    "left_edge": list(left_edge),
                    "right_edge": list(right_edge),
                    "overlap_tree_ids": sorted(left & right),
                }
            )

    saturated_set = set(saturated)
    saturated_crossings = {
        str(tree_id): sum(edge in saturated_set for edge in edges)
        for tree_id, edges in tree_edges.items()
    }
    classifications = collections.Counter(entry["classification"] for entry in entries)
    return {
        "tree_count": len(tree_edges),
        "saturated_candidate_count": len(entries),
        "canonical_classification": dict(sorted(classifications.items())),
        "saturated_candidates": entries,
        "laminar": not violations,
        "violations": violations,
        "r": max(saturated_crossings.values(), default=0),
        "saturated_edges_crossed_by_tree": saturated_crossings,
    }


def main() -> None:
    args = parse_args()
    solution = json.loads(args.solution.read_text(encoding="utf-8"))
    report = analyze(solution, args.tolerance)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    if args.ledger is not None:
        update_ledger(args.ledger, "laminarity", report)
    print(rendered, end="")


if __name__ == "__main__":
    main()
