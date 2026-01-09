#!/usr/bin/env python3
"""Benchmark LP planner advantage across worker/relay configurations.

This script evaluates different multicast tree algorithms under various
network capacity profiles to identify scenarios where LP-based planning
provides significant throughput advantages.

Usage:
    python examples/lp/benchmark_lp_advantage.py [--output results.json]

By default, outputs a timestamped JSON under `examples/lp/experiment_results/`.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# Add parent to path for direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.lp.graph import Graph
from examples.lp.solver import compute_tree_edges, TreeResult


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""
    n_workers: int
    n_relays: int
    profile: str
    algorithm: str
    throughput: float | None
    planning_time_ms: float
    tree_depth: int
    relays_used: int
    edges: list[tuple[int, int]]
    error: str | None = None


@dataclass
class BenchmarkSummary:
    """Summary statistics for a configuration."""
    n_workers: int
    n_relays: int
    profile: str
    results: dict[str, BenchmarkResult] = field(default_factory=dict)

    @property
    def lp_advantage_ratio(self) -> float | None:
        """Ratio of cf_bottleneck throughput to best baseline."""
        cf = self.results.get("cf_bottleneck")
        if cf is None or cf.throughput is None:
            return None

        baselines = ["star", "two_level", "basic_tree"]
        baseline_throughputs = [
            self.results[algo].throughput
            for algo in baselines
            if algo in self.results and self.results[algo].throughput is not None
        ]
        if not baseline_throughputs:
            return None

        best_baseline = max(baseline_throughputs)
        if best_baseline <= 0:
            return None
        return cf.throughput / best_baseline


def make_symmetric_full_mesh_graph(
    *,
    n_nodes: int,
    capacity_undirected: dict[tuple[int, int], float],
) -> Graph:
    """Create a full-mesh graph with symmetric directed edges."""
    nodes = list(range(1, n_nodes + 1))
    edges: list[tuple[int, int]] = [(i, j) for i in nodes for j in nodes if i != j]

    capacities: dict[tuple[int, int], float] = {}
    for i in nodes:
        for j in nodes:
            if i == j:
                continue
            u, v = (i, j) if i < j else (j, i)
            capacities[(i, j)] = float(capacity_undirected[(u, v)])

    return Graph(nodes, edges, capacities)


# Capacity profile generators


def make_uniform_caps(n_nodes: int, value: float = 100.0) -> dict[tuple[int, int], float]:
    """Uniform capacity on all links."""
    caps: dict[tuple[int, int], float] = {}
    for u in range(1, n_nodes + 1):
        for v in range(u + 1, n_nodes + 1):
            caps[(u, v)] = float(value)
    return caps


def make_weak_direct_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
    weak_cap: float = 10.0,
    strong_cap: float = 100.0,
) -> dict[tuple[int, int], float]:
    """Weak direct links from src to workers, strong via relays."""
    caps = make_uniform_caps(n_nodes, strong_cap)

    for w in workers:
        u, v = (src, w) if src < w else (w, src)
        caps[(u, v)] = weak_cap

    return caps


def make_hierarchy_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
    default_cap: float = 30.0,
    src_to_worker_cap: float = 5.0,
    src_to_relay_cap: float = 100.0,
    relay_to_worker_cap: float = 100.0,
) -> dict[tuple[int, int], float]:
    """Hierarchy: strong src→relay and relay→worker, weak src→worker."""
    caps = make_uniform_caps(n_nodes, default_cap)

    for w in workers:
        u, v = (src, w) if src < w else (w, src)
        caps[(u, v)] = src_to_worker_cap

    for r in relays:
        u, v = (src, r) if src < r else (r, src)
        caps[(u, v)] = src_to_relay_cap

        for w in workers:
            u, v = (r, w) if r < w else (w, r)
            caps[(u, v)] = relay_to_worker_cap

    return caps


def make_random_caps(
    n_nodes: int,
    min_cap: float = 10.0,
    max_cap: float = 100.0,
    seed: int = 42,
) -> dict[tuple[int, int], float]:
    """Random uniform capacities."""
    rng = random.Random(seed)
    caps: dict[tuple[int, int], float] = {}
    for u in range(1, n_nodes + 1):
        for v in range(u + 1, n_nodes + 1):
            caps[(u, v)] = float(rng.uniform(min_cap, max_cap))
    return caps


def make_clustered_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
    strong_cap: float = 100.0,
    weak_cap: float = 10.0,
) -> dict[tuple[int, int], float]:
    """Half workers have strong direct links, half have weak."""
    caps = make_uniform_caps(n_nodes, strong_cap)

    weak_workers = workers[len(workers) // 2:]
    for w in weak_workers:
        u, v = (src, w) if src < w else (w, src)
        caps[(u, v)] = weak_cap

    return caps


def make_competing_paths_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
    seed: int = 123,
) -> dict[tuple[int, int], float]:
    """Multiple relay options with different cost/benefit tradeoffs.

    This creates a scenario where:
    - Direct links are weak (10 Mbps)
    - Each relay has different capacity to src and to different workers
    - LP should identify the best relay-worker assignments
    """
    rng = random.Random(seed)
    caps: dict[tuple[int, int], float] = {}

    # Default low capacity
    for u in range(1, n_nodes + 1):
        for v in range(u + 1, n_nodes + 1):
            caps[(u, v)] = 20.0

    # Weak direct src->worker links
    for w in workers:
        u, v = (src, w) if src < w else (w, src)
        caps[(u, v)] = 10.0

    # Each relay has varying capacity to src and to workers
    for i, r in enumerate(relays):
        # src->relay capacity varies
        u, v = (src, r) if src < r else (r, src)
        caps[(u, v)] = 50.0 + i * 20  # 50, 70, 90, ...

        # relay->worker capacity: each relay is "good" for different workers
        for j, w in enumerate(workers):
            u, v = (r, w) if r < w else (w, r)
            # Relay i is best for worker (i % n_workers)
            if j == i % len(workers):
                caps[(u, v)] = 100.0
            else:
                caps[(u, v)] = 30.0 + rng.uniform(0, 20)

    return caps


def make_chain_required_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
) -> dict[tuple[int, int], float]:
    """Scenario where multi-hop chains through relays are required.

    - Direct links are very weak (5 Mbps)
    - src->relay[0] is strong, relay[i]->relay[i+1] is strong
    - Only last relay has good links to workers
    """
    caps: dict[tuple[int, int], float] = {}

    # Default weak
    for u in range(1, n_nodes + 1):
        for v in range(u + 1, n_nodes + 1):
            caps[(u, v)] = 5.0

    if not relays:
        return caps

    # src -> first relay is strong
    r0 = relays[0]
    u, v = (src, r0) if src < r0 else (r0, src)
    caps[(u, v)] = 100.0

    # Chain between relays
    for i in range(len(relays) - 1):
        r1, r2 = relays[i], relays[i + 1]
        u, v = (r1, r2) if r1 < r2 else (r2, r1)
        caps[(u, v)] = 100.0

    # Last relay -> workers
    last_relay = relays[-1]
    for w in workers:
        u, v = (last_relay, w) if last_relay < w else (w, last_relay)
        caps[(u, v)] = 100.0

    return caps


def make_one_strong_relay_caps(
    n_nodes: int,
    src: int,
    workers: list[int],
    relays: list[int],
) -> dict[tuple[int, int], float]:
    """Only one relay has good connectivity; others are weak.

    LP should identify the single "hub" relay that can serve all workers.
    Basic heuristics may pick wrong relays based on local decisions.
    """
    caps: dict[tuple[int, int], float] = {}

    # Default low
    for u in range(1, n_nodes + 1):
        for v in range(u + 1, n_nodes + 1):
            caps[(u, v)] = 15.0

    # Direct src->worker: weak
    for w in workers:
        u, v = (src, w) if src < w else (w, src)
        caps[(u, v)] = 10.0

    if not relays:
        return caps

    # Pick middle relay as the "strong hub"
    hub = relays[len(relays) // 2]

    # src -> hub is strong
    u, v = (src, hub) if src < hub else (hub, src)
    caps[(u, v)] = 100.0

    # hub -> all workers is strong
    for w in workers:
        u, v = (hub, w) if hub < w else (w, hub)
        caps[(u, v)] = 100.0

    return caps


def compute_tree_depth(edges: list[tuple[int, int]], src: int) -> int:
    """Compute max depth of tree from source."""
    if not edges:
        return 0

    children: dict[int, list[int]] = {}
    for u, v in edges:
        children.setdefault(u, []).append(v)

    def dfs(node: int, depth: int) -> int:
        max_depth = depth
        for child in children.get(node, []):
            max_depth = max(max_depth, dfs(child, depth + 1))
        return max_depth

    return dfs(src, 0)


def count_relays_used(edges: list[tuple[int, int]], src: int, workers: set[int]) -> int:
    """Count non-source, non-worker nodes used in tree."""
    nodes_in_tree = set()
    for u, v in edges:
        nodes_in_tree.add(u)
        nodes_in_tree.add(v)

    relays = nodes_in_tree - {src} - workers
    return len(relays)


def run_single_benchmark(
    graph: Graph,
    src: int,
    workers: list[int],
    n_workers: int,
    n_relays: int,
    profile: str,
    algorithm: str,
    hop_limit: int,
    allow_destinations_as_relays: bool,
) -> BenchmarkResult:
    """Run a single algorithm on a single configuration."""
    start = time.perf_counter()

    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=workers,
            algorithm=algorithm,
            hop_limit=hop_limit,
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            n_workers=n_workers,
            n_relays=n_relays,
            profile=profile,
            algorithm=algorithm,
            throughput=None,
            planning_time_ms=elapsed_ms,
            tree_depth=0,
            relays_used=0,
            edges=[],
            error=str(e),
        )

    elapsed_ms = (time.perf_counter() - start) * 1000

    tree_depth = compute_tree_depth(result.edges, src)
    relays_used = count_relays_used(result.edges, src, set(workers))

    return BenchmarkResult(
        n_workers=n_workers,
        n_relays=n_relays,
        profile=profile,
        algorithm=algorithm,
        throughput=result.throughput,
        planning_time_ms=elapsed_ms,
        tree_depth=tree_depth,
        relays_used=relays_used,
        edges=result.edges,
        error=result.error,
    )


def run_benchmark(
    n_workers_list: list[int] = [2, 4, 6, 8, 10],
    n_relays_list: list[int] = [2, 4, 6, 8, 10],
    profiles: list[str] = ["uniform", "weak_direct", "hierarchy", "random", "clustered"],
    algorithms: list[str] = ["star", "two_level", "basic_tree", "cf_bottleneck"],
    hop_limit: int = 3,
    allow_destinations_as_relays: bool = False,
    verbose: bool = True,
) -> list[BenchmarkSummary]:
    """Run full benchmark across all configurations."""
    summaries: list[BenchmarkSummary] = []

    total_configs = len(n_workers_list) * len(n_relays_list) * len(profiles)
    current = 0

    for n_workers in n_workers_list:
        for n_relays in n_relays_list:
            for profile in profiles:
                current += 1

                # Build topology
                src = 1
                workers = list(range(2, 2 + n_workers))
                relays = list(range(2 + n_workers, 2 + n_workers + n_relays))
                n_nodes = 1 + n_workers + n_relays

                # Generate capacity profile
                if profile == "uniform":
                    caps = make_uniform_caps(n_nodes)
                elif profile == "weak_direct":
                    caps = make_weak_direct_caps(n_nodes, src, workers, relays)
                elif profile == "hierarchy":
                    caps = make_hierarchy_caps(n_nodes, src, workers, relays)
                elif profile == "random":
                    caps = make_random_caps(n_nodes)
                elif profile == "clustered":
                    caps = make_clustered_caps(n_nodes, src, workers, relays)
                elif profile == "competing_paths":
                    caps = make_competing_paths_caps(n_nodes, src, workers, relays)
                elif profile == "chain_required":
                    caps = make_chain_required_caps(n_nodes, src, workers, relays)
                elif profile == "one_strong_relay":
                    caps = make_one_strong_relay_caps(n_nodes, src, workers, relays)
                else:
                    raise ValueError(f"Unknown profile: {profile}")

                graph = make_symmetric_full_mesh_graph(n_nodes=n_nodes, capacity_undirected=caps)

                summary = BenchmarkSummary(
                    n_workers=n_workers,
                    n_relays=n_relays,
                    profile=profile,
                )

                for algorithm in algorithms:
                    result = run_single_benchmark(
                        graph=graph,
                        src=src,
                        workers=workers,
                        n_workers=n_workers,
                        n_relays=n_relays,
                        profile=profile,
                        algorithm=algorithm,
                        hop_limit=hop_limit,
                        allow_destinations_as_relays=allow_destinations_as_relays,
                    )
                    summary.results[algorithm] = result

                summaries.append(summary)

                if verbose:
                    ratio = summary.lp_advantage_ratio
                    ratio_str = f"{ratio:.2f}x" if ratio else "N/A"
                    print(
                        f"[{current}/{total_configs}] "
                        f"W={n_workers} R={n_relays} {profile}: "
                        f"LP advantage={ratio_str}"
                    )

    return summaries


def summarize_results(summaries: list[BenchmarkSummary]) -> dict:
    """Generate summary statistics."""
    # Find best scenarios for LP advantage
    valid_summaries = [s for s in summaries if s.lp_advantage_ratio is not None]

    if not valid_summaries:
        return {"error": "No valid results"}

    # Sort by LP advantage
    by_advantage = sorted(valid_summaries, key=lambda s: s.lp_advantage_ratio or 0, reverse=True)

    # Group by profile
    by_profile: dict[str, list[float]] = {}
    for s in valid_summaries:
        ratio = s.lp_advantage_ratio
        if ratio:
            by_profile.setdefault(s.profile, []).append(ratio)

    profile_stats = {
        profile: {
            "mean": statistics.mean(ratios),
            "max": max(ratios),
            "min": min(ratios),
        }
        for profile, ratios in by_profile.items()
    }

    return {
        "top_10_scenarios": [
            {
                "workers": s.n_workers,
                "relays": s.n_relays,
                "profile": s.profile,
                "lp_advantage": s.lp_advantage_ratio,
                "cf_throughput": s.results.get("cf_bottleneck", BenchmarkResult(0, 0, "", "", None, 0, 0, 0, [])).throughput,
            }
            for s in by_advantage[:10]
        ],
        "profile_stats": profile_stats,
        "total_configs": len(summaries),
        "valid_configs": len(valid_summaries),
    }


def export_results(summaries: list[BenchmarkSummary], output_path: str) -> None:
    """Export full results to JSON."""
    data = {
        "summaries": [
            {
                "n_workers": s.n_workers,
                "n_relays": s.n_relays,
                "profile": s.profile,
                "lp_advantage_ratio": s.lp_advantage_ratio,
                "results": {
                    algo: {
                        "throughput": r.throughput,
                        "planning_time_ms": r.planning_time_ms,
                        "tree_depth": r.tree_depth,
                        "relays_used": r.relays_used,
                        "error": r.error,
                    }
                    for algo, r in s.results.items()
                },
            }
            for s in summaries
        ],
        "summary": summarize_results(summaries),
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark LP planner advantage.")
    parser.add_argument(
        "--workers",
        type=str,
        default="2,4,6,8,10",
        help="Comma-separated worker counts (default: 2,4,6,8,10)",
    )
    parser.add_argument(
        "--relays",
        type=str,
        default="2,4,6,8,10",
        help="Comma-separated relay counts (default: 2,4,6,8,10)",
    )
    parser.add_argument(
        "--profiles",
        type=str,
        default="uniform,weak_direct,hierarchy,random,clustered",
        help="Comma-separated capacity profiles",
    )
    parser.add_argument(
        "--algorithms",
        type=str,
        default="star,two_level,basic_tree,cf_bottleneck",
        help="Comma-separated algorithms to compare",
    )
    parser.add_argument(
        "--hop-limit",
        type=int,
        default=3,
        help="Max hops for tree algorithms (default: 3)",
    )
    parser.add_argument(
        "--allow-destination-relays",
        action="store_true",
        help="Allow workers to relay to other workers",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output JSON file path (default: examples/lp/experiment_results/benchmark_advantage_<ts>.json)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )

    args = parser.parse_args()

    n_workers_list = [int(x.strip()) for x in args.workers.split(",")]
    n_relays_list = [int(x.strip()) for x in args.relays.split(",")]
    profiles = [x.strip() for x in args.profiles.split(",")]
    algorithms = [x.strip() for x in args.algorithms.split(",")]

    print("=" * 60)
    print("LP Planner Advantage Benchmark")
    print("=" * 60)
    print(f"Workers: {n_workers_list}")
    print(f"Relays: {n_relays_list}")
    print(f"Profiles: {profiles}")
    print(f"Algorithms: {algorithms}")
    print(f"Hop limit: {args.hop_limit}")
    print(f"Allow destination relays: {args.allow_destination_relays}")
    print("=" * 60 + "\n")

    summaries = run_benchmark(
        n_workers_list=n_workers_list,
        n_relays_list=n_relays_list,
        profiles=profiles,
        algorithms=algorithms,
        hop_limit=args.hop_limit,
        allow_destinations_as_relays=args.allow_destination_relays,
        verbose=not args.quiet,
    )

    out_path = str(args.output or "").strip()
    if not out_path:
        output_dir = Path(__file__).parent / "experiment_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        out_path = str(output_dir / f"benchmark_advantage_{timestamp}.json")

    export_results(summaries, out_path)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    summary = summarize_results(summaries)

    print("\nTop scenarios for LP advantage:")
    for i, scenario in enumerate(summary.get("top_10_scenarios", [])[:5], 1):
        print(
            f"  {i}. W={scenario['workers']} R={scenario['relays']} "
            f"{scenario['profile']}: {scenario['lp_advantage']:.2f}x "
            f"(CF={scenario['cf_throughput']:.1f} Mbps)"
        )

    print("\nAverage LP advantage by profile:")
    for profile, stats in summary.get("profile_stats", {}).items():
        print(f"  {profile}: mean={stats['mean']:.2f}x (min={stats['min']:.2f}, max={stats['max']:.2f})")

    print(f"\nResults exported to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
