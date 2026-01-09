"""
Full Algorithm Benchmark Matrix.

Tests all 8 algorithms across:
- Workers: 2, 4, 6, 8, 10
- Relays: 2, 4, 6, 8, 10  
- max_relays (budget): 2, 4, 6, 8, 10 (only when n_relays >= max_relays)

All results saved to JSON.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .graph import Graph
from .solver import TreeResult, compute_tree_edges
from .lp_advantage_bench import (
    ALGORITHMS,
    PROFILES,
    BenchmarkResult,
    mbps_to_completion_sec,
    build_graph,
)


def run_full_benchmark(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    max_relays: int | None,
    seed: int,
    hop_limit: int = 4,
    allow_destinations_as_relays: bool = False,
) -> list[BenchmarkResult]:
    """Run all 8 algorithms on a single configuration."""
    from .lp_advantage_bench import PROFILES
    
    rng = random.Random(seed)
    
    n_nodes = 1 + n_workers + n_relays
    src = 1
    terminals = list(range(2, 2 + n_workers))
    relays = list(range(2 + n_workers, 2 + n_workers + n_relays))
    
    # Generate capacity profile
    profile_fn = PROFILES.get(profile_name)
    if profile_fn is None:
        raise ValueError(f"Unknown profile: {profile_name}")
    
    capacities = profile_fn(n_nodes, src, terminals, relays, rng)
    graph, src, terminals, relays = build_graph(n_workers, n_relays, capacities)
    
    results: list[BenchmarkResult] = []
    
    for algo in ALGORITHMS:
        tree_result: TreeResult | None = None
        try:
            if max_relays is not None and max_relays < n_relays:
                # With relay selection
                tree_result = compute_tree_edges(
                    graph,
                    src=src,
                    destinations=terminals,
                    algorithm=algo,
                    hop_limit=hop_limit,
                    eta=0.1,
                    max_length=hop_limit,
                    num_paths=2,
                    allow_destinations_as_relays=allow_destinations_as_relays,
                    relay_nodes=relays,
                    max_relays=max_relays,
                    relay_scoring="coverage",
                )
            else:
                # Without relay selection
                tree_result = compute_tree_edges(
                    graph,
                    src=src,
                    destinations=terminals,
                    algorithm=algo,
                    hop_limit=hop_limit,
                    eta=0.1,
                    max_length=hop_limit,
                    num_paths=2,
                    allow_destinations_as_relays=allow_destinations_as_relays,
                )
            
            throughput = tree_result.throughput
            completion = mbps_to_completion_sec(throughput)
            error = tree_result.error
        except Exception as e:
            tree_result = None
            throughput = None
            completion = None
            error = str(e)
        
        results.append(BenchmarkResult(
            profile=profile_name,
            n_workers=n_workers,
            n_relays=n_relays,
            algorithm=algo,
            throughput_mbps=throughput,
            completion_time_sec=completion,
            lp_f_star=tree_result.lp_f_star if tree_result is not None else None,
            tree_edges=len(tree_result.edges) if tree_result is not None else 0,
            error=error,
            max_relays=max_relays,
        ))
    
    return results


def print_results_table(results: list[BenchmarkResult], max_relays: int | None) -> None:
    """Print results as a formatted table."""
    if not results:
        return
    
    profile = results[0].profile
    n_workers = results[0].n_workers
    n_relays = results[0].n_relays
    
    budget_str = f", Budget: {max_relays}" if max_relays else ", No Budget"
    
    print(f"\n{'='*80}")
    print(f"Profile: {profile}, Workers: {n_workers}, Relays: {n_relays}{budget_str}")
    print(f"{'='*80}")
    print(f"{'Algorithm':<20} {'Throughput':>12} {'Completion':>12} {'vs Best':>10}")
    print(f"{'':<20} {'(Mbps)':>12} {'(sec)':>12} {'':>10}")
    print("-" * 80)
    
    valid_results = [r for r in results if r.throughput_mbps is not None]
    best_tput = max(r.throughput_mbps for r in valid_results) if valid_results else 0
    
    for r in results:
        if r.throughput_mbps is None:
            tput_str = "FAIL"
            time_str = "N/A"
            vs_best = "N/A"
        else:
            tput_str = f"{r.throughput_mbps:.1f}"
            time_str = f"{r.completion_time_sec:.1f}" if r.completion_time_sec else "N/A"
            if best_tput > 0 and abs(r.throughput_mbps - best_tput) < 0.1:
                vs_best = "BEST"
            elif best_tput > 0:
                pct = (r.throughput_mbps - best_tput) / best_tput * 100
                vs_best = f"{pct:+.0f}%"
            else:
                vs_best = "N/A"
        
        print(f"{r.algorithm:<20} {tput_str:>12} {time_str:>12} {vs_best:>10}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Full Algorithm Benchmark Matrix")
    parser.add_argument("--profile", type=str, default="lp_trap",
                        help="Capacity profile to test")
    parser.add_argument("--workers", type=str, default="2,4,6,8,10",
                        help="Comma-separated worker counts")
    parser.add_argument("--relays", type=str, default="2,4,6,8,10",
                        help="Comma-separated relay counts")
    parser.add_argument("--budgets", type=str, default="2,4,6,8,10",
                        help="Comma-separated relay budgets")
    parser.add_argument("--hop-limit", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--allow-dest-relays", action="store_true",
                        help="Allow workers to also act as relays")
    parser.add_argument("--output-json", type=str, default=None)
    args = parser.parse_args()
    
    worker_counts = [int(x.strip()) for x in args.workers.split(",")]
    relay_counts = [int(x.strip()) for x in args.relays.split(",")]
    budget_values = [int(x.strip()) for x in args.budgets.split(",")]
    profile = args.profile
    
    all_results: list[BenchmarkResult] = []
    
    print("=" * 80)
    print("Full Algorithm Benchmark Matrix")
    print(f"Profile: {profile}")
    print(f"Workers: {worker_counts}")
    print(f"Relays: {relay_counts}")
    print(f"Budgets: {budget_values}")
    print("=" * 80)
    
    total_tests = 0
    
    for n_workers in worker_counts:
        for n_relays in relay_counts:
            for max_relays in budget_values:
                # Only test if budget <= available relays
                if max_relays > n_relays:
                    continue
                
                results = run_full_benchmark(
                    profile,
                    n_workers,
                    n_relays,
                    max_relays=max_relays if max_relays < n_relays else None,
                    seed=args.seed,
                    hop_limit=args.hop_limit,
                    allow_destinations_as_relays=args.allow_dest_relays,
                )
                all_results.extend(results)
                print_results_table(results, max_relays if max_relays < n_relays else None)
                total_tests += 1
    
    # Save to JSON
    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    
    # Include microseconds to avoid collisions when running multiple jobs quickly.
    timestamp = time.strftime("%Y%m%d_%H%M%S") + f"_{int(time.time() * 1_000_000) % 1_000_000:06d}"
    out_path = args.output_json or str(output_dir / f"full_matrix_{timestamp}.json")
    
    with open(out_path, "w") as f:
        json.dump({
            "config": {
                "profile": profile,
                "workers": worker_counts,
                "relays": relay_counts,
                "budgets": budget_values,
                "hop_limit": args.hop_limit,
                "seed": args.seed,
            },
            "results": [asdict(r) for r in all_results],
            "total_tests": total_tests,
        }, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"Total test configurations: {total_tests}")
    print(f"Total algorithm runs: {len(all_results)}")
    print(f"Results saved to: {out_path}")
    print("=" * 80)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
