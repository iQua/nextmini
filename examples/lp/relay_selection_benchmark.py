"""
Comprehensive LP Relay Selection Benchmark.

Experiments based on Paper Table 4 methodology:
- Workers: 2, 4, 6, 8, 10
- Relays (pool): 2, 4, 6, 8, 10
- Budget: 2, 4, 6, 8, 10 (where budget <= relays)
- Trials: 20 per configuration
- Methods: CF-RelaySelect, Capacity Heuristic, Random

Usage:
    python -m examples.lp.relay_selection_benchmark --profile=lp_trap --trials=20
    python -m examples.lp.relay_selection_benchmark --quick  # Fast test with 3 trials
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import matplotlib.pyplot as plt
    import numpy as np
    PLOTTING_AVAILABLE = True
except ImportError:
    plt = None
    np = None
    PLOTTING_AVAILABLE = False

from .graph import Graph
from .solver import compute_tree_edges, TreeResult
from .lp_advantage_bench import PROFILES, build_graph


# =============================================================================
# Configuration
# =============================================================================

WORKERS = [2, 4, 6, 8, 10]
RELAYS = [2, 4, 6, 8, 10]
BUDGETS = [2, 4, 6, 8, 10]
DEFAULT_TRIALS = 20
DEFAULT_PROFILE = "lp_trap"


# =============================================================================
# Relay Selection Methods
# =============================================================================

def select_by_capacity(graph: Graph, relays: list[int], k: int) -> list[int]:
    """Select top-k relays by sum of outgoing capacities (capacity heuristic)."""
    if k <= 0 or not relays:
        return []
    scored = []
    for r_id in relays:
        score = sum(cap for _, cap in graph.adj.get(r_id, []))
        scored.append((r_id, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [r_id for r_id, _ in scored[:min(k, len(scored))]]


def select_random(relays: list[int], k: int, rng: random.Random) -> list[int]:
    """Randomly select k relays."""
    if k <= 0 or not relays:
        return []
    return rng.sample(relays, min(k, len(relays)))


# =============================================================================
# Result Data Structures
# =============================================================================

@dataclass
class TrialResult:
    """Result from a single trial."""
    trial: int
    seed: int
    cf_throughput: float
    capacity_throughput: float
    random_throughput: float


@dataclass 
class ConfigResult:
    """Aggregated result for a configuration (multiple trials)."""
    n_workers: int
    n_relays: int
    budget: int
    profile: str
    n_trials: int
    cf_mean: float
    cf_std: float
    capacity_mean: float
    capacity_std: float
    random_mean: float
    random_std: float
    lp_vs_capacity_pct: float
    lp_vs_random_pct: float
    trials: list[TrialResult]


# =============================================================================
# Benchmark Runner
# =============================================================================

def run_single_trial(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    budget: int,
    trial: int,
    seed: int,
    hop_limit: int = 4,
    allow_destinations_as_relays: bool = False,
) -> TrialResult:
    """Run a single trial comparing all three relay selection methods."""

    rng = random.Random(seed)

    n_nodes = 1 + n_workers + n_relays
    src = 1
    terminals = list(range(2, 2 + n_workers))
    relays = list(range(2 + n_workers, 2 + n_workers + n_relays))

    # Generate capacity profile
    profile_fn = PROFILES[profile_name]
    capacities = profile_fn(n_nodes, src, terminals, relays, rng)
    graph, src, terminals, relays = build_graph(n_workers, n_relays, capacities)

    # Method 1: CF-RelaySelect (LP-guided)
    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm="cf_bottleneck",
            hop_limit=hop_limit,
            eta=0.1,
            max_length=hop_limit,
            num_paths=2,
            relay_nodes=relays,
            max_relays=budget,
            relay_scoring="coverage",
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
        cf_throughput = result.throughput or 0.0
    except Exception:
        cf_throughput = 0.0

    # Method 2: Capacity heuristic
    try:
        chosen = select_by_capacity(graph, relays, budget)
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm="cf_bottleneck",
            hop_limit=hop_limit,
            eta=0.1,
            max_length=hop_limit,
            num_paths=2,
            relay_nodes=chosen,
            max_relays=None,
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
        capacity_throughput = result.throughput or 0.0
    except Exception:
        capacity_throughput = 0.0

    # Method 3: Random selection
    try:
        rng_random = random.Random(seed + 10000)  # Different seed for random selection
        chosen = select_random(relays, budget, rng_random)
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm="cf_bottleneck",
            hop_limit=hop_limit,
            eta=0.1,
            max_length=hop_limit,
            num_paths=2,
            relay_nodes=chosen,
            max_relays=None,
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
        random_throughput = result.throughput or 0.0
    except Exception:
        random_throughput = 0.0
    
    return TrialResult(
        trial=trial,
        seed=seed,
        cf_throughput=cf_throughput,
        capacity_throughput=capacity_throughput,
        random_throughput=random_throughput,
    )


def run_config(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    budget: int,
    n_trials: int,
    base_seed: int = 0,
    allow_destinations_as_relays: bool = False,
) -> ConfigResult:
    """Run multiple trials for a single configuration."""

    trials = []
    for i in range(n_trials):
        seed = base_seed + i
        trials.append(run_single_trial(profile_name, n_workers, n_relays, budget, i, seed, allow_destinations_as_relays=allow_destinations_as_relays))

    # Aggregate statistics
    cf_values = [t.cf_throughput for t in trials]
    cap_values = [t.capacity_throughput for t in trials]
    rand_values = [t.random_throughput for t in trials]
    
    cf_mean = statistics.mean(cf_values)
    cf_std = statistics.stdev(cf_values) if len(cf_values) > 1 else 0.0
    cap_mean = statistics.mean(cap_values)
    cap_std = statistics.stdev(cap_values) if len(cap_values) > 1 else 0.0
    rand_mean = statistics.mean(rand_values)
    rand_std = statistics.stdev(rand_values) if len(rand_values) > 1 else 0.0
    
    # Calculate LP advantage percentages
    lp_vs_capacity = ((cf_mean - cap_mean) / cap_mean * 100) if cap_mean > 0 else 0.0
    lp_vs_random = ((cf_mean - rand_mean) / rand_mean * 100) if rand_mean > 0 else 0.0
    
    return ConfigResult(
        n_workers=n_workers,
        n_relays=n_relays,
        budget=budget,
        profile=profile_name,
        n_trials=n_trials,
        cf_mean=cf_mean,
        cf_std=cf_std,
        capacity_mean=cap_mean,
        capacity_std=cap_std,
        random_mean=rand_mean,
        random_std=rand_std,
        lp_vs_capacity_pct=lp_vs_capacity,
        lp_vs_random_pct=lp_vs_random,
        trials=trials,
    )


def run_full_benchmark(
    profile_name: str,
    workers: list[int],
    relays: list[int],
    budgets: list[int],
    n_trials: int,
    allow_destinations_as_relays: bool = False,
) -> list[ConfigResult]:
    """Run the complete benchmark matrix."""

    results = []
    total_configs = sum(1 for r in relays for b in budgets if b <= r) * len(workers)
    current = 0

    print(f"Running {total_configs} configurations × {n_trials} trials = {total_configs * n_trials} tests")
    print("=" * 70)

    for n_workers in workers:
        for n_relays in relays:
            for budget in budgets:
                if budget > n_relays:
                    continue

                current += 1
                print(f"[{current}/{total_configs}] Workers={n_workers}, Relays={n_relays}, Budget={budget}...", end=" ")

                result = run_config(profile_name, n_workers, n_relays, budget, n_trials, allow_destinations_as_relays=allow_destinations_as_relays)
                results.append(result)
                
                print(f"CF={result.cf_mean:.1f}±{result.cf_std:.1f}, "
                      f"Cap={result.capacity_mean:.1f}±{result.capacity_std:.1f}, "
                      f"LP{result.lp_vs_capacity_pct:+.1f}%")
    
    return results


# =============================================================================
# Visualization
# =============================================================================

def plot_budget_comparison(results: list[ConfigResult], output_dir: Path):
    """Plot throughput vs budget (like Paper Table 4)."""
    
    # Group by budget, averaging across all worker/relay configs
    budget_data: dict[int, dict[str, list[float]]] = {}
    
    for r in results:
        if r.budget not in budget_data:
            budget_data[r.budget] = {"cf": [], "capacity": [], "random": []}
        budget_data[r.budget]["cf"].append(r.cf_mean)
        budget_data[r.budget]["capacity"].append(r.capacity_mean)
        budget_data[r.budget]["random"].append(r.random_mean)
    
    budgets = sorted(budget_data.keys())
    cf_means = [statistics.mean(budget_data[b]["cf"]) for b in budgets]
    cap_means = [statistics.mean(budget_data[b]["capacity"]) for b in budgets]
    rand_means = [statistics.mean(budget_data[b]["random"]) for b in budgets]
    
    plt.figure(figsize=(10, 6))
    x = np.arange(len(budgets))
    width = 0.25
    
    bars1 = plt.bar(x - width, cf_means, width, label='CF-RelaySelect (LP)', color='#2ecc71')
    bars2 = plt.bar(x, cap_means, width, label='Capacity Heuristic', color='#3498db')
    bars3 = plt.bar(x + width, rand_means, width, label='Random', color='#e74c3c')
    
    plt.xlabel('Relay Budget', fontsize=12)
    plt.ylabel('Throughput (Mbps)', fontsize=12)
    plt.title('LP Relay Selection Advantage vs Budget', fontsize=14)
    plt.xticks(x, budgets)
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / "budget_comparison.png", dpi=150)
    plt.close()
    print(f"Saved: {output_dir / 'budget_comparison.png'}")


def plot_lp_advantage_heatmap(results: list[ConfigResult], output_dir: Path, budget: int = 2):
    """Plot LP advantage heatmap (workers × relays) for fixed budget."""
    
    # Filter for specific budget
    filtered = [r for r in results if r.budget == budget]
    
    # Create matrix
    workers_set = sorted(set(r.n_workers for r in filtered))
    relays_set = sorted(set(r.n_relays for r in filtered))
    
    matrix = np.zeros((len(workers_set), len(relays_set)))
    for r in filtered:
        i = workers_set.index(r.n_workers)
        j = relays_set.index(r.n_relays)
        matrix[i, j] = r.lp_vs_capacity_pct
    
    plt.figure(figsize=(8, 6))
    im = plt.imshow(matrix, cmap='RdYlGn', aspect='auto', vmin=-50, vmax=100)
    
    plt.xticks(range(len(relays_set)), relays_set)
    plt.yticks(range(len(workers_set)), workers_set)
    plt.xlabel('Relay Pool Size', fontsize=12)
    plt.ylabel('Worker Count', fontsize=12)
    plt.title(f'LP Advantage (%) over Capacity Heuristic (Budget={budget})', fontsize=14)
    
    # Add text annotations
    for i in range(len(workers_set)):
        for j in range(len(relays_set)):
            val = matrix[i, j]
            color = 'white' if abs(val) > 50 else 'black'
            plt.text(j, i, f'{val:.0f}%', ha='center', va='center', color=color, fontsize=10)
    
    plt.colorbar(im, label='LP Advantage (%)')
    plt.tight_layout()
    plt.savefig(output_dir / f"lp_advantage_heatmap_budget{budget}.png", dpi=150)
    plt.close()
    print(f"Saved: {output_dir / f'lp_advantage_heatmap_budget{budget}.png'}")


def plot_completion_time(results: list[ConfigResult], output_dir: Path, artifact_gb: float = 10.0):
    """Plot completion time for 10GB artifact."""
    
    # Filter for budget=2 (most interesting)
    filtered = [r for r in results if r.budget == 2]
    
    # Sort by workers
    filtered.sort(key=lambda r: (r.n_workers, r.n_relays))
    
    labels = [f"W{r.n_workers}R{r.n_relays}" for r in filtered]
    
    # Convert throughput to completion time
    artifact_bits = artifact_gb * 8 * 1024 * 1024 * 1024  # bits
    cf_times = [artifact_bits / (r.cf_mean * 1e6) if r.cf_mean > 0 else 0 for r in filtered]
    cap_times = [artifact_bits / (r.capacity_mean * 1e6) if r.capacity_mean > 0 else 0 for r in filtered]
    
    plt.figure(figsize=(12, 6))
    x = np.arange(len(labels))
    width = 0.35
    
    plt.bar(x - width/2, cf_times, width, label='CF-RelaySelect', color='#2ecc71')
    plt.bar(x + width/2, cap_times, width, label='Capacity Heuristic', color='#3498db')
    
    plt.xlabel('Configuration (Workers-Relays)', fontsize=12)
    plt.ylabel('Completion Time (seconds)', fontsize=12)
    plt.title(f'Completion Time for {artifact_gb:.0f}GB Artifact (Budget=2)', fontsize=14)
    plt.xticks(x, labels, rotation=45, ha='right')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / "completion_time.png", dpi=150)
    plt.close()
    print(f"Saved: {output_dir / 'completion_time.png'}")


def generate_summary_table(results: list[ConfigResult]) -> str:
    """Generate markdown summary table."""
    
    lines = [
        "# LP Relay Selection Benchmark Results",
        "",
        "## Summary by Budget (Paper Table 4 Style)",
        "",
        "Values are mean±SD across (workers, relay-pool) configurations of each configuration's mean throughput over trials.",
        "",
        "| Budget | CF-RelaySelect | Capacity | Random | LP vs Cap | LP vs Rand | Wins (LP>Cap) |",
        "|--------|---------------|----------|--------|-----------|------------|--------------|",
    ]
    
    # Group by budget
    budget_data: dict[int, list[ConfigResult]] = {}
    for r in results:
        if r.budget not in budget_data:
            budget_data[r.budget] = []
        budget_data[r.budget].append(r)
    
    for budget in sorted(budget_data.keys()):
        configs = budget_data[budget]
        cf_means = [r.cf_mean for r in configs]
        cap_means = [r.capacity_mean for r in configs]
        rand_means = [r.random_mean for r in configs]

        cf_mean = statistics.mean(cf_means)
        cf_std = statistics.stdev(cf_means) if len(cf_means) > 1 else 0.0
        cap_mean = statistics.mean(cap_means)
        cap_std = statistics.stdev(cap_means) if len(cap_means) > 1 else 0.0
        rand_mean = statistics.mean(rand_means)
        rand_std = statistics.stdev(rand_means) if len(rand_means) > 1 else 0.0
        
        lp_vs_cap = (cf_mean - cap_mean) / cap_mean * 100 if cap_mean > 0 else 0
        lp_vs_rand = (cf_mean - rand_mean) / rand_mean * 100 if rand_mean > 0 else 0
        wins_vs_cap = sum(1 for r in configs if r.cf_mean > r.capacity_mean + 1e-9)
        
        lines.append(
            f"| {budget} | {cf_mean:.1f}±{cf_std:.1f} | {cap_mean:.1f}±{cap_std:.1f} | "
            f"{rand_mean:.1f}±{rand_std:.1f} | {lp_vs_cap:+.1f}% | {lp_vs_rand:+.1f}% | {wins_vs_cap}/{len(configs)} |"
        )
    
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Comprehensive LP Relay Selection Benchmark")
    parser.add_argument("--profile", type=str, default=DEFAULT_PROFILE,
                        help=f"Capacity profile (default: {DEFAULT_PROFILE})")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS,
                        help=f"Number of trials per config (default: {DEFAULT_TRIALS})")
    parser.add_argument("--workers", type=str, default=",".join(map(str, WORKERS)),
                        help="Comma-separated worker counts")
    parser.add_argument("--relays", type=str, default=",".join(map(str, RELAYS)),
                        help="Comma-separated relay pool sizes")
    parser.add_argument("--budgets", type=str, default=",".join(map(str, BUDGETS)),
                        help="Comma-separated budget values")
    parser.add_argument("--quick", action="store_true",
                        help="Quick test with 3 trials")
    parser.add_argument("--no-plot", action="store_true",
                        help="Skip plot generation")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory")
    parser.add_argument(
        "--allow-destination-relays",
        action="store_true",
        help="Allow destination nodes to forward (workers-as-relays)",
    )
    args = parser.parse_args()

    # Parse arguments
    workers = [int(x.strip()) for x in args.workers.split(",")]
    relays = [int(x.strip()) for x in args.relays.split(",")]
    budgets = [int(x.strip()) for x in args.budgets.split(",")]
    n_trials = 3 if args.quick else args.trials

    # Output directory: default to `experiment_results/` (gitignored).
    output_dir = Path(args.output_dir) if args.output_dir else Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True, parents=True)

    print("=" * 70)
    print("LP Relay Selection Benchmark")
    print(f"Profile: {args.profile}")
    print(f"Workers: {workers}")
    print(f"Relays: {relays}")
    print(f"Budgets: {budgets}")
    print(f"Trials: {n_trials}")
    print(f"Allow destination relays: {args.allow_destination_relays}")
    print("=" * 70)

    # Run benchmark
    start_time = time.time()
    results = run_full_benchmark(args.profile, workers, relays, budgets, n_trials, allow_destinations_as_relays=args.allow_destination_relays)
    elapsed = time.time() - start_time
    
    print("=" * 70)
    print(f"Completed in {elapsed:.1f} seconds")
    print("=" * 70)
    
    # Save results to JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = output_dir / f"relay_selection_{timestamp}.json"
    
    # Convert to JSON-serializable format
    json_data = {
        "config": {
            "profile": args.profile,
            "workers": workers,
            "relays": relays,
            "budgets": budgets,
            "n_trials": n_trials,
        },
        "results": [
            {
                "n_workers": r.n_workers,
                "n_relays": r.n_relays,
                "budget": r.budget,
                "cf_mean": r.cf_mean,
                "cf_std": r.cf_std,
                "capacity_mean": r.capacity_mean,
                "capacity_std": r.capacity_std,
                "random_mean": r.random_mean,
                "random_std": r.random_std,
                "lp_vs_capacity_pct": r.lp_vs_capacity_pct,
                "lp_vs_random_pct": r.lp_vs_random_pct,
            }
            for r in results
        ],
        "elapsed_seconds": elapsed,
    }
    
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"Results saved to: {json_path}")
    
    # Generate summary
    summary = generate_summary_table(results)
    summary_path = output_dir / f"summary_{timestamp}.md"
    with open(summary_path, "w") as f:
        f.write(summary)
    print(f"Summary saved to: {summary_path}")
    print("\n" + summary)
    
    # Generate plots
    if not args.no_plot and PLOTTING_AVAILABLE:
        print("\nGenerating plots...")
        try:
            plot_budget_comparison(results, output_dir)
            plot_lp_advantage_heatmap(results, output_dir, budget=2)
            plot_completion_time(results, output_dir)
        except Exception as e:
            print(f"Warning: Could not generate plots: {e}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
