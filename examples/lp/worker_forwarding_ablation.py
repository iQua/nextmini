r"""
Worker-Forwarding Ablation Benchmark.

Compares two relay-eligibility modes:
1. Workers-as-Leaves: R = V \ ({s} ∪ T) - workers only receive
2. Worker-Forwarding: R = V \ {s} - workers can also forward

Usage:
    python -m examples.lp.worker_forwarding_ablation --profile=lp_trap --trials=20
    python -m examples.lp.worker_forwarding_ablation --quick  # Fast test with 3 trials
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .graph import Graph
from .solver import compute_tree_edges
from .lp_advantage_bench import PROFILES, build_graph


# =============================================================================
# Configuration
# =============================================================================

WORKERS = [4, 6, 8]
RELAYS = [2, 4, 6]
BUDGETS = [2, 4]
DEFAULT_TRIALS = 20
DEFAULT_PROFILE = "lp_trap"


# =============================================================================
# Result Data Structures
# =============================================================================

@dataclass
class AblationResult:
    """Result comparing two modes for a single configuration."""
    profile: str
    n_workers: int
    n_relays: int
    budget: int
    n_trials: int
    # Workers-as-Leaves mode
    leaves_mean: float
    leaves_std: float
    # Worker-Forwarding mode
    forward_mean: float
    forward_std: float
    # Improvement
    improvement_pct: float


# =============================================================================
# Benchmark Runner
# =============================================================================

def run_single_trial(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    budget: int,
    seed: int,
    allow_dest_relays: bool,
    backend: str = "mwu",
    hop_limit: int = 4,
) -> float:
    """Run a single trial with specified mode."""
    
    rng = random.Random(seed)
    
    n_nodes = 1 + n_workers + n_relays
    src = 1
    terminals = list(range(2, 2 + n_workers))
    relays = list(range(2 + n_workers, 2 + n_workers + n_relays))
    
    # Generate capacity profile
    profile_fn = PROFILES[profile_name]
    capacities = profile_fn(n_nodes, src, terminals, relays, rng)
    graph, src, terminals, relays = build_graph(n_workers, n_relays, capacities)
    
    try:
        if backend == "mwu":
            algorithm = "cf_bottleneck_mwu"
        elif backend == "lp":
            algorithm = "cf_bottleneck"
        else:
            raise ValueError(f"Unknown backend: {backend}")

        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm=algorithm,
            hop_limit=hop_limit,
            eta=0.1,
            max_length=hop_limit,
            num_paths=2,
            relay_nodes=relays,
            max_relays=budget,
            relay_scoring="coverage",
            allow_destinations_as_relays=allow_dest_relays,
        )
        return result.throughput or 0.0
    except Exception:
        return 0.0


def run_config(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    budget: int,
    n_trials: int,
    *,
    backend: str,
    hop_limit: int,
) -> AblationResult:
    """Run ablation comparison for a single configuration."""
    
    leaves_results = []
    forward_results = []
    
    for trial in range(n_trials):
        seed = trial
        
        # Workers-as-Leaves mode
        leaves_tput = run_single_trial(
            profile_name, n_workers, n_relays, budget, seed,
            allow_dest_relays=False,
            backend=backend,
            hop_limit=hop_limit,
        )
        leaves_results.append(leaves_tput)
        
        # Worker-Forwarding mode
        forward_tput = run_single_trial(
            profile_name, n_workers, n_relays, budget, seed,
            allow_dest_relays=True,
            backend=backend,
            hop_limit=hop_limit,
        )
        forward_results.append(forward_tput)
    
    leaves_mean = statistics.mean(leaves_results)
    leaves_std = statistics.stdev(leaves_results) if len(leaves_results) > 1 else 0.0
    forward_mean = statistics.mean(forward_results)
    forward_std = statistics.stdev(forward_results) if len(forward_results) > 1 else 0.0
    
    improvement = ((forward_mean - leaves_mean) / leaves_mean * 100) if leaves_mean > 0 else 0.0
    
    return AblationResult(
        profile=profile_name,
        n_workers=n_workers,
        n_relays=n_relays,
        budget=budget,
        n_trials=n_trials,
        leaves_mean=leaves_mean,
        leaves_std=leaves_std,
        forward_mean=forward_mean,
        forward_std=forward_std,
        improvement_pct=improvement,
    )


def run_full_ablation(
    profile_name: str,
    workers: list[int],
    relays: list[int],
    budgets: list[int],
    n_trials: int,
    *,
    backend: str,
    hop_limit: int,
) -> list[AblationResult]:
    """Run the complete ablation matrix."""
    
    results = []
    configs = [(w, r, b) for w in workers for r in relays for b in budgets if b <= r]
    total = len(configs)
    
    print(f"Running {total} configurations × {n_trials} trials × 2 modes = {total * n_trials * 2} tests", flush=True)
    print("=" * 80, flush=True)
    
    for i, (n_workers, n_relays, budget) in enumerate(configs):
        print(f"[{i+1}/{total}] W={n_workers}, R={n_relays}, B={budget}...", end=" ", flush=True)
        
        result = run_config(
            profile_name,
            n_workers,
            n_relays,
            budget,
            n_trials,
            backend=backend,
            hop_limit=hop_limit,
        )
        results.append(result)
        
        sign = "+" if result.improvement_pct >= 0 else ""
        print(f"Leaves={result.leaves_mean:.1f}±{result.leaves_std:.1f}, "
              f"Forward={result.forward_mean:.1f}±{result.forward_std:.1f}, "
              f"Δ={sign}{result.improvement_pct:.1f}%", flush=True)
    
    return results


def print_summary_table(results: list[AblationResult]) -> str:
    """Generate markdown summary table."""
    
    lines = [
        "# Worker-Forwarding Ablation Results",
        "",
        "## Summary Table",
        "",
        "| Profile | Workers | Relays | Budget | Workers-as-Leaves | Worker-Forwarding | Improvement |",
        "|---------|---------|--------|--------|-------------------|-------------------|-------------|",
    ]
    
    for r in results:
        sign = "+" if r.improvement_pct >= 0 else ""
        bold = "**" if abs(r.improvement_pct) > 5 else ""
        lines.append(
            f"| {r.profile} | {r.n_workers} | {r.n_relays} | {r.budget} | "
            f"{r.leaves_mean:.1f}±{r.leaves_std:.1f} | "
            f"{r.forward_mean:.1f}±{r.forward_std:.1f} | "
            f"{bold}{sign}{r.improvement_pct:.1f}%{bold} |"
        )
    
    # Add aggregate stats
    positive = [r for r in results if r.improvement_pct > 0]
    negative = [r for r in results if r.improvement_pct < 0]
    avg_improvement = statistics.mean(r.improvement_pct for r in results) if results else 0
    
    lines.extend([
        "",
        "## Summary Statistics",
        "",
        f"- **Configurations with improvement**: {len(positive)}/{len(results)}",
        f"- **Configurations with degradation**: {len(negative)}/{len(results)}",
        f"- **Average improvement**: {avg_improvement:+.1f}%",
    ])
    
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Worker-Forwarding Ablation Benchmark")
    parser.add_argument("--profile", type=str, default=DEFAULT_PROFILE,
                        help=f"Capacity profile (default: {DEFAULT_PROFILE})")
    parser.add_argument("--profiles", type=str, default="",
                        help="Comma-separated profiles (overrides --profile)")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS,
                        help=f"Number of trials per config (default: {DEFAULT_TRIALS})")
    parser.add_argument("--backend", type=str, choices=["lp", "mwu"], default="mwu",
                        help="Stage-1 conceptual-flow backend (default: mwu)")
    parser.add_argument("--hop-limit", type=int, default=4,
                        help="Hop limit H (default: 4)")
    parser.add_argument("--workers", type=str, default=",".join(map(str, WORKERS)),
                        help="Comma-separated worker counts")
    parser.add_argument("--relays", type=str, default=",".join(map(str, RELAYS)),
                        help="Comma-separated relay counts")
    parser.add_argument("--budgets", type=str, default=",".join(map(str, BUDGETS)),
                        help="Comma-separated budget values")
    parser.add_argument("--quick", action="store_true",
                        help="Quick test with 3 trials")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory")
    args = parser.parse_args()
    
    # Parse arguments
    workers = [int(x.strip()) for x in args.workers.split(",")]
    relays = [int(x.strip()) for x in args.relays.split(",")]
    budgets = [int(x.strip()) for x in args.budgets.split(",")]
    n_trials = 3 if args.quick else args.trials
    profiles = [x.strip() for x in args.profiles.split(",") if x.strip()] if args.profiles else [args.profile]
    backend = args.backend
    hop_limit = int(args.hop_limit)
    
    # Output directory
    output_dir = Path(args.output_dir) if args.output_dir else Path(__file__).parent / "ablation_results"
    output_dir.mkdir(exist_ok=True)
    
    print("=" * 80, flush=True)
    print("Worker-Forwarding Ablation Benchmark", flush=True)
    print(f"Profiles: {profiles}", flush=True)
    print(f"Workers: {workers}", flush=True)
    print(f"Relays: {relays}", flush=True)
    print(f"Budgets: {budgets}", flush=True)
    print(f"Trials: {n_trials}", flush=True)
    print(f"Backend: {backend}", flush=True)
    print(f"Hop limit: {hop_limit}", flush=True)
    print("=" * 80, flush=True)
    
    all_results = []
    start_time = time.time()
    
    for profile in profiles:
        print(f"\n=== Profile: {profile} ===\n")
        results = run_full_ablation(
            profile,
            workers,
            relays,
            budgets,
            n_trials,
            backend=backend,
            hop_limit=hop_limit,
        )
        all_results.extend(results)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 80)
    print(f"Completed in {elapsed:.1f} seconds")
    print("=" * 80)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # JSON
    json_path = output_dir / f"ablation_{timestamp}.json"
    json_data = {
        "config": {
            "profiles": profiles,
            "workers": workers,
            "relays": relays,
            "budgets": budgets,
            "n_trials": n_trials,
            "backend": backend,
            "hop_limit": hop_limit,
        },
        "results": [
            {
                "profile": r.profile,
                "n_workers": r.n_workers,
                "n_relays": r.n_relays,
                "budget": r.budget,
                "leaves_mean": r.leaves_mean,
                "leaves_std": r.leaves_std,
                "forward_mean": r.forward_mean,
                "forward_std": r.forward_std,
                "improvement_pct": r.improvement_pct,
            }
            for r in all_results
        ],
        "elapsed_seconds": elapsed,
    }
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"Results saved to: {json_path}")
    
    # Markdown summary
    summary = print_summary_table(all_results)
    summary_path = output_dir / f"summary_{timestamp}.md"
    with open(summary_path, "w") as f:
        f.write(summary)
    print(f"Summary saved to: {summary_path}")
    
    print("\n" + summary)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
