"""
Hop limit sensitivity experiments for Skyrocket-style planners.

This script measures how throughput changes with increasing hop limit H
for different planners.

Run from the Nextmini repo root as:
  python -m examples.lp.hop_limit_sensitivity_experiments

  # With workers-as-relays enabled:
  python -m examples.lp.hop_limit_sensitivity_experiments --allow-destination-relays

This runner is also used to generate the hop-sweep artifact used by the paper:
`xindan-icdcs26/evaluation.tex` (`tab:hop-sweep`).

Outputs a JSON file under `examples/lp/experiment_results/`.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:  # Silence GLPK chatter if cvxopt is available.
    from cvxopt import solvers

    solvers.options["glpk"] = {"msg_lev": "GLP_MSG_OFF"}
except Exception:
    pass

from .experiments import generate_topology
from .solver import TreeResult, compute_tree_edges


@dataclass
class HopLimitTrial:
    trial: int
    hop_limit: int
    algorithm: str
    throughput: float | None
    lp_f_star: float | None
    computation_time_ms: float
    error: str | None


def _run_trial(
    *,
    trial: int,
    n_nodes: int,
    n_terminals: int,
    hop_limit: int,
    capacity_range: tuple[float, float],
    eta: float,
    num_paths: int,
    algorithm: str,
    allow_destinations_as_relays: bool = False,
    seed: int | None = None,
) -> HopLimitTrial:
    # Use consistent seed across hop limits for fair comparison
    if seed is None:
        seed = trial * 1000 + n_nodes

    graph = generate_topology(
        n_nodes,
        "full_mesh",
        capacity_range,
        seed=seed,
    )

    src = graph.nodes[0]
    # Use consistent terminal selection across hop limits
    rng = random.Random(seed)
    terminals = rng.sample(graph.nodes[1:], min(n_terminals, len(graph.nodes) - 1))

    start = time.perf_counter()
    result: TreeResult | None
    error: str | None = None
    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm=algorithm,
            hop_limit=hop_limit,
            eta=eta,
            max_length=hop_limit,
            num_paths=num_paths,
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
    except Exception as exc:
        result = None
        error = str(exc)

    elapsed_ms = (time.perf_counter() - start) * 1000

    if result is None:
        return HopLimitTrial(
            trial=trial,
            hop_limit=hop_limit,
            algorithm=algorithm,
            throughput=None,
            lp_f_star=None,
            computation_time_ms=elapsed_ms,
            error=error,
        )

    return HopLimitTrial(
        trial=trial,
        hop_limit=hop_limit,
        algorithm=algorithm,
        throughput=result.throughput,
        lp_f_star=result.lp_f_star,
        computation_time_ms=elapsed_ms,
        error=result.error,
    )


def _summarize_by_hop(trials: list[HopLimitTrial], algorithm: str, hop_limit: int) -> dict[str, Any]:
    rows = [t for t in trials if t.algorithm == algorithm and t.hop_limit == hop_limit and t.throughput is not None]
    if not rows:
        return {"n_trials": 0, "hop_limit": hop_limit}

    throughputs = [t.throughput for t in rows if t.throughput is not None]
    times = [t.computation_time_ms for t in rows]

    return {
        "hop_limit": hop_limit,
        "n_trials": len(rows),
        "avg_f_tree": statistics.mean(throughputs),
        "std_f_tree": statistics.pstdev(throughputs) if len(throughputs) > 1 else 0.0,
        "avg_time_ms": statistics.mean(times),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Hop limit sensitivity experiments.")
    parser.add_argument("--n-trials", type=int, default=20, help="Number of trials per hop limit")
    parser.add_argument("--n-nodes", type=int, default=16, help="Number of nodes")
    parser.add_argument("--n-terminals", type=int, default=8, help="Number of terminals")
    parser.add_argument("--hop-limits", type=str, default="2,4,6,8", help="Comma-separated hop limits to test")
    parser.add_argument(
        "--algorithms",
        type=str,
        default="basic_tree,cf_bottleneck",
        help="Comma-separated algorithms to test (e.g., basic_tree,cf_bottleneck)",
    )
    parser.add_argument("--eta", type=float, default=0.1, help="LP weight parameter")
    parser.add_argument("--num-paths", type=int, default=2, help="K candidate paths per terminal")
    parser.add_argument("--min-capacity", type=float, default=10.0)
    parser.add_argument("--max-capacity", type=float, default=100.0)
    parser.add_argument(
        "--allow-destination-relays",
        action="store_true",
        help="Allow destination nodes to forward (workers-as-relays)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for results",
    )
    args = parser.parse_args()

    hop_limits = [int(h.strip()) for h in args.hop_limits.split(",")]
    algorithms = [a.strip() for a in args.algorithms.split(",") if a.strip()]
    if not algorithms:
        raise SystemExit("--algorithms must contain at least one entry")
    capacity_range = (args.min_capacity, args.max_capacity)

    print("=" * 70)
    print("Hop Limit Sensitivity Experiments")
    print("=" * 70)
    print(f"Nodes: {args.n_nodes}")
    print(f"Terminals: {args.n_terminals}")
    print(f"Hop limits: {hop_limits}")
    print(f"Algorithms: {algorithms}")
    print(f"Trials per hop limit: {args.n_trials}")
    print(f"Workers-as-relays: {args.allow_destination_relays}")
    print("=" * 70)

    trials: list[HopLimitTrial] = []

    for hop_limit in hop_limits:
        print(f"\nTesting H={hop_limit}...")
        for trial in range(args.n_trials):
            # Use same seed for all algorithms at same trial/hop_limit for fair comparison
            seed = trial * 1000 + args.n_nodes
            for algorithm in algorithms:
                t = _run_trial(
                    trial=trial,
                    n_nodes=args.n_nodes,
                    n_terminals=args.n_terminals,
                    hop_limit=hop_limit,
                    capacity_range=capacity_range,
                    eta=args.eta,
                    num_paths=args.num_paths,
                    algorithm=algorithm,
                    allow_destinations_as_relays=args.allow_destination_relays,
                    seed=seed,
                )
                trials.append(t)

        # Print interim results for this hop limit
        for algo in algorithms:
            summary = _summarize_by_hop(trials, algo, hop_limit)
            if summary["n_trials"] > 0:
                print(f"  {algo}: {summary['avg_f_tree']:.2f} ± {summary['std_f_tree']:.2f}")

    # Build summary
    summary = {}
    for algo in algorithms:
        summary[algo] = [_summarize_by_hop(trials, algo, h) for h in hop_limits]

    # Print final table
    print("\n" + "=" * 70)
    print("Summary Table")
    print("=" * 70)

    # Header
    header = "| H |"
    for algo in algorithms:
        header += f" {algo:^20} |"
    print(header)
    print("|" + "-" * 3 + "|" + (("-" * 22 + "|") * len(algorithms)))

    # Rows
    for hop_limit in hop_limits:
        row = f"| {hop_limit} |"
        for algo in algorithms:
            s = _summarize_by_hop(trials, algo, hop_limit)
            if s["n_trials"] > 0:
                row += f" {s['avg_f_tree']:6.2f} ± {s['std_f_tree']:5.2f}  |"
            else:
                row += f" {'N/A':^20} |"
        print(row)

    # Save results
    payload = {
        "config": {
            "n_trials": args.n_trials,
            "n_nodes": args.n_nodes,
            "topology": "full_mesh",
            "n_terminals": args.n_terminals,
            "hop_limits": hop_limits,
            "capacity_range": capacity_range,
            "eta": args.eta,
            "num_paths": args.num_paths,
            "allow_destinations_as_relays": args.allow_destination_relays,
        },
        "results": [asdict(t) for t in trials],
        "summary": summary,
    }

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True, parents=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    relay_mode = "worker_relays_on" if args.allow_destination_relays else "worker_relays_off"
    out_path = output_dir / f"hop_sweep_n{args.n_nodes}_t{args.n_terminals}_{relay_mode}_{timestamp}.json"
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
