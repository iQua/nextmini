"""
LP backend comparison experiment for Skyrocket-style planners.

This script measures the impact of replacing the LP solve in Stage 1 with the
CF-MWU approximation while keeping the Stage 2 rounding (CF-Bottleneck) the same.

Run from the Nextmini repo root as:
  python -m examples.lp.mwu_backend_experiments

Outputs a JSON file under examples/lp/experiment_results/.
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
class BackendTrial:
    trial: int
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
) -> BackendTrial:
    graph = generate_topology(
        n_nodes,
        "full_mesh",
        capacity_range,
        seed=trial * 1000 + n_nodes,
    )

    src = graph.nodes[0]
    terminals = random.sample(graph.nodes[1:], min(n_terminals, len(graph.nodes) - 1))

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
        )
    except Exception as exc:
        result = None
        error = str(exc)

    elapsed_ms = (time.perf_counter() - start) * 1000

    if result is None:
        return BackendTrial(
            trial=trial,
            algorithm=algorithm,
            throughput=None,
            lp_f_star=None,
            computation_time_ms=elapsed_ms,
            error=error,
        )

    return BackendTrial(
        trial=trial,
        algorithm=algorithm,
        throughput=result.throughput,
        lp_f_star=result.lp_f_star,
        computation_time_ms=elapsed_ms,
        error=result.error,
    )


def _summarize(trials: list[BackendTrial], algorithm: str) -> dict[str, Any]:
    rows = [t for t in trials if t.algorithm == algorithm and t.throughput is not None]
    if not rows:
        return {"n_trials": 0}

    throughputs = [t.throughput for t in rows if t.throughput is not None]
    f_stars = [t.lp_f_star for t in rows if t.lp_f_star is not None]
    times = [t.computation_time_ms for t in rows]

    return {
        "n_trials": len(rows),
        "avg_f_tree": statistics.mean(throughputs),
        "std_f_tree": statistics.pstdev(throughputs),
        "avg_f_star": statistics.mean(f_stars) if f_stars else None,
        "std_f_star": statistics.pstdev(f_stars) if f_stars else None,
        "avg_time_ms": statistics.mean(times),
        "std_time_ms": statistics.pstdev(times),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare LP vs MWU backends for CF-Bottleneck.")
    parser.add_argument("--n-trials", type=int, default=50)
    parser.add_argument("--n-nodes", type=int, default=16)
    parser.add_argument("--n-terminals", type=int, default=8)
    parser.add_argument("--hop-limit", type=int, default=3)
    parser.add_argument("--eta", type=float, default=0.1)
    parser.add_argument("--num-paths", type=int, default=2)
    parser.add_argument("--min-capacity", type=float, default=10.0)
    parser.add_argument("--max-capacity", type=float, default=100.0)
    args = parser.parse_args()

    capacity_range = (args.min_capacity, args.max_capacity)

    trials: list[BackendTrial] = []
    for trial in range(args.n_trials):
        for algorithm in ("cf_bottleneck", "cf_bottleneck_mwu"):
            trials.append(
                _run_trial(
                    trial=trial,
                    n_nodes=args.n_nodes,
                    n_terminals=args.n_terminals,
                    hop_limit=args.hop_limit,
                    capacity_range=capacity_range,
                    eta=args.eta,
                    num_paths=args.num_paths,
                    algorithm=algorithm,
                )
            )

    summary = {
        "cf_bottleneck": _summarize(trials, "cf_bottleneck"),
        "cf_bottleneck_mwu": _summarize(trials, "cf_bottleneck_mwu"),
    }

    payload = {
        "args": {
            "n_trials": args.n_trials,
            "n_nodes": args.n_nodes,
            "topology": "full_mesh",
            "n_terminals": args.n_terminals,
            "hop_limit": args.hop_limit,
            "capacity_range": capacity_range,
            "eta": args.eta,
            "num_paths": args.num_paths,
        },
        "results": [asdict(t) for t in trials],
        "summary": summary,
    }

    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"mwu_backend_{timestamp}.json"
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"Saved: {out_path}")
    for algo, stats in summary.items():
        if not stats.get("n_trials"):
            continue
        print(
            f"{algo}: n={stats['n_trials']} "
            f"f_tree={stats['avg_f_tree']:.2f}±{stats['std_f_tree']:.2f} "
            f"f*={stats['avg_f_star']:.2f}±{stats['std_f_star']:.2f} "
            f"time={stats['avg_time_ms']:.2f}±{stats['std_time_ms']:.2f}ms"
        )


if __name__ == "__main__":
    main()
