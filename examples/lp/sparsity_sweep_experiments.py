"""
Overlay sparsity sweep for Stage-1 guidance value.

This experiment varies Erdős–Rényi edge probability p and compares how
Basic-Bottleneck (capacity-only) and CF-Bottleneck (conceptual-flow-guided)
perform on the same synthetic distribution.

Run from the Nextmini repo root as:
  python -m examples.lp.sparsity_sweep_experiments

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
class SparsityTrial:
    trial: int
    edge_probability: float
    algorithm: str
    eta: float
    throughput: float | None
    computation_time_ms: float
    error: str | None


def _parse_ps(arg: str) -> list[float]:
    return [float(x.strip()) for x in arg.split(",") if x.strip()]


def _safe_pstdev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return statistics.pstdev(values)


def _summarize(trials: list[SparsityTrial], algorithm: str, p: float) -> dict[str, Any]:
    rows = [t for t in trials if t.algorithm == algorithm and t.edge_probability == p]
    ok = [t for t in rows if t.throughput is not None]
    throughputs = [t.throughput for t in ok if t.throughput is not None]
    times = [t.computation_time_ms for t in ok]
    return {
        "n_total": len(rows),
        "n_success": len(ok),
        "success_rate": len(ok) / len(rows) if rows else 0.0,
        "avg_f_tree": statistics.mean(throughputs) if throughputs else None,
        "std_f_tree": _safe_pstdev(throughputs) if throughputs else None,
        "avg_time_ms": statistics.mean(times) if times else None,
        "std_time_ms": _safe_pstdev(times) if times else None,
    }


def _run_trial(
    *,
    trial: int,
    edge_probability: float,
    n_nodes: int,
    n_terminals: int,
    hop_limit: int,
    capacity_range: tuple[float, float],
    eta: float,
    num_paths: int,
    algorithm: str,
) -> SparsityTrial:
    seed = trial * 10_000 + n_nodes * 10 + int(round(edge_probability * 1000))
    graph = generate_topology(
        n_nodes,
        "random",
        capacity_range,
        edge_probability=edge_probability,
        seed=seed,
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
        return SparsityTrial(
            trial=trial,
            edge_probability=edge_probability,
            algorithm=algorithm,
            eta=eta,
            throughput=None,
            computation_time_ms=elapsed_ms,
            error=error,
        )

    return SparsityTrial(
        trial=trial,
        edge_probability=edge_probability,
        algorithm=algorithm,
        eta=eta,
        throughput=result.throughput,
        computation_time_ms=elapsed_ms,
        error=result.error,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Overlay sparsity sweep for planner throughput.")
    parser.add_argument("--n-trials", type=int, default=30)
    parser.add_argument("--n-nodes", type=int, default=16)
    parser.add_argument("--n-terminals", type=int, default=8)
    parser.add_argument("--hop-limit", type=int, default=4)
    parser.add_argument("--eta", type=float, default=0.1, help="eta for CF-based methods")
    parser.add_argument("--num-paths", type=int, default=2)
    parser.add_argument("--min-capacity", type=float, default=10.0)
    parser.add_argument("--max-capacity", type=float, default=100.0)
    parser.add_argument("--ps", type=str, default="0.2,0.35,0.5", help="Comma-separated p values.")
    parser.add_argument(
        "--include-mwu",
        action="store_true",
        help="Also run cf_bottleneck_mwu for reference.",
    )
    args = parser.parse_args()

    ps = _parse_ps(args.ps)
    capacity_range = (args.min_capacity, args.max_capacity)

    algorithms: list[tuple[str, float]] = [
        ("basic_bottleneck", 0.0),  # capacity-only baseline
        ("cf_bottleneck", args.eta),
    ]
    if args.include_mwu:
        algorithms.append(("cf_bottleneck_mwu", args.eta))

    trials: list[SparsityTrial] = []
    for trial in range(args.n_trials):
        for p in ps:
            for algorithm, eta in algorithms:
                trials.append(
                    _run_trial(
                        trial=trial,
                        edge_probability=p,
                        n_nodes=args.n_nodes,
                        n_terminals=args.n_terminals,
                        hop_limit=args.hop_limit,
                        capacity_range=capacity_range,
                        eta=eta,
                        num_paths=args.num_paths,
                        algorithm=algorithm,
                    )
                )

    summary: dict[str, dict[float, dict[str, Any]]] = {}
    for algorithm, _eta in algorithms:
        summary[algorithm] = {p: _summarize(trials, algorithm, p) for p in ps}

    payload = {
        "args": {
            "n_trials": args.n_trials,
            "n_nodes": args.n_nodes,
            "topology": "random",
            "edge_probabilities": ps,
            "n_terminals": args.n_terminals,
            "hop_limit": args.hop_limit,
            "capacity_range": capacity_range,
            "eta": args.eta,
            "num_paths": args.num_paths,
            "algorithms": [{"name": a, "eta": e} for a, e in algorithms],
        },
        "results": [asdict(t) for t in trials],
        "summary": summary,
    }

    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"sparsity_sweep_{timestamp}.json"
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"Saved: {out_path}")
    for algorithm, _eta in algorithms:
        print(f"\n{algorithm}:")
        for p in ps:
            s = summary[algorithm][p]
            if s.get("n_total", 0) == 0:
                print(f"  p={p}: no trials")
                continue
            if s.get("avg_f_tree") is None:
                print(f"  p={p}: success={s['n_success']}/{s['n_total']} ({s['success_rate']:.2f})")
                continue
            print(
                f"  p={p}: success={s['n_success']}/{s['n_total']} ({s['success_rate']:.2f}) "
                f"f_tree={s['avg_f_tree']:.2f}±{s['std_f_tree']:.2f} "
                f"time={s['avg_time_ms']:.2f}±{s['std_time_ms']:.2f}ms"
            )


if __name__ == "__main__":
    main()

