"""
Candidate-path budget (K) sensitivity for Stage-1 conceptual-flow planning.

This experiment sweeps the number of candidate paths per terminal (K) and
measures how it impacts:
  - achieved tree throughput (f_tree),
  - conceptual-flow estimate (f*),
  - deviation to f*,
  - end-to-end planner time.

Run from the Nextmini repo root as:
  python -m examples.lp.k_sensitivity_experiments

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
from typing import Any, Iterable

try:  # Silence GLPK chatter if cvxopt is available.
    from cvxopt import solvers

    solvers.options["glpk"] = {"msg_lev": "GLP_MSG_OFF"}
except Exception:
    pass

from .experiments import generate_topology
from .solver import TreeResult, compute_tree_edges


@dataclass
class KTrial:
    trial: int
    algorithm: str
    num_paths: int
    throughput: float | None
    lp_f_star: float | None
    computation_time_ms: float
    error: str | None


def _iter_ks(arg: str) -> list[int]:
    return [int(x.strip()) for x in arg.split(",") if x.strip()]


def _safe_pstdev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return statistics.pstdev(values)


def _summarize(trials: list[KTrial], algorithm: str, num_paths: int) -> dict[str, Any]:
    rows = [
        t
        for t in trials
        if t.algorithm == algorithm and t.num_paths == num_paths and t.throughput is not None
    ]
    if not rows:
        return {"n_trials": 0}

    throughputs = [t.throughput for t in rows if t.throughput is not None]
    f_stars = [t.lp_f_star for t in rows if t.lp_f_star is not None and t.lp_f_star > 0]
    times = [t.computation_time_ms for t in rows]
    dev_pcts: list[float] = []
    for t in rows:
        if t.throughput is None or t.lp_f_star is None or t.lp_f_star <= 0:
            continue
        dev_pcts.append((t.lp_f_star - t.throughput) / t.lp_f_star * 100.0)

    return {
        "n_trials": len(rows),
        "avg_f_tree": statistics.mean(throughputs),
        "std_f_tree": _safe_pstdev(throughputs),
        "avg_f_star": statistics.mean(f_stars) if f_stars else None,
        "std_f_star": _safe_pstdev(f_stars) if f_stars else None,
        "avg_dev_pct": statistics.mean(dev_pcts) if dev_pcts else None,
        "std_dev_pct": _safe_pstdev(dev_pcts) if dev_pcts else None,
        "avg_time_ms": statistics.mean(times),
        "std_time_ms": _safe_pstdev(times),
    }


def _run_trial(
    *,
    trial: int,
    n_nodes: int,
    n_terminals: int,
    hop_limit: int,
    topology: str,
    edge_probability: float,
    capacity_range: tuple[float, float],
    eta: float,
    num_paths: int,
    algorithm: str,
) -> KTrial:
    graph = generate_topology(
        n_nodes,
        topology,
        capacity_range,
        edge_probability=edge_probability,
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
        return KTrial(
            trial=trial,
            algorithm=algorithm,
            num_paths=num_paths,
            throughput=None,
            lp_f_star=None,
            computation_time_ms=elapsed_ms,
            error=error,
        )

    return KTrial(
        trial=trial,
        algorithm=algorithm,
        num_paths=num_paths,
        throughput=result.throughput,
        lp_f_star=result.lp_f_star,
        computation_time_ms=elapsed_ms,
        error=result.error,
    )


def _print_summary(
    *,
    algorithms: Iterable[str],
    ks: list[int],
    summary: dict[str, dict[int, dict[str, Any]]],
) -> None:
    for algo in algorithms:
        print(f"\n{algo}:")
        for k in ks:
            s = summary[algo][k]
            if not s.get("n_trials"):
                print(f"  K={k}: no successful trials")
                continue
            dev = (
                f"{s['avg_dev_pct']:.1f}±{s['std_dev_pct']:.1f}%"
                if s.get("avg_dev_pct") is not None
                else "NA"
            )
            print(
                f"  K={k}: n={s['n_trials']} f_tree={s['avg_f_tree']:.2f}±{s['std_f_tree']:.2f} "
                f"f*={s['avg_f_star']:.2f}±{s['std_f_star']:.2f} dev={dev} "
                f"time={s['avg_time_ms']:.2f}±{s['std_time_ms']:.2f}ms"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="K-sensitivity sweep for CF-Bottleneck planners.")
    parser.add_argument("--n-trials", type=int, default=30)
    parser.add_argument("--n-nodes", type=int, default=16)
    parser.add_argument("--n-terminals", type=int, default=8)
    parser.add_argument("--hop-limit", type=int, default=4)
    parser.add_argument("--eta", type=float, default=0.1)
    parser.add_argument("--topology", type=str, default="random", choices=("random", "full_mesh"))
    parser.add_argument("--edge-probability", type=float, default=0.5)
    parser.add_argument("--min-capacity", type=float, default=10.0)
    parser.add_argument("--max-capacity", type=float, default=100.0)
    parser.add_argument("--ks", type=str, default="1,2,4,8", help="Comma-separated K values.")
    parser.add_argument(
        "--algorithms",
        type=str,
        default="cf_bottleneck,cf_bottleneck_mwu",
        help="Comma-separated algorithms to test.",
    )
    args = parser.parse_args()

    ks = _iter_ks(args.ks)
    algorithms = [x.strip() for x in args.algorithms.split(",") if x.strip()]
    capacity_range = (args.min_capacity, args.max_capacity)

    trials: list[KTrial] = []
    for trial in range(args.n_trials):
        for k in ks:
            for algorithm in algorithms:
                trials.append(
                    _run_trial(
                        trial=trial,
                        n_nodes=args.n_nodes,
                        n_terminals=args.n_terminals,
                        hop_limit=args.hop_limit,
                        topology=args.topology,
                        edge_probability=args.edge_probability,
                        capacity_range=capacity_range,
                        eta=args.eta,
                        num_paths=k,
                        algorithm=algorithm,
                    )
                )

    summary: dict[str, dict[int, dict[str, Any]]] = {}
    for algorithm in algorithms:
        summary[algorithm] = {k: _summarize(trials, algorithm, k) for k in ks}

    payload = {
        "args": {
            "n_trials": args.n_trials,
            "n_nodes": args.n_nodes,
            "topology": args.topology,
            "edge_probability": args.edge_probability if args.topology == "random" else None,
            "n_terminals": args.n_terminals,
            "hop_limit": args.hop_limit,
            "capacity_range": capacity_range,
            "eta": args.eta,
            "ks": ks,
            "algorithms": algorithms,
        },
        "results": [asdict(t) for t in trials],
        "summary": summary,
    }

    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"k_sensitivity_{timestamp}.json"
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"Saved: {out_path}")
    _print_summary(algorithms=algorithms, ks=ks, summary=summary)


if __name__ == "__main__":
    main()
