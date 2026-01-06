"""
Relay-budget experiments for Skyrocket-style planners.

This script evaluates how multicast throughput changes as the allowed relay set
shrinks (a simple proxy for relay budgeting). It compares:
  - CF-RelaySelect (path-flow scoring) + CF-Bottleneck planning
  - CF-RelaySelect (coverage scoring) + CF-Bottleneck planning
  - Incident-edge scoring + CF-Bottleneck planning
  - A non-LP capacity heuristic (top relays by outgoing capacity)
  - Random relay subsets (averaged over samples)

Run from the Nextmini repo root as:
  python -m examples.lp.relay_budget_experiments
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .experiments import generate_topology
from .solver import Graph, TreeResult, compute_tree_edges


@dataclass
class RelayBudgetPoint:
    config_name: str
    trial: int
    budget: int
    method: str
    throughput: float | None
    hop_limit: int
    n_nodes: int
    n_terminals: int


def _capacity_score_relays(graph: Graph, relay_candidates: list[int]) -> dict[int, float]:
    return {
        node: sum(cap for _, cap in graph.adj.get(node, []))
        for node in relay_candidates
    }


def _run_plan(
    graph: Graph,
    *,
    src: int,
    terminals: list[int],
    hop_limit: int,
    eta: float,
    relay_nodes: list[int] | None,
    max_relays: int | None,
    relay_scoring: str,
    num_paths: int,
) -> TreeResult | None:
    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm="cf_bottleneck",
            hop_limit=hop_limit,
            eta=eta,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            max_length=hop_limit,
            num_paths=num_paths,
        )
    except Exception:
        return None
    return result if result.edges else None


def run_relay_budget_suite(
    *,
    config_name: str,
    n_nodes: int,
    topology: str,
    n_terminals: int,
    hop_limit: int,
    capacity_range: tuple[float, float],
    budgets: list[int],
    eta: float,
    num_paths: int,
    n_trials: int,
    n_random_samples: int,
    seed: int,
) -> list[RelayBudgetPoint]:
    rng = random.Random(seed)
    points: list[RelayBudgetPoint] = []

    for trial in range(n_trials):
        graph = generate_topology(
            n_nodes,
            topology,
            capacity_range,
            seed=trial * 1000 + n_nodes,
        )
        src = graph.nodes[0]
        terminals = rng.sample(graph.nodes[1:], min(n_terminals, len(graph.nodes) - 1))

        terminal_set = set(terminals)
        relay_candidates = [n for n in graph.nodes if n != src and n not in terminal_set]

        cap_scores = _capacity_score_relays(graph, relay_candidates)
        cap_ranked = [n for n, _ in sorted(cap_scores.items(), key=lambda kv: kv[1], reverse=True)]

        for budget in budgets:
            k = max(0, min(budget, len(relay_candidates)))

            # CF-RelaySelect (LP path-flow score)
            res = _run_plan(
                graph,
                src=src,
                terminals=terminals,
                hop_limit=hop_limit,
                eta=eta,
                relay_nodes=None,
                max_relays=k,
                relay_scoring="path_flow",
                num_paths=num_paths,
            )
            points.append(
                RelayBudgetPoint(
                    config_name=config_name,
                    trial=trial,
                    budget=k,
                    method="cf_path_flow",
                    throughput=res.throughput if res else None,
                    hop_limit=hop_limit,
                    n_nodes=n_nodes,
                    n_terminals=len(terminals),
                )
            )

            # CF-RelaySelect (terminal coverage with diminishing returns)
            res = _run_plan(
                graph,
                src=src,
                terminals=terminals,
                hop_limit=hop_limit,
                eta=eta,
                relay_nodes=None,
                max_relays=k,
                relay_scoring="coverage",
                num_paths=num_paths,
            )
            points.append(
                RelayBudgetPoint(
                    config_name=config_name,
                    trial=trial,
                    budget=k,
                    method="cf_coverage",
                    throughput=res.throughput if res else None,
                    hop_limit=hop_limit,
                    n_nodes=n_nodes,
                    n_terminals=len(terminals),
                )
            )

            # LP incident-edge score
            res = _run_plan(
                graph,
                src=src,
                terminals=terminals,
                hop_limit=hop_limit,
                eta=eta,
                relay_nodes=None,
                max_relays=k,
                relay_scoring="incident",
                num_paths=num_paths,
            )
            points.append(
                RelayBudgetPoint(
                    config_name=config_name,
                    trial=trial,
                    budget=k,
                    method="cf_incident",
                    throughput=res.throughput if res else None,
                    hop_limit=hop_limit,
                    n_nodes=n_nodes,
                    n_terminals=len(terminals),
                )
            )

            # Capacity heuristic: top-k outgoing capacity relays, then run CF-Bottleneck
            fixed_relays = cap_ranked[:k]
            res = _run_plan(
                graph,
                src=src,
                terminals=terminals,
                hop_limit=hop_limit,
                eta=eta,
                relay_nodes=fixed_relays,
                max_relays=None,
                relay_scoring="path_flow",
                num_paths=num_paths,
            )
            points.append(
                RelayBudgetPoint(
                    config_name=config_name,
                    trial=trial,
                    budget=k,
                    method="cap_heuristic",
                    throughput=res.throughput if res else None,
                    hop_limit=hop_limit,
                    n_nodes=n_nodes,
                    n_terminals=len(terminals),
                )
            )

            # Random relay subsets, averaged over multiple samples
            random_tputs: list[float] = []
            for _ in range(n_random_samples):
                subset = rng.sample(relay_candidates, k) if k else []
                res = _run_plan(
                    graph,
                    src=src,
                    terminals=terminals,
                    hop_limit=hop_limit,
                    eta=eta,
                    relay_nodes=subset,
                    max_relays=None,
                    relay_scoring="path_flow",
                    num_paths=num_paths,
                )
                if res and res.throughput is not None:
                    random_tputs.append(res.throughput)

            points.append(
                RelayBudgetPoint(
                    config_name=config_name,
                    trial=trial,
                    budget=k,
                    method="random",
                    throughput=statistics.mean(random_tputs) if random_tputs else None,
                    hop_limit=hop_limit,
                    n_nodes=n_nodes,
                    n_terminals=len(terminals),
                )
            )

    return points


def summarize(points: list[RelayBudgetPoint]) -> dict:
    by_key: dict[tuple[int, str], list[float]] = {}
    for p in points:
        if p.throughput is None:
            continue
        by_key.setdefault((p.budget, p.method), []).append(p.throughput)

    budgets = sorted({b for b, _ in by_key})
    methods = sorted({m for _, m in by_key})

    summary: dict = {"budgets": budgets, "methods": methods, "stats": {}}
    for budget in budgets:
        for method in methods:
            vals = by_key.get((budget, method), [])
            if not vals:
                continue
            summary["stats"][f"{method}@{budget}"] = {
                "n": len(vals),
                "mean": statistics.mean(vals),
                "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
            }
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Relay budget sweep experiments")
    parser.add_argument("--n-nodes", type=int, default=16)
    parser.add_argument("--topology", type=str, default="full_mesh")
    parser.add_argument("--n-terminals", type=int, default=8)
    parser.add_argument("--hop-limit", type=int, default=3)
    parser.add_argument("--eta", type=float, default=0.1)
    parser.add_argument("--num-paths", type=int, default=2)
    parser.add_argument("--n-trials", type=int, default=20)
    parser.add_argument("--n-random-samples", type=int, default=10)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--budgets",
        type=str,
        default="0,2,4,6",
        help="Comma-separated relay budgets (max relays).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    budgets = [int(x.strip()) for x in args.budgets.split(",") if x.strip()]
    budgets = sorted({b for b in budgets if b >= 0})
    if not budgets:
        raise SystemExit("--budgets must be non-empty")

    points = run_relay_budget_suite(
        config_name=f"relay_budget_{args.topology}",
        n_nodes=args.n_nodes,
        topology=args.topology,
        n_terminals=args.n_terminals,
        hop_limit=args.hop_limit,
        capacity_range=(10.0, 100.0),
        budgets=budgets,
        eta=args.eta,
        num_paths=args.num_paths,
        n_trials=args.n_trials,
        n_random_samples=args.n_random_samples,
        seed=args.seed,
    )
    summary = summarize(points)

    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"relay_budget_{timestamp}.json"
    out_path.write_text(
        json.dumps(
            {
                "args": vars(args),
                "results": [asdict(p) for p in points],
                "summary": summary,
            },
            indent=2,
        )
    )

    print(f"Saved: {out_path}")
    for k, v in sorted(summary["stats"].items()):
        print(f"{k}: n={v['n']} mean={v['mean']:.2f} stdev={v['stdev']:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
