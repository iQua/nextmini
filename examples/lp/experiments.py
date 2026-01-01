"""
Experiments for CF-Tree Algorithm Evaluation.

This script generates experimental data for comparing:
1. Basic-Tree (capacity-based, no LP guidance)
2. CF-Tree (LP-guided)
3. mFlow (original LP-based tree extraction)

Metrics:
- Tree throughput (f_tree)
- LP upper bound gap (f* - f_tree)
- Number of tree edges
- Computation time
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from .graph import Graph
from .solver import compute_tree_edges, TreeResult


@dataclass
class ExperimentConfig:
    """Configuration for a single experiment."""
    name: str
    n_nodes: int
    topology: str  # "full_mesh", "ring", "random", "fat_tree"
    n_terminals: int
    hop_limit: int
    capacity_range: tuple[float, float]
    eta_values: list[float]
    n_trials: int


@dataclass
class ExperimentResult:
    """Result from a single trial."""
    config_name: str
    trial: int
    algorithm: str
    throughput: float | None
    lp_upper_bound: float | None
    n_edges: int
    computation_time_ms: float
    eta: float | None
    hop_limit: int
    n_terminals: int
    n_nodes: int


def generate_topology(
    n_nodes: int,
    topology: str,
    capacity_range: tuple[float, float],
    edge_probability: float = 0.5,
    seed: int | None = None,
) -> Graph:
    """Generate a test topology.

    Args:
        n_nodes: Number of nodes
        topology: Type of topology
        capacity_range: (min_capacity, max_capacity)
        edge_probability: For random topologies
        seed: Random seed

    Returns:
        Graph instance
    """
    if seed is not None:
        random.seed(seed)

    nodes = list(range(1, n_nodes + 1))
    edges = []
    capacities = {}

    min_cap, max_cap = capacity_range

    if topology == "full_mesh":
        for i in nodes:
            for j in nodes:
                if i != j:
                    edges.append((i, j))
                    capacities[(i, j)] = random.uniform(min_cap, max_cap)

    elif topology == "ring":
        for i in range(n_nodes):
            u = nodes[i]
            v = nodes[(i + 1) % n_nodes]
            edges.extend([(u, v), (v, u)])
            cap = random.uniform(min_cap, max_cap)
            capacities[(u, v)] = cap
            capacities[(v, u)] = cap

    elif topology == "random":
        for i in nodes:
            for j in nodes:
                if i < j and random.random() < edge_probability:
                    cap = random.uniform(min_cap, max_cap)
                    edges.extend([(i, j), (j, i)])
                    capacities[(i, j)] = cap
                    capacities[(j, i)] = cap

    elif topology == "star":
        # Hub is node 1
        hub = nodes[0]
        for spoke in nodes[1:]:
            cap = random.uniform(min_cap, max_cap)
            edges.extend([(hub, spoke), (spoke, hub)])
            capacities[(hub, spoke)] = cap
            capacities[(spoke, hub)] = cap

    elif topology == "tree":
        # Binary tree
        for i in range(1, n_nodes):
            parent = (i - 1) // 2 + 1
            child = i + 1
            if parent <= n_nodes and child <= n_nodes:
                cap = random.uniform(min_cap, max_cap)
                edges.extend([(parent, child), (child, parent)])
                capacities[(parent, child)] = cap
                capacities[(child, parent)] = cap

    else:
        raise ValueError(f"Unknown topology: {topology}")

    return Graph(nodes, edges, capacities)


def run_single_trial(
    graph: Graph,
    src: int,
    terminals: list[int],
    algorithm: str,
    hop_limit: int,
    eta: float = 0.1,
) -> tuple[TreeResult | None, float]:
    """Run a single trial.

    Returns:
        (result, computation_time_ms)
    """
    start = time.perf_counter()

    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm=algorithm,
            hop_limit=hop_limit,
            eta=eta,
        )
        elapsed = (time.perf_counter() - start) * 1000
        return result, elapsed
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        print(f"  Trial failed ({algorithm}): {e}")
        return None, elapsed


def run_experiment(config: ExperimentConfig) -> list[ExperimentResult]:
    """Run all trials for an experiment configuration."""
    results = []

    print(f"\n{'='*60}")
    print(f"Experiment: {config.name}")
    print(f"  Nodes: {config.n_nodes}, Topology: {config.topology}")
    print(f"  Terminals: {config.n_terminals}, Hop limit: {config.hop_limit}")
    print(f"{'='*60}")

    for trial in range(config.n_trials):
        # Generate topology with trial as seed for reproducibility
        graph = generate_topology(
            config.n_nodes,
            config.topology,
            config.capacity_range,
            seed=trial * 1000 + config.n_nodes,
        )

        # Select source and terminals
        src = graph.nodes[0]
        terminals = random.sample(graph.nodes[1:], min(config.n_terminals, len(graph.nodes) - 1))

        print(f"\nTrial {trial + 1}/{config.n_trials}: src={src}, terminals={terminals}")

        # Test Basic-Tree
        result, time_ms = run_single_trial(
            graph, src, terminals, "basic_tree", config.hop_limit
        )
        if result:
            results.append(ExperimentResult(
                config_name=config.name,
                trial=trial,
                algorithm="basic_tree",
                throughput=result.throughput,
                lp_upper_bound=result.lp_upper_bound,
                n_edges=len(result.edges),
                computation_time_ms=time_ms,
                eta=None,
                hop_limit=config.hop_limit,
                n_terminals=len(terminals),
                n_nodes=config.n_nodes,
            ))
            print(f"  basic_tree: throughput={result.throughput:.2f}, edges={len(result.edges)}, time={time_ms:.2f}ms")

        # Test CF-Tree with different eta values
        for eta in config.eta_values:
            result, time_ms = run_single_trial(
                graph, src, terminals, "cf_tree", config.hop_limit, eta=eta
            )
            if result:
                results.append(ExperimentResult(
                    config_name=config.name,
                    trial=trial,
                    algorithm=f"cf_tree",
                    throughput=result.throughput,
                    lp_upper_bound=result.lp_upper_bound,
                    n_edges=len(result.edges),
                    computation_time_ms=time_ms,
                    eta=eta,
                    hop_limit=config.hop_limit,
                    n_terminals=len(terminals),
                    n_nodes=config.n_nodes,
                ))
                gap = (result.lp_upper_bound - result.throughput) / result.lp_upper_bound * 100 if result.lp_upper_bound else 0
                print(f"  cf_tree (eta={eta}): throughput={result.throughput:.2f}, LP_bound={result.lp_upper_bound:.2f}, gap={gap:.1f}%, time={time_ms:.2f}ms")

        # Test mFlow
        result, time_ms = run_single_trial(
            graph, src, terminals, "mflow", config.hop_limit
        )
        if result:
            results.append(ExperimentResult(
                config_name=config.name,
                trial=trial,
                algorithm="mflow",
                throughput=result.throughput,
                lp_upper_bound=result.lp_upper_bound,
                n_edges=len(result.edges),
                computation_time_ms=time_ms,
                eta=None,
                hop_limit=config.hop_limit,
                n_terminals=len(terminals),
                n_nodes=config.n_nodes,
            ))
            print(f"  mflow: throughput={result.throughput:.2f}, edges={len(result.edges)}, time={time_ms:.2f}ms")

    return results


def summarize_results(results: list[ExperimentResult]) -> dict[str, Any]:
    """Summarize experiment results."""
    from collections import defaultdict

    # Group by algorithm
    by_algo: dict[str, list[ExperimentResult]] = defaultdict(list)
    for r in results:
        key = f"{r.algorithm}" + (f"_eta{r.eta}" if r.eta is not None else "")
        by_algo[key].append(r)

    summary = {}
    for algo, algo_results in by_algo.items():
        throughputs = [r.throughput for r in algo_results if r.throughput]
        times = [r.computation_time_ms for r in algo_results]
        edges = [r.n_edges for r in algo_results]

        lp_bounds = [r.lp_upper_bound for r in algo_results if r.lp_upper_bound]
        if throughputs and lp_bounds:
            gaps = [(b - t) / b * 100 for t, b in zip(throughputs, lp_bounds)]
        else:
            gaps = []

        summary[algo] = {
            "n_trials": len(algo_results),
            "avg_throughput": sum(throughputs) / len(throughputs) if throughputs else 0,
            "avg_time_ms": sum(times) / len(times) if times else 0,
            "avg_edges": sum(edges) / len(edges) if edges else 0,
            "avg_gap_percent": sum(gaps) / len(gaps) if gaps else None,
        }

    return summary


def run_standard_experiments() -> list[ExperimentResult]:
    """Run standard experiment suite."""
    configs = [
        # Small networks
        ExperimentConfig(
            name="small_full_mesh",
            n_nodes=8,
            topology="full_mesh",
            n_terminals=4,
            hop_limit=3,
            capacity_range=(10.0, 100.0),
            eta_values=[0.0, 0.05, 0.1, 0.2],
            n_trials=5,
        ),
        # Medium networks
        ExperimentConfig(
            name="medium_full_mesh",
            n_nodes=16,
            topology="full_mesh",
            n_terminals=8,
            hop_limit=3,
            capacity_range=(10.0, 100.0),
            eta_values=[0.0, 0.1, 0.2],
            n_trials=3,
        ),
        # Random topologies
        ExperimentConfig(
            name="random_sparse",
            n_nodes=12,
            topology="random",
            n_terminals=5,
            hop_limit=4,
            capacity_range=(10.0, 100.0),
            eta_values=[0.0, 0.1],
            n_trials=5,
        ),
        # Hop limit sensitivity
        ExperimentConfig(
            name="hop_limit_2",
            n_nodes=10,
            topology="full_mesh",
            n_terminals=5,
            hop_limit=2,
            capacity_range=(10.0, 100.0),
            eta_values=[0.1],
            n_trials=5,
        ),
        ExperimentConfig(
            name="hop_limit_4",
            n_nodes=10,
            topology="full_mesh",
            n_terminals=5,
            hop_limit=4,
            capacity_range=(10.0, 100.0),
            eta_values=[0.1],
            n_trials=5,
        ),
    ]

    all_results = []
    for config in configs:
        results = run_experiment(config)
        all_results.extend(results)

    return all_results


def main():
    """Main entry point for experiments."""
    print("=" * 60)
    print("CF-Tree Algorithm Experiments")
    print("=" * 60)

    all_results = run_standard_experiments()

    # Summarize
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    summary = summarize_results(all_results)
    for algo, stats in summary.items():
        print(f"\n{algo}:")
        print(f"  Trials: {stats['n_trials']}")
        print(f"  Avg Throughput: {stats['avg_throughput']:.2f}")
        print(f"  Avg Time: {stats['avg_time_ms']:.2f}ms")
        print(f"  Avg Edges: {stats['avg_edges']:.1f}")
        if stats['avg_gap_percent'] is not None:
            print(f"  Avg Gap from LP bound: {stats['avg_gap_percent']:.1f}%")

    # Save results to JSON
    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"results_{timestamp}.json"

    with open(output_file, "w") as f:
        json.dump({
            "results": [asdict(r) for r in all_results],
            "summary": summary,
        }, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
