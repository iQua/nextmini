"""
LP-Advantage Benchmark Suite for Multicast Planners.

This script evaluates all broadcast algorithms with manually designed capacity profiles
to identify scenarios where LP-based planners outperform baselines.

Capacity range: 30 Mbps ~ 3000 Mbps (2 orders of magnitude)

Run from the Nextmini repo root as:
    python -m examples.lp.lp_advantage_bench

Usage:
    # Full benchmark (all worker/relay combinations)
    python -m examples.lp.lp_advantage_bench --all

    # Specific configuration
    python -m examples.lp.lp_advantage_bench --workers=4 --relays=4

    # Single profile test
    python -m examples.lp.lp_advantage_bench --workers=4 --relays=4 --profiles=hierarchy_weak_direct
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import statistics
import time
from datetime import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from .graph import Graph
from .solver import TreeResult, compute_tree_edges

# Constants
ARTIFACT_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
CAPACITY_MIN = 30.0    # 30 Mbps
CAPACITY_MAX = 3000.0  # 3000 Mbps (3 Gbps)

ALGORITHMS = [
    "star",
    "two_level",
    "basic_tree",
    "basic_bottleneck",
    "cf_tree",
    "cf_tree_mwu",
    "cf_bottleneck",
    "cf_bottleneck_mwu",
]

OLD_ALGORITHMS = [
    "star",
    "two_level",
    "basic_bottleneck",
    "cf_bottleneck",
    "cf_bottleneck_mwu",
]

# Relay selection methods for budget testing
RELAY_METHODS = [
    ("cf_bottleneck", "coverage"),      # LP-based coverage scoring
    ("cf_bottleneck", "path_flow"),     # LP-based path flow scoring  
    ("basic_bottleneck", "capacity"),   # Capacity heuristic (external selection)
    ("basic_bottleneck", "random"),     # Random selection (external selection)
]


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""
    profile: str
    n_workers: int
    n_relays: int
    algorithm: str
    throughput_mbps: float | None
    completion_time_sec: float | None
    lp_f_star: float | None
    tree_edges: int
    error: str | None
    max_relays: int | None = None
    relay_scoring: str | None = None


def mbps_to_completion_sec(throughput_mbps: float | None) -> float | None:
    """Convert throughput to completion time for 10GB artifact."""
    if throughput_mbps is None or throughput_mbps <= 0:
        return None
    # 10 GB = 10 * 1024 * 1024 * 1024 * 8 bits = 85899345920 bits
    # throughput_mbps = Mbps = 10^6 bits/sec
    bits = ARTIFACT_SIZE_BYTES * 8
    return bits / (throughput_mbps * 1e6)


# =============================================================================
# Capacity Profile Generators
# =============================================================================

def make_hierarchy_weak_direct(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    Hierarchy profile with weak direct links (`hierarchy_weak_direct`).
    
    Design:
    - Direct src→terminal links: WEAK (30-100 Mbps)
    - src→relay links: STRONG (1000-3000 Mbps)
    - relay→terminal links: STRONG (500-2000 Mbps)
    - Other links: MEDIUM (100-500 Mbps)
    
    LP advantage: Finds multi-hop relay paths that capacity heuristic may miss.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))
    
    for u in all_nodes:
        for v in all_nodes:
            if u == v:
                continue
            
            # Direct src→terminal: WEAK
            if u == src and v in terminals:
                caps[(u, v)] = rng.uniform(30.0, 100.0)
            # Direct terminal→src: symmetric
            elif v == src and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(30.0, 100.0))
            # src→relay: STRONG
            elif u == src and v in relays:
                caps[(u, v)] = rng.uniform(1000.0, 3000.0)
            elif v == src and u in relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(1000.0, 3000.0))
            # relay→terminal: STRONG
            elif u in relays and v in terminals:
                caps[(u, v)] = rng.uniform(500.0, 2000.0)
            elif v in relays and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(500.0, 2000.0))
            # relay→relay: MEDIUM-STRONG
            elif u in relays and v in relays:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(300.0, 1500.0)
                caps[(u, v)] = caps[key]
            # Other: MEDIUM
            else:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(100.0, 500.0)
                caps[(u, v)] = caps[key]
    
    return caps


def make_bottleneck_trap(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    Bottleneck trap profile for capacity heuristic (`bottleneck_trap`).
    
    Design:
    - One "decoy" relay has very high src→relay capacity (2500-3000)
      but LOW relay→terminal capacity (30-50)
    - Other relays have moderate src→relay (500-1000)
      but HIGH relay→terminal (800-1500)
    
    LP advantage: Capacity heuristic picks decoy relay; LP avoids it.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))
    
    # Pick one decoy relay (if we have relays)
    decoy_relay = relays[0] if relays else None
    good_relays = relays[1:] if len(relays) > 1 else []
    
    for u in all_nodes:
        for v in all_nodes:
            if u == v:
                continue
            
            # src→terminal: WEAK
            if u == src and v in terminals:
                caps[(u, v)] = rng.uniform(50.0, 150.0)
            elif v == src and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(50.0, 150.0))
            
            # src→decoy relay: VERY STRONG (the trap!)
            elif u == src and v == decoy_relay:
                caps[(u, v)] = rng.uniform(2500.0, 3000.0)
            elif v == src and u == decoy_relay:
                caps[(u, v)] = caps.get((v, u), rng.uniform(2500.0, 3000.0))
            
            # decoy→terminal: VERY WEAK (the trap!)
            elif u == decoy_relay and v in terminals:
                caps[(u, v)] = rng.uniform(30.0, 50.0)
            elif v == decoy_relay and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(30.0, 50.0))
            
            # src→good relays: MODERATE
            elif u == src and v in good_relays:
                caps[(u, v)] = rng.uniform(500.0, 1000.0)
            elif v == src and u in good_relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(500.0, 1000.0))
            
            # good relay→terminal: STRONG
            elif u in good_relays and v in terminals:
                caps[(u, v)] = rng.uniform(800.0, 1500.0)
            elif v in good_relays and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(800.0, 1500.0))
            
            # relay→relay: MEDIUM
            elif u in relays and v in relays:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(200.0, 800.0)
                caps[(u, v)] = caps[key]
            
            # Other: MEDIUM
            else:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(100.0, 400.0)
                caps[(u, v)] = caps[key]
    
    return caps


def make_asymmetric_fan(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    Asymmetric fanout profile (`asymmetric_fan`).
    
    Design:
    - Each relay has high capacity to only SOME terminals, low to others
    - LP must intelligently assign terminals to the right relays
    
    LP advantage: Better load balancing across relays.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))
    
    # Assign each terminal a "preferred" relay (cyclic if needed)
    terminal_preferred: dict[int, int] = {}
    for i, t in enumerate(terminals):
        if relays:
            terminal_preferred[t] = relays[i % len(relays)]
    
    for u in all_nodes:
        for v in all_nodes:
            if u == v:
                continue
            
            # src→terminal: WEAK
            if u == src and v in terminals:
                caps[(u, v)] = rng.uniform(40.0, 120.0)
            elif v == src and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(40.0, 120.0))
            
            # src→relay: STRONG
            elif u == src and v in relays:
                caps[(u, v)] = rng.uniform(1000.0, 2500.0)
            elif v == src and u in relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(1000.0, 2500.0))
            
            # relay→terminal: depends on "preference"
            elif u in relays and v in terminals:
                if terminal_preferred.get(v) == u:
                    # Preferred relay: HIGH capacity
                    caps[(u, v)] = rng.uniform(1000.0, 2000.0)
                else:
                    # Non-preferred: LOW capacity
                    caps[(u, v)] = rng.uniform(30.0, 80.0)
            elif v in relays and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(100.0, 500.0))
            
            # relay→relay: MEDIUM
            elif u in relays and v in relays:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(300.0, 1000.0)
                caps[(u, v)] = caps[key]
            
            # Other: MEDIUM
            else:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(100.0, 500.0)
                caps[(u, v)] = caps[key]
    
    return caps


def make_multi_tier(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    Multi-tier relay profile (requires 2-hop relay paths) (`multi_tier`).
    
    Design:
    - Relays are split into "tier-1" (close to src) and "tier-2" (close to terminals)
    - Best path is: src → tier1-relay → tier2-relay → terminal
    - Direct and single-hop paths are weak
    
    LP advantage: Handles complex multi-hop topologies.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))
    
    # Split relays into two tiers
    mid = len(relays) // 2
    tier1_relays = relays[:mid] if mid > 0 else relays[:1]
    tier2_relays = relays[mid:] if mid > 0 else relays[1:]
    
    for u in all_nodes:
        for v in all_nodes:
            if u == v:
                continue
            
            # src→terminal: VERY WEAK
            if u == src and v in terminals:
                caps[(u, v)] = rng.uniform(30.0, 60.0)
            elif v == src and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(30.0, 60.0))
            
            # src→tier1: STRONG
            elif u == src and v in tier1_relays:
                caps[(u, v)] = rng.uniform(1500.0, 3000.0)
            elif v == src and u in tier1_relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(1500.0, 3000.0))
            
            # src→tier2: WEAK (force multi-hop)
            elif u == src and v in tier2_relays:
                caps[(u, v)] = rng.uniform(50.0, 100.0)
            elif v == src and u in tier2_relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(50.0, 100.0))
            
            # tier1→tier2: STRONG
            elif u in tier1_relays and v in tier2_relays:
                caps[(u, v)] = rng.uniform(1000.0, 2500.0)
            elif v in tier1_relays and u in tier2_relays:
                caps[(u, v)] = caps.get((v, u), rng.uniform(1000.0, 2500.0))
            
            # tier1→terminal: WEAK (force multi-hop)
            elif u in tier1_relays and v in terminals:
                caps[(u, v)] = rng.uniform(40.0, 80.0)
            elif v in tier1_relays and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(40.0, 80.0))
            
            # tier2→terminal: STRONG
            elif u in tier2_relays and v in terminals:
                caps[(u, v)] = rng.uniform(800.0, 1800.0)
            elif v in tier2_relays and u in terminals:
                caps[(u, v)] = caps.get((v, u), rng.uniform(800.0, 1800.0))
            
            # relay→relay same tier: MEDIUM
            elif (u in tier1_relays and v in tier1_relays) or (u in tier2_relays and v in tier2_relays):
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(200.0, 600.0)
                caps[(u, v)] = caps[key]
            
            # terminal→terminal: WEAK
            elif u in terminals and v in terminals:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(50.0, 150.0)
                caps[(u, v)] = caps[key]
            
            # Other: MEDIUM
            else:
                key = (min(u, v), max(u, v))
                if key not in caps:
                    caps[key] = rng.uniform(100.0, 400.0)
                caps[(u, v)] = caps[key]
    
    return caps


def make_random_wide(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    Random-wide baseline profile (`random_wide`).
    
    Baseline profile to see how algorithms perform on random topologies.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))
    
    for u in all_nodes:
        for v in all_nodes:
            if u >= v:
                continue
            cap = rng.uniform(CAPACITY_MIN, CAPACITY_MAX)
            caps[(u, v)] = cap
            caps[(v, u)] = cap
    
    return caps



def make_lp_trap(
    n_nodes: int,
    src: int,
    terminals: list[int],
    relays: list[int],
    rng: random.Random,
) -> dict[tuple[int, int], float]:
    """
    LP-Trap profile designed to defeat capacity heuristic (`lp_trap`).
    
    Capacity heuristic scores relays by: sum of ALL outgoing edge capacities.
    
    Design:
    - "Trap" relays (first half): HIGH relay-degree capacity
        - trap↔trap: VERY HIGH (2000-3000 Mbps each)
        - trap↔terminal: VERY LOW (30-40 Mbps)
        - trap↔(good relays): LOW (50-150 Mbps)
        - Result: trap relays win sum(outgoing) once there are enough trap relays.
    - "Good" relays (second half): LOWER relay-degree capacity, but good to terminals
        - good↔terminal: MODERATE-HIGH (400-600 Mbps)
        - good↔(any relays): LOW (50-150 Mbps)
    
    LP advantage: with a relay budget, capacity-based selection tends to pick trap relays,
    but LP-guided selection (CF-RelaySelect) prefers good relays because they actually
    carry flow to terminals.

    Note: this profile is symmetric (u↔v capacities match) so it can be realized via
    controller `[[link]]` entries.
    """
    caps: dict[tuple[int, int], float] = {}
    all_nodes = list(range(1, n_nodes + 1))

    # Split relays: first half are traps, second half are good.
    mid = max(1, len(relays) // 2)
    trap_relays = set(relays[:mid])
    good_relays = set(relays[mid:])
    terminal_set = set(terminals)

    for u in all_nodes:
        for v in all_nodes:
            if u >= v:
                continue

            cap: float

            # src ↔ terminal: weak (force relay usage)
            if (u == src and v in terminal_set) or (v == src and u in terminal_set):
                cap = float(rng.uniform(30.0, 50.0))

            # src ↔ relay: moderate (same for trap/good)
            elif (u == src and v in trap_relays | good_relays) or (v == src and u in trap_relays | good_relays):
                cap = float(rng.uniform(400.0, 600.0))

            # terminal ↔ relay:
            # - trap relays are bottlenecked to terminals
            # - good relays sustain higher rates to terminals
            elif (u in terminal_set and v in trap_relays) or (v in terminal_set and u in trap_relays):
                cap = float(rng.uniform(30.0, 40.0))
            elif (u in terminal_set and v in good_relays) or (v in terminal_set and u in good_relays):
                cap = float(rng.uniform(400.0, 600.0))

            # relay ↔ relay:
            # - trap↔trap links inflate total relay degree capacity for trap relays
            elif u in trap_relays and v in trap_relays:
                cap = float(rng.uniform(2000.0, 3000.0))
            elif (u in trap_relays | good_relays) and (v in trap_relays | good_relays):
                cap = float(rng.uniform(50.0, 150.0))

            # terminal ↔ terminal: low
            elif u in terminal_set and v in terminal_set:
                cap = float(rng.uniform(40.0, 80.0))

            # Everything else: medium
            else:
                cap = float(rng.uniform(100.0, 300.0))

            caps[(u, v)] = cap
            caps[(v, u)] = cap

    return caps


PROFILES: dict[str, Callable] = {
    "hierarchy_weak_direct": make_hierarchy_weak_direct,
    "bottleneck_trap": make_bottleneck_trap,
    "asymmetric_fan": make_asymmetric_fan,
    "multi_tier": make_multi_tier,
    "random_wide": make_random_wide,
    "lp_trap": make_lp_trap,
}


# =============================================================================
# Benchmark Runner
# =============================================================================

def build_graph(
    n_workers: int,
    n_relays: int,
    capacities: dict[tuple[int, int], float],
) -> tuple[Graph, int, list[int], list[int]]:
    """Build a full-mesh graph with the given capacities."""
    # Node layout: 1 = trainer (src), 2..n_workers+1 = workers, rest = relays
    src = 1
    terminals = list(range(2, 2 + n_workers))
    relays = list(range(2 + n_workers, 2 + n_workers + n_relays))
    
    n_nodes = 1 + n_workers + n_relays
    nodes = list(range(1, n_nodes + 1))
    edges = [(u, v) for u in nodes for v in nodes if u != v]
    
    # Fill in any missing capacities with a default
    full_caps: dict[tuple[int, int], float] = {}
    for e in edges:
        full_caps[e] = capacities.get(e, 100.0)
    
    graph = Graph(nodes, edges, full_caps)
    return graph, src, terminals, relays


def _select_relays_by_outgoing_capacity(
    graph: Graph, relay_candidates: list[int], *, k: int
) -> list[int]:
    """Pick top-k relays by sum of outgoing capacities.

    This matches the "capacity heuristic" baseline used in the relay-selection experiments.
    """
    if k <= 0 or not relay_candidates:
        return []
    scored = []
    for r_id in relay_candidates:
        score = 0.0
        for _, cap in graph.adj.get(r_id, []):
            score += float(cap)
        scored.append((r_id, score))
    scored.sort(key=lambda kv: kv[1], reverse=True)
    return [r_id for r_id, _ in scored[: min(k, len(scored))]]


def run_algorithm(
    graph: Graph,
    src: int,
    terminals: list[int],
    algorithm: str,
    hop_limit: int = 4,
    relay_nodes: list[int] | None = None,
    max_relays: int | None = None,
    relay_scoring: str = "coverage",
) -> TreeResult:
    """Run a single algorithm and return result."""
    try:
        result = compute_tree_edges(
            graph,
            src=src,
            destinations=terminals,
            algorithm=algorithm,
            hop_limit=hop_limit,
            eta=0.1,
            max_length=hop_limit,
            num_paths=2,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
        )
        return result
    except Exception as e:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm=algorithm,
            lp_f_star=None,
            error=str(e),
        )


def run_benchmark(
    profile_name: str,
    n_workers: int,
    n_relays: int,
    seed: int,
    hop_limit: int = 4,
    max_relays: int | None = None,
) -> list[BenchmarkResult]:
    """Run all algorithms on a single configuration."""
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
    
    # If max_relays is set, test relay selection methods
    if max_relays is not None and max_relays < n_relays:
        for algo, scoring in RELAY_METHODS:
            if algo == "basic_bottleneck":
                # basic_bottleneck does not implement relay_scoring internally.
                # To compare against CF-RelaySelect, we select a relay subset externally.
                if scoring == "capacity":
                    chosen = _select_relays_by_outgoing_capacity(graph, relays, k=max_relays)
                elif scoring == "random":
                    chosen = rng.sample(relays, max_relays) if max_relays > 0 else []
                else:
                    raise ValueError(f"Unknown relay selection baseline: {algo}_{scoring}")

                tree_result = run_algorithm(
                    graph,
                    src,
                    terminals,
                    algo,
                    hop_limit,
                    relay_nodes=chosen,
                    max_relays=None,
                )
            else:
                tree_result = run_algorithm(
                    graph,
                    src,
                    terminals,
                    algo,
                    hop_limit,
                    relay_nodes=relays,
                    max_relays=max_relays,
                    relay_scoring=scoring,
                )
            
            throughput = tree_result.throughput
            completion = mbps_to_completion_sec(throughput)
            label = f"{algo}_{scoring}"
            
            results.append(BenchmarkResult(
                profile=profile_name,
                n_workers=n_workers,
                n_relays=n_relays,
                algorithm=label,
                throughput_mbps=throughput,
                completion_time_sec=completion,
                lp_f_star=tree_result.lp_f_star,
                tree_edges=len(tree_result.edges),
                error=tree_result.error,
                max_relays=max_relays,
                relay_scoring=scoring,
            ))
        
        # Also run star as baseline
        tree_result = run_algorithm(graph, src, terminals, "star", hop_limit)
        throughput = tree_result.throughput
        completion = mbps_to_completion_sec(throughput)
        results.append(BenchmarkResult(
            profile=profile_name,
            n_workers=n_workers,
            n_relays=n_relays,
            algorithm="star",
            throughput_mbps=throughput,
            completion_time_sec=completion,
            lp_f_star=tree_result.lp_f_star,
            tree_edges=len(tree_result.edges),
            error=tree_result.error,
            max_relays=max_relays,
        ))
    else:
        # No relay budget: run standard algorithms
        for algo in ALGORITHMS:
            tree_result = run_algorithm(graph, src, terminals, algo, hop_limit)
            
            throughput = tree_result.throughput
            completion = mbps_to_completion_sec(throughput)
            
            results.append(BenchmarkResult(
                profile=profile_name,
                n_workers=n_workers,
                n_relays=n_relays,
                algorithm=algo,
                throughput_mbps=throughput,
                completion_time_sec=completion,
                lp_f_star=tree_result.lp_f_star,
                tree_edges=len(tree_result.edges),
                error=tree_result.error,
            ))
    
    return results


def print_results_table(results: list[BenchmarkResult]) -> None:
    """Print results as a formatted table."""
    if not results:
        return
    
    profile = results[0].profile
    n_workers = results[0].n_workers
    n_relays = results[0].n_relays
    
    print(f"\n{'='*70}")
    print(f"Profile: {profile}, Workers: {n_workers}, Relays: {n_relays}")
    print(f"{'='*70}")
    print(f"{'Algorithm':<20} {'Throughput':>12} {'Completion':>12} {'vs Best':>10}")
    print(f"{'':<20} {'(Mbps)':>12} {'(sec)':>12} {'':<10}")
    print("-" * 70)
    
    # Find best throughput
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
            if best_tput > 0 and r.throughput_mbps == best_tput:
                vs_best = "BEST"
            elif best_tput > 0:
                pct = (r.throughput_mbps - best_tput) / best_tput * 100
                vs_best = f"{pct:+.0f}%"
            else:
                vs_best = "N/A"
        
        print(f"{r.algorithm:<20} {tput_str:>12} {time_str:>12} {vs_best:>10}")
        if r.throughput_mbps is None and r.error:
            print(f"{'':<20} {'':>12} {'':>12} error={r.error}")


def main() -> int:
    parser = argparse.ArgumentParser(description="LP-Advantage Benchmark Suite")
    parser.add_argument("--workers", type=str, default="2,4,6,8,10",
                        help="Comma-separated worker counts")
    parser.add_argument("--relays", type=str, default="2,4,6,8,10",
                        help="Comma-separated relay counts")
    parser.add_argument("--profiles", type=str, default=",".join(PROFILES.keys()),
                        help="Comma-separated profile names")
    parser.add_argument("--hop-limit", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-relays", type=int, default=None,
                        help="Relay budget - max relays to select")
    parser.add_argument("--all", action="store_true",
                        help="Run full matrix (all workers × relays × profiles)")
    parser.add_argument("--output-json", type=str, default=None,
                        help="Output JSON file path")
    args = parser.parse_args()
    
    worker_counts = [int(x.strip()) for x in args.workers.split(",")]
    relay_counts = [int(x.strip()) for x in args.relays.split(",")]
    profile_names = [x.strip() for x in args.profiles.split(",") if x.strip()]
    
    all_results: list[BenchmarkResult] = []
    
    print("=" * 70)
    print("LP-Advantage Benchmark Suite")
    print(f"Capacity range: {CAPACITY_MIN} - {CAPACITY_MAX} Mbps")
    print(f"Artifact size: {ARTIFACT_SIZE_BYTES / (1024**3):.0f} GB")
    print("=" * 70)
    
    for profile in profile_names:
        for n_workers in worker_counts:
            for n_relays in relay_counts:
                results = run_benchmark(
                    profile,
                    n_workers,
                    n_relays,
                    seed=args.seed,
                    hop_limit=args.hop_limit,
                    max_relays=args.max_relays,
                )
                all_results.extend(results)
                print_results_table(results)
    
    # Save to JSON
    output_dir = Path(__file__).parent / "experiment_results"
    output_dir.mkdir(exist_ok=True)
    
    # Include microseconds to avoid collisions when running multiple jobs quickly.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    out_path = args.output_json or str(output_dir / f"lp_advantage_{timestamp}.json")
    
    with open(out_path, "w") as f:
        json.dump({
            "config": {
                "workers": worker_counts,
                "relays": relay_counts,
                "profiles": profile_names,
                "hop_limit": args.hop_limit,
                "seed": args.seed,
                "capacity_range": [CAPACITY_MIN, CAPACITY_MAX],
                "artifact_size_gb": ARTIFACT_SIZE_BYTES / (1024**3),
            },
            "results": [asdict(r) for r in all_results],
        }, f, indent=2)
    
    print(f"\nResults saved to: {out_path}")
    
    # Summary: LP advantage analysis
    print("\n" + "=" * 70)
    print("LP ADVANTAGE SUMMARY")
    print("=" * 70)
    
    for profile in profile_names:
        profile_results = [r for r in all_results if r.profile == profile]
        
        cf_wins = 0
        basic_wins = 0
        ties = 0
        
        # Group by (n_workers, n_relays)
        configs = set((r.n_workers, r.n_relays) for r in profile_results)
        for n_w, n_r in configs:
            cfg_results = [r for r in profile_results if r.n_workers == n_w and r.n_relays == n_r]

            # In no-budget mode, the algorithms are named `cf_bottleneck` / `basic_bottleneck`.
            # In budget mode (max_relays < n_relays), results are emitted as:
            #   - `cf_bottleneck_coverage` / `cf_bottleneck_path_flow`
            #   - `basic_bottleneck_capacity` / `basic_bottleneck_random`
            # For a stable summary, compare the best LP-backed CF variant against the
            # capacity-based baseline when available.
            cf_candidates = [
                r.throughput_mbps
                for r in cfg_results
                if r.throughput_mbps is not None
                and (r.algorithm == "cf_bottleneck" or r.algorithm.startswith("cf_bottleneck_"))
                and r.algorithm != "cf_bottleneck_mwu"
            ]
            basic_candidates = [
                r.throughput_mbps
                for r in cfg_results
                if r.throughput_mbps is not None
                and (
                    r.algorithm == "basic_bottleneck"
                    or r.algorithm.startswith("basic_bottleneck_capacity")
                )
            ]

            cf_tput = max(cf_candidates) if cf_candidates else None
            basic_tput = max(basic_candidates) if basic_candidates else None
            
            if cf_tput is not None and basic_tput is not None:
                if cf_tput > basic_tput + 1e-6:
                    cf_wins += 1
                elif basic_tput > cf_tput + 1e-6:
                    basic_wins += 1
                else:
                    ties += 1
        
        total = cf_wins + basic_wins + ties
        if total > 0:
            print(f"\n{profile}:")
            print(f"  CF-Bottleneck wins: {cf_wins}/{total} ({100*cf_wins/total:.0f}%)")
            print(f"  Basic-Bottleneck wins: {basic_wins}/{total} ({100*basic_wins/total:.0f}%)")
            print(f"  Ties: {ties}/{total} ({100*ties/total:.0f}%)")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
