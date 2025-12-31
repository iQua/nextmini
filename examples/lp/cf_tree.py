"""
CF-Tree: Conceptual-Flow Guided Hop-Limited Relay Broadcast.

This module implements the CF-Tree algorithm from algo.tex, which uses LP-derived
edge importance signals to build hop-limited multicast trees.

Reference: CF-Tree paper (algo.tex in repo root)
"""

from __future__ import annotations

import heapq
import math
import typing as t
from dataclasses import dataclass

from .graph import Graph

# Type aliases
NodeId = int
Edge = tuple[NodeId, NodeId]


@dataclass
class CFTreeResult:
    """Result of CF-Tree algorithm."""
    edges: list[Edge]
    tree_rate: float
    lp_upper_bound: float
    nodes_in_tree: set[NodeId]


@dataclass
class LPSolution:
    """LP solution containing edge flows and optimal rate."""
    f_star: float  # Optimal common rate (LP upper bound)
    edge_flows: dict[Edge, float]  # x(e) values from LP
    path_flows: dict[tuple[NodeId, ...], float]  # Path throughputs


def extract_lp_solution(
    graph: Graph,
    variables: dict[str, int],
    sol: list[float],
    src: NodeId,
) -> LPSolution:
    """Extract structured LP solution from raw solver output.

    Args:
        graph: Network topology
        variables: Variable name to index mapping
        sol: Solution values
        src: Source node ID

    Returns:
        LPSolution with f_star, edge_flows, and path_flows
    """
    f_star = sol[variables["x"]] if "x" in variables else 0.0

    # Extract edge flows (sum across all sessions from this source)
    edge_flows: dict[Edge, float] = {}
    for name, idx in variables.items():
        if name.startswith(f"e_{src}_"):
            parts = name.split("_")
            if len(parts) >= 4:
                n1, n2 = int(parts[2]), int(parts[3])
                edge = (n1, n2)
                edge_flows[edge] = edge_flows.get(edge, 0.0) + sol[idx]

    # Extract path flows
    path_flows: dict[tuple[NodeId, ...], float] = {}
    for name, idx in variables.items():
        if name.startswith("p_"):
            path_str = name[2:]  # Remove "p_"
            path = tuple(map(int, path_str.split("_")))
            if path[0] == src:
                path_flows[path] = sol[idx]

    return LPSolution(f_star=f_star, edge_flows=edge_flows, path_flows=path_flows)


def compute_edge_importance(
    lp_sol: LPSolution,
    epsilon: float = 1e-9,
) -> dict[Edge, float]:
    """Compute edge importance signal p(e) from LP solution.

    p(e) = min(1, x(e) / (f* + epsilon))

    Args:
        lp_sol: LP solution
        epsilon: Small value to avoid division by zero

    Returns:
        Dict mapping edges to importance scores in [0, 1]
    """
    importance: dict[Edge, float] = {}
    denom = lp_sol.f_star + epsilon

    for edge, flow in lp_sol.edge_flows.items():
        p = min(1.0, flow / denom)
        importance[edge] = p

    return importance


def compute_cf_weights(
    graph: Graph,
    importance: dict[Edge, float],
    eta: float = 0.1,
    delta: float = 1e-3,
) -> dict[Edge, float]:
    """Compute CF-Tree edge weights.

    omega_cf(e) = 1/C(e) + eta * (-log(max{p(e), delta}))

    The max{} formulation ensures the penalty term is always nonnegative:
    - When p(e) = 1 (high importance): -log(1) = 0 (no penalty)
    - When p(e) = 0 (low importance): -log(delta) >> 0 (high penalty)

    Args:
        graph: Network topology with capacities
        importance: Edge importance scores p(e) in [0, 1]
        eta: Weight for LP guidance term (0 = pure capacity-based)
        delta: Floor for importance to prevent log(0), must be in (0, 1)

    Returns:
        Dict mapping edges to CF-Tree weights (all nonnegative)
    """
    weights: dict[Edge, float] = {}

    for edge, capacity in graph.capacities.items():
        # Basic weight: prefer high-capacity edges
        w_basic = 1.0 / capacity if capacity > 0 else float("inf")

        # LP guidance: prefer edges with high importance
        # Use max{p, delta} to ensure penalty is nonnegative
        p = importance.get(edge, 0.0)
        w_lp = -math.log(max(p, delta))

        weights[edge] = w_basic + eta * w_lp

    return weights


def compute_basic_weights(graph: Graph) -> dict[Edge, float]:
    """Compute basic (non-LP-guided) edge weights.

    omega_basic(e) = 1/C(e)

    Args:
        graph: Network topology with capacities

    Returns:
        Dict mapping edges to basic weights
    """
    weights: dict[Edge, float] = {}
    for edge, capacity in graph.capacities.items():
        weights[edge] = 1.0 / capacity if capacity > 0 else float("inf")
    return weights


class LayeredGraph:
    """Layered graph for hop-limited shortest path computation.

    Each node v has copies (v, h) for h in 0..H, representing
    "node v reached in exactly h hops".
    """

    def __init__(
        self,
        graph: Graph,
        weights: dict[Edge, float],
        hop_limit: int,
    ):
        self.graph = graph
        self.weights = weights
        self.hop_limit = hop_limit

    def dijkstra_to_terminal(
        self,
        tree_nodes: dict[NodeId, int],  # node -> depth in current tree
        terminal: NodeId,
    ) -> tuple[list[NodeId], float] | None:
        """Find shortest hop-limited path from any tree node to terminal.

        Uses layered graph with one-copy-per-physical-node rule.

        Args:
            tree_nodes: Current tree nodes with their depths
            terminal: Target terminal node

        Returns:
            (path, cost) if reachable within hop limit, None otherwise
        """
        # Priority queue: (cost, layer, node, path)
        # path is list of (node, layer) tuples
        pq: list[tuple[float, int, NodeId, list[tuple[NodeId, int]]]] = []

        # Initialize from all tree nodes at their fixed depths
        for node, depth in tree_nodes.items():
            heapq.heappush(pq, (0.0, depth, node, [(node, depth)]))

        # Best cost to reach (node, layer)
        best: dict[tuple[NodeId, int], float] = {}

        while pq:
            cost, layer, node, path = heapq.heappop(pq)

            # Check if we reached terminal
            if node == terminal:
                # Extract physical path
                return [p[0] for p in path], cost

            # Skip if we've found a better path to this (node, layer)
            key = (node, layer)
            if key in best and best[key] <= cost:
                continue
            best[key] = cost

            # Cannot extend beyond hop limit
            if layer >= self.hop_limit:
                continue

            # Expand to neighbors
            for neighbor, _ in self.graph.adj.get(node, []):
                next_layer = layer + 1

                # One-copy rule: if neighbor is already in tree at different depth, skip
                if neighbor in tree_nodes and tree_nodes[neighbor] != next_layer:
                    continue

                # Also skip if neighbor already in this path (avoid cycles)
                path_nodes = {p[0] for p in path}
                if neighbor in path_nodes:
                    continue

                edge = (node, neighbor)
                edge_weight = self.weights.get(edge, float("inf"))

                new_cost = cost + edge_weight
                new_key = (neighbor, next_layer)

                if new_key not in best or best[new_key] > new_cost:
                    new_path = path + [(neighbor, next_layer)]
                    heapq.heappush(pq, (new_cost, next_layer, neighbor, new_path))

        return None  # Terminal unreachable


def build_cf_tree(
    graph: Graph,
    weights: dict[Edge, float],
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int,
) -> tuple[list[Edge], dict[NodeId, int]]:
    """Build hop-limited arborescence using weighted Dijkstra.

    Implements Algorithm 2 (BuildTree) from algo.tex.

    Args:
        graph: Network topology
        weights: Edge weights (CF or basic)
        src: Source node
        terminals: Destination nodes
        hop_limit: Maximum hops from source

    Returns:
        (edges, node_depths) where edges form the tree and node_depths
        maps each tree node to its depth from source

    Raises:
        ValueError: If any terminal is unreachable within hop limit
    """
    # Initialize tree with just the source
    tree_edges: list[Edge] = []
    tree_nodes: dict[NodeId, int] = {src: 0}  # node -> depth

    layered = LayeredGraph(graph, weights, hop_limit)

    # Sort terminals by estimated hop distance for better tree structure
    # (closer terminals first tends to produce better trees)
    terminal_list = sorted(
        [t for t in terminals if t != src],
        key=lambda t: _estimate_hops(graph, src, t),
    )

    for terminal in terminal_list:
        if terminal in tree_nodes:
            continue  # Already in tree

        result = layered.dijkstra_to_terminal(tree_nodes, terminal)

        if result is None:
            raise ValueError(
                f"Terminal {terminal} unreachable from source {src} "
                f"within {hop_limit} hops"
            )

        path, _ = result

        # Add new edges and nodes to tree
        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            edge = (u, v)

            if v not in tree_nodes:
                tree_nodes[v] = tree_nodes[u] + 1
                tree_edges.append(edge)

    return tree_edges, tree_nodes


def _estimate_hops(graph: Graph, src: NodeId, dst: NodeId) -> int:
    """Estimate minimum hops from src to dst using BFS."""
    if src == dst:
        return 0

    visited = {src}
    queue = [(src, 0)]

    while queue:
        node, hops = queue.pop(0)
        for neighbor, _ in graph.adj.get(node, []):
            if neighbor == dst:
                return hops + 1
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, hops + 1))

    return float("inf")  # Unreachable


def compute_tree_rate(
    graph: Graph,
    tree_edges: list[Edge],
    tree_nodes: dict[NodeId, int],
    lp_upper_bound: float | None = None,
    node_egress_budgets: dict[NodeId, float] | None = None,
) -> float:
    """Compute maximum sustainable broadcast rate on the tree.

    f_tree = min(f*, min_{e in tree} C(e), min_{u in tree} B_u / deg+(u))

    Args:
        graph: Network topology with capacities
        tree_edges: Edges in the tree
        tree_nodes: Nodes in the tree with depths
        lp_upper_bound: f* from LP (optional upper bound)
        node_egress_budgets: Per-node upload budgets B_u (optional)

    Returns:
        Maximum sustainable common rate
    """
    if not tree_edges:
        return 0.0

    # Minimum edge capacity
    min_edge_cap = min(
        graph.capacities.get(e, float("inf")) for e in tree_edges
    )

    # Node degree constraints
    out_degree: dict[NodeId, int] = {}
    for u, v in tree_edges:
        out_degree[u] = out_degree.get(u, 0) + 1

    min_node_rate = float("inf")
    if node_egress_budgets:
        for node, deg in out_degree.items():
            if node in node_egress_budgets and deg > 0:
                rate = node_egress_budgets[node] / deg
                min_node_rate = min(min_node_rate, rate)

    # Combine all constraints
    f_tree = min_edge_cap
    if lp_upper_bound is not None:
        f_tree = min(f_tree, lp_upper_bound)
    if min_node_rate < float("inf"):
        f_tree = min(f_tree, min_node_rate)

    return f_tree


def cf_tree_from_lp(
    graph: Graph,
    variables: dict[str, int],
    sol: list[float],
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int = 3,
    eta: float = 0.1,
    delta: float = 1e-3,
    use_lp_guidance: bool = True,
) -> CFTreeResult:
    """Main entry point: Build CF-Tree from LP solution.

    Args:
        graph: Network topology
        variables: LP variable name to index mapping
        sol: LP solution values
        src: Source node
        terminals: Destination nodes
        hop_limit: Maximum hops (H in paper, typically 2-4)
        eta: LP guidance weight (0 for pure capacity-based)
        delta: Smoothing for log term
        use_lp_guidance: If False, use basic weights (no LP guidance)

    Returns:
        CFTreeResult with tree edges, rate, and LP upper bound
    """
    # Extract LP solution
    lp_sol = extract_lp_solution(graph, variables, sol, src)

    # Compute weights
    if use_lp_guidance and lp_sol.f_star > 0:
        importance = compute_edge_importance(lp_sol)
        weights = compute_cf_weights(graph, importance, eta=eta, delta=delta)
    else:
        weights = compute_basic_weights(graph)

    # Build tree
    tree_edges, tree_nodes = build_cf_tree(
        graph, weights, src, terminals, hop_limit
    )

    # Compute achievable rate
    tree_rate = compute_tree_rate(
        graph, tree_edges, tree_nodes, lp_upper_bound=lp_sol.f_star
    )

    return CFTreeResult(
        edges=tree_edges,
        tree_rate=tree_rate,
        lp_upper_bound=lp_sol.f_star,
        nodes_in_tree=set(tree_nodes.keys()),
    )


def cf_tree_direct(
    graph: Graph,
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int = 3,
) -> CFTreeResult:
    """Build a basic tree without LP (pure capacity-based weights).

    This is the "Basic-Tree" baseline from the paper.

    Args:
        graph: Network topology
        src: Source node
        terminals: Destination nodes
        hop_limit: Maximum hops

    Returns:
        CFTreeResult (lp_upper_bound will be 0)
    """
    weights = compute_basic_weights(graph)

    tree_edges, tree_nodes = build_cf_tree(
        graph, weights, src, terminals, hop_limit
    )

    tree_rate = compute_tree_rate(graph, tree_edges, tree_nodes)

    return CFTreeResult(
        edges=tree_edges,
        tree_rate=tree_rate,
        lp_upper_bound=0.0,
        nodes_in_tree=set(tree_nodes.keys()),
    )
