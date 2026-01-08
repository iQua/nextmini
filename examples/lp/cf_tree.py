"""
CF-Tree / CF-Bottleneck: LP-guided hop-limited relay broadcast planning.

This module implements Skyrocket's Stage-2 rounding pipeline:
- BuildTree (CF-Tree): grows a hop-limited replication arborescence using
  LP-derived edge-importance signals.
- CF-Bottleneck: optionally sweeps candidate session rates and prunes
  low-capacity edges to avoid bottleneck throttling.
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
NodeEgressBudgets = dict[NodeId, float | None]
FanoutCaps = dict[NodeId, int | None]


@dataclass
class CFTreeResult:
    """Result of CF-Tree algorithm."""
    edges: list[Edge]
    tree_rate: float
    lp_f_star: float
    nodes_in_tree: set[NodeId]


@dataclass
class LPSolution:
    """LP solution containing edge flows and optimal rate."""
    f_star: float  # Optimal common rate (LP f*)
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
    f_star = float(sol[variables["x"]]) if "x" in variables else 0.0

    # Extract path flows (clamp to nonnegative; solvers may return tiny negative noise).
    path_flows: dict[tuple[NodeId, ...], float] = {}
    for name, idx in variables.items():
        if not name.startswith("p_"):
            continue
        path_str = name[2:]  # Remove "p_"
        path = tuple(map(int, path_str.split("_")))
        if not path or path[0] != src:
            continue
        value = float(sol[idx])
        if value <= 0.0:
            continue
        path_flows[path] = value

    # Stabilize: ensure each destination receives exactly f_star (up to tolerance).
    # This avoids degenerate LP optima that send "extra" conceptual flow and pollute
    # the edge-importance signal.
    if f_star > 0.0:
        totals: dict[NodeId, float] = {}
        for path, flow in path_flows.items():
            totals[path[-1]] = totals.get(path[-1], 0.0) + flow

        tol = 1e-7
        for dst, total in totals.items():
            if total <= f_star * (1.0 + tol):
                continue
            scale = f_star / total
            for path in [p for p in path_flows if p[-1] == dst]:
                path_flows[path] *= scale

    # Compute effective edge usage x(e) = max_{dst} f_dst(e) from path flows.
    edge_flows: dict[Edge, float] = {}
    per_edge_per_dst: dict[tuple[NodeId, Edge], float] = {}
    for path, flow in path_flows.items():
        dst = path[-1]
        for i in range(len(path) - 1):
            edge = (path[i], path[i + 1])
            key = (dst, edge)
            per_edge_per_dst[key] = per_edge_per_dst.get(key, 0.0) + flow

    for (dst, edge), flow in per_edge_per_dst.items():
        edge_flows[edge] = max(edge_flows.get(edge, 0.0), flow)

    return LPSolution(f_star=f_star, edge_flows=edge_flows, path_flows=path_flows)


def compute_edge_importance(
    lp_sol: LPSolution,
    epsilon: float = 1e-9,
) -> dict[Edge, float]:
    """Compute edge-importance signal (π(e) in the paper) from LP solution.

    π(e) = min(1, x(e) / (f* + epsilon))

    Args:
        lp_sol: LP solution
        epsilon: Small value to avoid division by zero

    Returns:
        Dict mapping edges to importance scores in [0, 1]
    """
    importance: dict[Edge, float] = {}
    denom = lp_sol.f_star + epsilon

    for edge, flow in lp_sol.edge_flows.items():
        importance_score = min(1.0, flow / denom)
        importance[edge] = importance_score

    return importance


def compute_cf_weights(
    graph: Graph,
    importance: dict[Edge, float],
    eta: float = 0.1,
    delta: float = 1e-3,
    epsilon_c: float = 1e-9,
) -> dict[Edge, float]:
    """Compute CF-Tree edge weights.

    omega_cf(e) = 1/(C(e)/C_max + epsilon_c) + eta * (-log(max{π(e), delta}))

    The max{} formulation ensures the penalty term is always nonnegative:
    - When π(e) = 1 (high importance): -log(1) = 0 (no penalty)
    - When π(e) = 0 (low importance): -log(delta) >> 0 (high penalty)

    Args:
        graph: Network topology with capacities
        importance: Edge importance scores p(e) in [0, 1]
        eta: Weight for LP guidance term (0 = pure capacity-based)
        delta: Floor for importance to prevent log(0), must be in (0, 1)

    Returns:
        Dict mapping edges to CF-Tree weights (all nonnegative)
    """
    weights: dict[Edge, float] = {}
    c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)

    for edge, capacity in graph.capacities.items():
        # Basic weight: prefer high-capacity edges (use normalized capacity for stability).
        if capacity <= 0.0 or c_max <= 0.0:
            w_basic = float("inf")
        else:
            c_norm = capacity / c_max
            w_basic = 1.0 / (c_norm + epsilon_c)

        # LP guidance: prefer edges with high importance.
        # Use max{π, delta} to ensure the penalty is nonnegative.
        importance_score = importance.get(edge, 0.0)
        w_lp = -math.log(max(importance_score, delta))

        weights[edge] = w_basic + eta * w_lp

    return weights


def compute_basic_weights(graph: Graph, epsilon_c: float = 1e-9) -> dict[Edge, float]:
    """Compute basic (non-LP-guided) edge weights.

    omega_basic(e) = 1/(C(e)/C_max + epsilon_c)

    Args:
        graph: Network topology with capacities

    Returns:
        Dict mapping edges to basic weights
    """
    weights: dict[Edge, float] = {}
    c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)
    for edge, capacity in graph.capacities.items():
        if capacity <= 0.0 or c_max <= 0.0:
            weights[edge] = float("inf")
        else:
            c_norm = capacity / c_max
            weights[edge] = 1.0 / (c_norm + epsilon_c)
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
        forwarding_nodes: set[NodeId] | None = None,
    ):
        self.graph = graph
        self.weights = weights
        self.hop_limit = hop_limit
        self.forwarding_nodes = forwarding_nodes

    def dijkstra_to_terminal(
        self,
        tree_nodes: dict[NodeId, int],  # node -> depth in current tree
        terminal: NodeId,
        *,
        required_rate: float | None = None,
        out_degree: dict[NodeId, int] | None = None,
        node_egress_budgets: dict[NodeId, float] | None = None,
        fanout_caps: dict[NodeId, int] | None = None,
    ) -> tuple[list[NodeId], float] | None:
        """Find shortest hop-limited path from any tree node to terminal.

        Uses layered graph with one-copy-per-physical-node rule.

        Args:
            tree_nodes: Current tree nodes with their depths
            terminal: Target terminal node

        Returns:
            (path, cost) if reachable within hop limit, None otherwise
        """
        # Priority queue: (cost, layer, node, path, visited)
        #
        # We include `visited` in the search state so the "simple path" constraint
        # (no physical-node revisits) is correct: two paths that end at the same
        # (node, layer) can have different visited sets, enabling different
        # onward expansions.
        pq: list[
            tuple[float, int, NodeId, tuple[NodeId, ...], frozenset[NodeId]]
        ] = []

        for node, depth in tree_nodes.items():
            heapq.heappush(pq, (0.0, depth, node, (node,), frozenset((node,))))

        best: dict[tuple[NodeId, int, frozenset[NodeId]], float] = {}

        while pq:
            cost, layer, node, path, visited = heapq.heappop(pq)

            # Check if we reached terminal
            if node == terminal:
                return list(path), cost

            # Skip if we've found a better path to this (node, layer)
            key = (node, layer, visited)
            if key in best and best[key] <= cost:
                continue
            best[key] = cost

            # Cannot extend beyond hop limit
            if layer >= self.hop_limit:
                continue

            # Enforce relay eligibility / non-forwarding terminals: only expand from allowed forwarders.
            # Note: reaching a terminal is still allowed; this gate only applies to outgoing expansion.
            if self.forwarding_nodes is not None and node not in self.forwarding_nodes:
                continue

            deg = out_degree.get(node, 0) if out_degree is not None else 0
            if fanout_caps is not None:
                cap = fanout_caps.get(node)
                if cap is not None and deg + 1 > int(cap):
                    continue

            if (
                required_rate is not None
                and required_rate > 0.0
                and node_egress_budgets is not None
            ):
                budget = node_egress_budgets.get(node)
                if budget is not None and (deg + 1) * required_rate > budget + 1e-12:
                    continue

            # Expand to neighbors
            for neighbor, _ in self.graph.adj.get(node, []):
                next_layer = layer + 1

                # No re-entry: do not step into existing tree nodes.
                if neighbor in tree_nodes:
                    continue

                # Avoid cycles under projection: enforce simple physical-node paths.
                if neighbor in visited:
                    continue

                edge = (node, neighbor)
                if required_rate is not None and required_rate > 0.0:
                    cap = float(self.graph.capacities.get(edge, 0.0))
                    if cap < required_rate:
                        continue
                edge_weight = self.weights.get(edge, float("inf"))

                new_cost = cost + edge_weight
                new_visited = visited.union((neighbor,))
                new_path = path + (neighbor,)
                new_key = (neighbor, next_layer, new_visited)

                if new_key not in best or best[new_key] > new_cost:
                    heapq.heappush(pq, (new_cost, next_layer, neighbor, new_path, new_visited))

        return None  # Terminal unreachable

    def dijkstra_to_any_terminal(
        self,
        tree_nodes: dict[NodeId, int],
        terminals: set[NodeId],
        *,
        required_rate: float | None = None,
        out_degree: dict[NodeId, int] | None = None,
        node_egress_budgets: dict[NodeId, float] | None = None,
        fanout_caps: dict[NodeId, int] | None = None,
    ) -> tuple[NodeId, list[NodeId], float] | None:
        """Find cheapest hop-limited path from the current tree to any terminal.

        This matches the BuildTree step in the paper: run Dijkstra from a super-source
        connected to each current tree node copy (v, depth(v)) with 0 cost, then stop
        when the first missing terminal is dequeued.

        Args:
            tree_nodes: Current tree nodes with their depths
            terminals: Remaining (not-yet-connected) terminals

        Returns:
            (terminal, path, cost) if reachable, None otherwise.
        """
        if not terminals:
            return None

        pq: list[
            tuple[float, int, NodeId, tuple[NodeId, ...], frozenset[NodeId]]
        ] = []
        for node, depth in tree_nodes.items():
            heapq.heappush(pq, (0.0, depth, node, (node,), frozenset((node,))))

        best: dict[tuple[NodeId, int, frozenset[NodeId]], float] = {}

        while pq:
            cost, layer, node, path, visited = heapq.heappop(pq)

            if node in terminals:
                return node, list(path), cost

            key = (node, layer, visited)
            if key in best and best[key] <= cost:
                continue
            best[key] = cost

            if layer >= self.hop_limit:
                continue

            if self.forwarding_nodes is not None and node not in self.forwarding_nodes:
                continue

            deg = out_degree.get(node, 0) if out_degree is not None else 0
            if fanout_caps is not None:
                cap = fanout_caps.get(node)
                if cap is not None and deg + 1 > int(cap):
                    continue

            if (
                required_rate is not None
                and required_rate > 0.0
                and node_egress_budgets is not None
            ):
                budget = node_egress_budgets.get(node)
                if budget is not None and (deg + 1) * required_rate > budget + 1e-12:
                    continue

            for neighbor, _ in self.graph.adj.get(node, []):
                next_layer = layer + 1

                # No re-entry: do not step into existing tree nodes.
                if neighbor in tree_nodes:
                    continue

                if neighbor in visited:
                    continue

                edge = (node, neighbor)
                if required_rate is not None and required_rate > 0.0:
                    cap = float(self.graph.capacities.get(edge, 0.0))
                    if cap < required_rate:
                        continue
                edge_weight = self.weights.get(edge, float("inf"))

                new_cost = cost + edge_weight
                new_visited = visited.union((neighbor,))
                new_path = path + (neighbor,)
                new_key = (neighbor, next_layer, new_visited)

                if new_key not in best or best[new_key] > new_cost:
                    heapq.heappush(pq, (new_cost, next_layer, neighbor, new_path, new_visited))

        return None


def build_cf_tree(
    graph: Graph,
    weights: dict[Edge, float],
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int,
    forwarding_nodes: set[NodeId] | None = None,
    *,
    required_rate: float | None = None,
    node_egress_budgets: NodeEgressBudgets | None = None,
    fanout_caps: FanoutCaps | None = None,
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
    out_degree: dict[NodeId, int] = {src: 0}

    layered = LayeredGraph(graph, weights, hop_limit, forwarding_nodes=forwarding_nodes)
    sanitized_budgets = _sanitize_node_egress_budgets(node_egress_budgets)
    sanitized_fanout = _sanitize_fanout_caps(fanout_caps)

    remaining = set(terminals)
    remaining.discard(src)

    while remaining:
        result = layered.dijkstra_to_any_terminal(
            tree_nodes,
            remaining,
            required_rate=required_rate,
            out_degree=out_degree,
            node_egress_budgets=sanitized_budgets,
            fanout_caps=sanitized_fanout,
        )
        if result is None:
            missing = sorted(remaining)
            raise ValueError(
                f"Terminals {missing} unreachable from source {src} "
                f"within {hop_limit} hops"
            )

        terminal, path, _ = result

        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            edge = (u, v)

            if v not in tree_nodes:
                tree_nodes[v] = tree_nodes[u] + 1
                tree_edges.append(edge)
                out_degree[u] = out_degree.get(u, 0) + 1
                out_degree.setdefault(v, 0)

        remaining.discard(terminal)

    return tree_edges, tree_nodes


def build_cf_tree_bottleneck(
    graph: Graph,
    weights: dict[Edge, float],
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int,
    *,
    forwarding_nodes: set[NodeId] | None = None,
    node_egress_budgets: NodeEgressBudgets | None = None,
    fanout_caps: FanoutCaps | None = None,
    candidate_rates: t.Sequence[float] | None = None,
    early_stop: bool = True,
) -> tuple[list[Edge], dict[NodeId, int], float]:
    """Bottleneck-aware CF-Tree: search over candidate rates.

    For each candidate broadcast rate f, we attempt to construct a hop-limited tree
    that can sustain at least f by:
    - filtering edges with C(e) < f, and
    - enforcing optional per-node egress budgets / fanout caps during growth.

    Returns the best tree found (by achieved f_tree).
    """
    if candidate_rates is None:
        rates: set[float] = {float(c) for c in graph.capacities.values() if float(c) > 0.0}
        # If per-node egress budgets apply, the achievable tree rate can also be
        # limited by U_u / deg^+(u). Include these breakpoints so the sweep can
        # find budget-limited optima (not just link-capacity-limited ones).
        sanitized_budgets = _sanitize_node_egress_budgets(node_egress_budgets)
        sanitized_fanout = _sanitize_fanout_caps(fanout_caps)
        if sanitized_budgets:
            terminals_set = {t for t in terminals if t != src}
            max_children_default = len(terminals_set)
            for node, budget in sanitized_budgets.items():
                cap = float(budget)
                if cap <= 0.0:
                    continue
                max_children = max_children_default
                if sanitized_fanout and node in sanitized_fanout:
                    max_children = max(0, int(sanitized_fanout[node]))
                for d in range(1, max_children + 1):
                    rates.add(cap / float(d))
        candidate_rates = sorted({r for r in rates if r > 0.0}, reverse=True)

    best_edges: list[Edge] = []
    best_nodes: dict[NodeId, int] = {src: 0}
    best_rate = 0.0

    for rate in candidate_rates:
        # Candidate rates are processed in descending order. Once the current
        # threshold drops below the best achieved tree rate, no subsequent
        # iteration can improve the optimum (any feasible tree at threshold f
        # has f_tree >= f, and any strictly better f_tree would have been
        # feasible at an earlier threshold >= f_tree).
        if early_stop and best_rate > 0.0 and rate <= best_rate + 1e-12:
            break
        if rate <= 0.0:
            continue
        try:
            edges, nodes = build_cf_tree(
                graph,
                weights,
                src,
                terminals,
                hop_limit,
                forwarding_nodes=forwarding_nodes,
                required_rate=rate,
                node_egress_budgets=node_egress_budgets,
                fanout_caps=fanout_caps,
            )
        except ValueError:
            continue

        achieved = compute_tree_rate(
            graph,
            edges,
            nodes,
            node_egress_budgets=node_egress_budgets,
        )
        if achieved > best_rate and edges:
            best_rate = achieved
            best_edges = edges
            best_nodes = nodes

    if not best_edges:
        raise ValueError(
            f"No feasible tree found for src={src} within {hop_limit} hops"
        )

    return best_edges, best_nodes, best_rate


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
    node_egress_budgets: NodeEgressBudgets | None = None,
) -> float:
    """Compute maximum sustainable broadcast rate on the tree.

    f_tree = min(min_{e in tree} C(e), min_{u in tree} B_u / deg+(u))

    Args:
        graph: Network topology with capacities
        tree_edges: Edges in the tree
        tree_nodes: Nodes in the tree with depths
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
    sanitized_budgets = _sanitize_node_egress_budgets(node_egress_budgets)
    if sanitized_budgets:
        for node, deg in out_degree.items():
            if deg <= 0:
                continue
            budget = sanitized_budgets.get(node)
            if budget is None:
                continue
            min_node_rate = min(min_node_rate, budget / float(deg))

    # Combine all constraints
    f_tree = min_edge_cap
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
    forwarding_nodes: set[NodeId] | None = None,
    node_egress_budgets: NodeEgressBudgets | None = None,
    fanout_caps: FanoutCaps | None = None,
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
        CFTreeResult with tree edges, rate, and LP f* metadata
    """
    # Extract LP solution
    lp_sol = extract_lp_solution(graph, variables, sol, src)

    # Compute weights
    if use_lp_guidance and lp_sol.f_star > 0:
        c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)
        epsilon_f = 1e-9 * c_max if c_max > 0.0 else 1e-9
        importance = compute_edge_importance(lp_sol, epsilon=epsilon_f)
        weights = compute_cf_weights(graph, importance, eta=eta, delta=delta)
    else:
        weights = compute_basic_weights(graph)

    # Build tree
    tree_edges, tree_nodes = build_cf_tree(
        graph,
        weights,
        src,
        terminals,
        hop_limit,
        forwarding_nodes=forwarding_nodes,
        node_egress_budgets=node_egress_budgets,
        fanout_caps=fanout_caps,
    )

    # Compute achievable rate
    tree_rate = compute_tree_rate(
        graph,
        tree_edges,
        tree_nodes,
        node_egress_budgets=node_egress_budgets,
    )

    return CFTreeResult(
        edges=tree_edges,
        tree_rate=tree_rate,
        lp_f_star=lp_sol.f_star,
        nodes_in_tree=set(tree_nodes.keys()),
    )


def cf_tree_bottleneck_from_lp(
    graph: Graph,
    variables: dict[str, int],
    sol: list[float],
    src: NodeId,
    terminals: t.Sequence[NodeId],
    *,
    hop_limit: int = 3,
    eta: float = 0.1,
    delta: float = 1e-3,
    use_lp_guidance: bool = True,
    forwarding_nodes: set[NodeId] | None = None,
    node_egress_budgets: NodeEgressBudgets | None = None,
    fanout_caps: FanoutCaps | None = None,
    candidate_rates: t.Sequence[float] | None = None,
) -> CFTreeResult:
    """Bottleneck-aware CF-Tree from an LP solution.

    This variant searches over candidate broadcast rates and tries to construct a
    hop-limited tree that can sustain each rate, then returns the best tree found.
    """
    lp_sol = extract_lp_solution(graph, variables, sol, src)

    if use_lp_guidance and lp_sol.f_star > 0.0:
        c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)
        epsilon_f = 1e-9 * c_max if c_max > 0.0 else 1e-9
        importance = compute_edge_importance(lp_sol, epsilon=epsilon_f)
        weights = compute_cf_weights(graph, importance, eta=eta, delta=delta)
    else:
        weights = compute_basic_weights(graph)

    edges, nodes, achieved = build_cf_tree_bottleneck(
        graph,
        weights,
        src,
        terminals,
        hop_limit,
        forwarding_nodes=forwarding_nodes,
        node_egress_budgets=node_egress_budgets,
        fanout_caps=fanout_caps,
        candidate_rates=candidate_rates,
    )

    return CFTreeResult(
        edges=edges,
        tree_rate=achieved,
        lp_f_star=lp_sol.f_star,
        nodes_in_tree=set(nodes.keys()),
    )


def cf_tree_direct(
    graph: Graph,
    src: NodeId,
    terminals: t.Sequence[NodeId],
    hop_limit: int = 3,
    forwarding_nodes: set[NodeId] | None = None,
    node_egress_budgets: NodeEgressBudgets | None = None,
    fanout_caps: FanoutCaps | None = None,
) -> CFTreeResult:
    """Build a basic tree without LP (pure capacity-based weights).

    This is the "Basic-Tree" baseline from the paper.

    Args:
        graph: Network topology
        src: Source node
        terminals: Destination nodes
        hop_limit: Maximum hops

    Returns:
        CFTreeResult (lp_f_star will be 0)
    """
    weights = compute_basic_weights(graph)

    tree_edges, tree_nodes = build_cf_tree(
        graph,
        weights,
        src,
        terminals,
        hop_limit,
        forwarding_nodes=forwarding_nodes,
        node_egress_budgets=node_egress_budgets,
        fanout_caps=fanout_caps,
    )

    tree_rate = compute_tree_rate(
        graph,
        tree_edges,
        tree_nodes,
        node_egress_budgets=node_egress_budgets,
    )

    return CFTreeResult(
        edges=tree_edges,
        tree_rate=tree_rate,
        lp_f_star=0.0,
        nodes_in_tree=set(tree_nodes.keys()),
    )


def _sanitize_node_egress_budgets(
    budgets: NodeEgressBudgets | None,
) -> dict[NodeId, float] | None:
    if not budgets:
        return None
    out: dict[NodeId, float] = {}
    for node, budget in budgets.items():
        if budget is None:
            continue
        cap = float(budget)
        if cap < 0.0:
            continue
        out[int(node)] = cap
    return out or None


def _sanitize_fanout_caps(
    caps: FanoutCaps | None,
) -> dict[NodeId, int] | None:
    if not caps:
        return None
    out: dict[NodeId, int] = {}
    for node, cap in caps.items():
        if cap is None:
            continue
        value = int(cap)
        if value < 0:
            continue
        out[int(node)] = value
    return out or None
