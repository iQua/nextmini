"""Multicast tree helpers for Nextmini.

Design goal: be importable from other examples (notably `examples/rl`) without requiring DB access.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass
from collections import deque
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from .graph import Graph

Edge = tuple[int, int]


@dataclass
class TreeResult:
    """Result from tree computation."""
    edges: list[Edge]
    throughput: float | None
    algorithm: str  # "mflow" or "cf_tree"
    lp_f_star: float | None = None
    error: str | None = None


def _split_relay_candidates(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    relay_nodes: t.Sequence[int] | None,
    allow_destinations_as_relays: bool,
) -> tuple[set[int], list[int]]:
    """Return (terminal_forwarders, nonterminal_relay_candidates).

    - terminal_forwarders: destination nodes that are allowed to forward (not budgeted).
    - nonterminal_relay_candidates: relay pool subject to --max-relays style caps.

    Note: If callers pass destination IDs in `relay_nodes`, we treat them as explicitly
    allowed destination-forwarders to preserve backward compatibility.
    """
    terminals = {int(n) for n in destinations}
    terminals.discard(int(src))

    explicit_relays: set[int] = set()
    if relay_nodes is not None:
        explicit_relays = {int(n) for n in relay_nodes}

    terminal_forwarders: set[int] = set()
    if allow_destinations_as_relays:
        terminal_forwarders |= terminals
    terminal_forwarders |= (terminals & explicit_relays)

    if relay_nodes is None:
        relay_candidates = [
            int(n) for n in graph.nodes if int(n) != int(src) and int(n) not in terminals
        ]
    else:
        relay_candidates = [
            int(n)
            for n in relay_nodes
            if int(n) != int(src) and int(n) not in terminals
        ]

    relay_candidates = sorted(set(relay_candidates))
    return terminal_forwarders, relay_candidates


def _select_relays_lp_guided(
    graph: Graph,
    *,
    relay_candidates: t.Sequence[int],
    terminals: t.Iterable[int],
    max_relays: int,
    relay_scoring: str,
    lp_sol: t.Any,
) -> list[int]:
    """Select up to `max_relays` relays using LP solution signals.

    relay_scoring modes:
      - "incident": sum of incident edge-importance * capacity
      - "path_flow": sum of (path_flow / f*) over terminals that traverse the relay
      - "coverage": greedy terminal-coverage maximization with diminishing returns
    """
    relay_list = sorted({int(r) for r in relay_candidates})
    if max_relays <= 0 or not relay_list:
        return []

    scores: dict[int, float] = {n: 0.0 for n in relay_list}
    if relay_scoring == "incident":
        from .cf_tree import compute_edge_importance

        c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)
        epsilon_f = 1e-9 * c_max if c_max > 0.0 else 1e-9
        importance = compute_edge_importance(lp_sol, epsilon=epsilon_f)
        for edge, capacity in graph.capacities.items():
            contrib = importance.get(edge, 0.0) * float(capacity)
            u, v = edge
            if u in scores:
                scores[u] += contrib
            if v in scores:
                scores[v] += contrib

        ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
        return [node for node, _ in ranked[:max_relays]]

    if relay_scoring == "path_flow":
        denom = float(getattr(lp_sol, "f_star", 0.0)) + 1e-12
        for path, flow in getattr(lp_sol, "path_flows", {}).items():
            if flow <= 0.0:
                continue
            internal = set(path[1:-1])
            contrib = float(flow) / denom
            for node in internal:
                if node in scores:
                    scores[node] += contrib
        ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
        return [node for node, _ in ranked[:max_relays]]

    if relay_scoring == "coverage":
        denom = float(getattr(lp_sol, "f_star", 0.0)) + 1e-12
        requested = [t_id for t_id in terminals if t_id is not None]
        requested_set = set(int(t_id) for t_id in requested)

        cover_by_relay: dict[int, dict[int, float]] = {n: {} for n in relay_list}
        for path, flow in getattr(lp_sol, "path_flows", {}).items():
            if flow <= 0.0:
                continue
            dst = int(path[-1])
            if dst not in requested_set:
                continue
            frac = float(flow) / denom
            for node in set(path[1:-1]):
                if node in cover_by_relay:
                    cover_by_relay[node][dst] = cover_by_relay[node].get(dst, 0.0) + frac

        # Cap each relay's per-terminal coverage to [0,1].
        for relay, per_term in cover_by_relay.items():
            for t_id, frac in list(per_term.items()):
                per_term[t_id] = min(1.0, frac)

        covered: dict[int, float] = {t_id: 0.0 for t_id in requested_set}
        fallback_score = {
            relay: sum(per_term.values()) for relay, per_term in cover_by_relay.items()
        }
        selected: list[int] = []
        remaining = set(relay_list)
        eps = 1e-12

        for _ in range(min(max_relays, len(relay_list))):
            best_relay: int | None = None
            best_gain = -1.0
            best_fallback = -1.0
            for relay in sorted(remaining):
                gain = 0.0
                for t_id, frac in cover_by_relay.get(relay, {}).items():
                    cur = covered.get(t_id, 0.0)
                    gain += max(0.0, min(1.0, cur + frac) - cur)
                if gain > best_gain + eps:
                    best_relay = relay
                    best_gain = gain
                    best_fallback = fallback_score.get(relay, 0.0)
                elif best_relay is not None and abs(gain - best_gain) <= eps:
                    fb = fallback_score.get(relay, 0.0)
                    if fb > best_fallback + eps:
                        best_relay = relay
                        best_fallback = fb
                    elif abs(fb - best_fallback) <= eps and relay < best_relay:
                        best_relay = relay

            if best_relay is None:
                break
            selected.append(best_relay)
            remaining.remove(best_relay)
            for t_id, frac in cover_by_relay.get(best_relay, {}).items():
                covered[t_id] = min(1.0, covered.get(t_id, 0.0) + frac)

        return selected

    raise ValueError(f"Unknown relay_scoring: {relay_scoring}")


def _validate_arborescence(
    edges: t.Sequence[Edge],
    *,
    src: int,
    destinations: t.Sequence[int],
    hop_limit: int | None,
) -> str | None:
    """Return an error string if `edges` is not a hop-limited arborescence spanning all destinations."""
    if not edges:
        return "empty tree"

    parent: dict[int, int] = {}
    adj: dict[int, list[int]] = {}
    for u, v in edges:
        if u == v:
            return f"self-loop edge ({u},{v})"
        if v == src:
            return f"edge enters source ({u}->{v})"
        if v in parent and parent[v] != u:
            return f"multiple parents for node {v} ({parent[v]} and {u})"
        parent[v] = u
        adj.setdefault(u, []).append(v)

    depth: dict[int, int] = {src: 0}
    q: deque[int] = deque([src])
    while q:
        u = q.popleft()
        for v in adj.get(u, []):
            if v in depth:
                continue
            depth[v] = depth[u] + 1
            q.append(v)

    for t_id in destinations:
        if t_id == src:
            continue
        if t_id not in depth:
            return f"destination {t_id} unreachable from source {src}"
        if hop_limit is not None and hop_limit > 0 and depth[t_id] > hop_limit:
            return f"destination {t_id} exceeds hop_limit={hop_limit} (depth={depth[t_id]})"

    return None


def load_toml(path: str | Path) -> dict:
    with Path(path).open("rb") as f:
        return tomllib.load(f)


def build_graph_from_controller_config(
    controller_config_path: str | Path,
    *,
    default_capacity: int | None = None,
) -> Graph:
    """Build an LP graph from a Nextmini controller-config.toml.

    We intentionally reuse the controller's topology config so we don't duplicate node IDs/edges.
    """
    cfg = load_toml(controller_config_path)
    topo = dict(cfg.get("topology", {}) or {})
    links = list(cfg.get("link", []) or [])
    if default_capacity is not None:
        topo["default_capacity"] = int(default_capacity)
    return Graph.from_toml({"topology": topo, "link": links})


def compute_mflow_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    relay_nodes: t.Sequence[int] | None = None,
    allow_destinations_as_relays: bool = False,
    max_length: int = -1,
    sort_by: str = "shortest",
    num_paths: int = 2,
) -> tuple[list[Edge], float | None]:
    """Compute a single multicast DAG using the modified mFlow LP solver.

    Returns (edges, throughput). If the solver yields no feasible tree, edges will be empty and
    throughput will be None.
    """
    # Import lazily so users that only need topology parsing don't pay solver import cost.
    from . import mFlow

    terminal_forwarders, relay_candidates = _split_relay_candidates(
        graph,
        src=src,
        destinations=destinations,
        relay_nodes=relay_nodes,
        allow_destinations_as_relays=allow_destinations_as_relays,
    )
    relay_nodes_effective = sorted(set(relay_candidates) | terminal_forwarders)

    allowed_intermediate_nodes = set(relay_nodes_effective)
    allowed_intermediate_nodes.add(src)

    try:
        variables, sol = mFlow.solve(
            graph,
            [src],
            {src: list(destinations)},
            max_length=max_length,
            sort_by=sort_by,
            num_paths=num_paths,
            allowed_intermediate_nodes=allowed_intermediate_nodes,
        )
    except ValueError:
        return [], None

    sources, session_trees = mFlow.convert_to_multicast_trees(variables, sol)
    try:
        idx = sources.index(src)
    except ValueError:
        return [], None

    trees = session_trees[idx] if idx < len(session_trees) else []
    if not trees:
        return [], None

    requested = set(destinations)
    requested.discard(src)

    # Each entry is (list_of_paths, throughput). Only accept trees that span all
    # requested destinations; otherwise return FAIL (empty result).
    feasible: list[tuple[list[list[int]], float]] = []
    for paths, tput in trees:
        covered = {p[-1] for p in paths if p}
        if requested.issubset(covered):
            feasible.append((paths, tput))

    if not feasible:
        return [], None

    best_paths, best_throughput = max(feasible, key=lambda it: it[1])
    edges = mFlow.paths_to_edges(best_paths)
    return edges, best_throughput


def compute_cf_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    hop_limit: int = 3,
    eta: float = 0.1,
    delta: float = 1e-3,
    use_lp_guidance: bool = True,
    lp_backend: str = "lp",
    relay_nodes: t.Sequence[int] | None = None,
    max_relays: int | None = None,
    relay_scoring: str = "coverage",
    allow_destinations_as_relays: bool = False,
    node_egress_budgets: dict[int, float | None] | None = None,
    fanout_caps: dict[int, int | None] | None = None,
    max_length: int = -1,
    sort_by: str = "shortest",
    num_paths: int = 2,
) -> TreeResult:
    """Compute multicast tree using CF-Tree algorithm.

    CF-Tree uses LP-derived edge importance signals to guide tree construction,
    producing hop-limited trees that tend to use high-value edges identified by
    the conceptual-flow LP relaxation.

    Args:
        graph: Network topology
        src: Source node ID
        destinations: List of destination node IDs
        hop_limit: Maximum hops from source (H in paper, typically 2-4)
        eta: LP guidance weight (0 = pure capacity-based, higher = more LP influence)
        delta: Smoothing parameter for log term (default 1e-3)
        use_lp_guidance: If False, use basic capacity-based weights (Basic-Tree)
        max_length: Max path length for LP (-1 = unlimited)
        num_paths: Number of paths per destination for LP

    Returns:
        TreeResult with edges, throughput, algorithm name, and LP f_star metadata
    """
    from . import mFlow
    from .cf_tree import (
        cf_tree_from_lp,
        cf_tree_bottleneck_from_lp,
        cf_tree_direct,
        compute_edge_importance,
        extract_lp_solution,
    )

    if use_lp_guidance and lp_backend == "mwu" and node_egress_budgets:
        if any(budget is not None for budget in node_egress_budgets.values()):
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="cf_tree_mwu",
                lp_f_star=None,
                error="CF-MWU does not support node egress budgets; use lp_backend='lp'.",
            )

    terminals = set(destinations)
    terminals.discard(src)
    terminal_forwarders, relay_candidates = _split_relay_candidates(
        graph,
        src=src,
        destinations=destinations,
        relay_nodes=relay_nodes,
        allow_destinations_as_relays=allow_destinations_as_relays,
    )

    if max_relays is not None and max_relays >= 0:
        if max_relays == 0:
            selected_relays: list[int] = []
        elif max_relays < len(relay_candidates):
            if use_lp_guidance:
                # LP-guided relay selection: score relays by conceptual-flow coverage.
                allowed_intermediate_nodes = set(relay_candidates)
                allowed_intermediate_nodes.update(terminal_forwarders)
                allowed_intermediate_nodes.add(src)

                if lp_backend == "mwu":
                    variables, sol = mFlow.solve_mwu(
                        graph,
                        [src],
                        {src: list(destinations)},
                        max_length=hop_limit,
                        sort_by=sort_by,
                        num_paths=num_paths,
                        allowed_intermediate_nodes=allowed_intermediate_nodes,
                        node_egress_budgets=node_egress_budgets,
                    )
                elif lp_backend == "lp":
                    try:
                        variables, sol = mFlow.solve(
                            graph,
                            [src],
                            {src: list(destinations)},
                            max_length=hop_limit,
                            sort_by=sort_by,
                            num_paths=num_paths,
                            allowed_intermediate_nodes=allowed_intermediate_nodes,
                            node_egress_budgets=node_egress_budgets,
                        )
                    except ValueError as e:
                        return TreeResult(
                            edges=[],
                            throughput=None,
                            algorithm="cf_tree",
                            lp_f_star=None,
                            error=str(e),
                        )
                else:
                    raise ValueError(f"Unknown lp_backend: {lp_backend}")
                lp_sol = extract_lp_solution(graph, variables, sol, src)
                selected_relays = _select_relays_lp_guided(
                    graph,
                    relay_candidates=relay_candidates,
                    terminals=terminals,
                    max_relays=max_relays,
                    relay_scoring=relay_scoring,
                    lp_sol=lp_sol,
                )
            else:
                # Basic-Tree should not depend on LP or solver extras.
                # Use a simple deterministic heuristic: rank relays by outgoing capacity.
                scores = {
                    node: sum(cap for _, cap in graph.adj.get(node, []))
                    for node in relay_candidates
                }

            if use_lp_guidance:
                # Selected above using LP signals.
                selected_relays = list(selected_relays or [])
            else:
                selected = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[
                    :max_relays
                ]
                selected_relays = [node for node, _ in selected]
        else:
            selected_relays = relay_candidates
    else:
        selected_relays = relay_candidates

    if not use_lp_guidance:
        # Basic-Tree: no LP, pure capacity-based
        forwarding_nodes = set(selected_relays)
        forwarding_nodes.update(terminal_forwarders)
        forwarding_nodes.add(src)
        try:
            result = cf_tree_direct(
                graph,
                src,
                destinations,
                hop_limit=hop_limit,
                forwarding_nodes=forwarding_nodes,
                node_egress_budgets=node_egress_budgets,
                fanout_caps=fanout_caps,
            )
        except ValueError as e:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="basic_tree",
                lp_f_star=None,
                error=str(e),
            )
        err = _validate_arborescence(
            result.edges, src=src, destinations=destinations, hop_limit=hop_limit
        )
        if err:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="basic_tree",
                lp_f_star=None,
                error=f"invalid arborescence: {err}",
            )
        return TreeResult(
            edges=result.edges,
            throughput=result.tree_rate,
            algorithm="basic_tree",
            lp_f_star=None,
        )

    # Solve conceptual-flow first (LP or MWU approximation)
    allowed_intermediate_nodes = set(selected_relays)
    allowed_intermediate_nodes.update(terminal_forwarders)
    allowed_intermediate_nodes.add(src)
    if lp_backend == "mwu":
        variables, sol = mFlow.solve_mwu(
            graph,
            [src],
            {src: list(destinations)},
            max_length=max_length if max_length > 0 else hop_limit,
            sort_by=sort_by,
            num_paths=num_paths,
            allowed_intermediate_nodes=allowed_intermediate_nodes,
            node_egress_budgets=node_egress_budgets,
        )
    elif lp_backend == "lp":
        try:
            variables, sol = mFlow.solve(
                graph,
                [src],
                {src: list(destinations)},
                max_length=max_length if max_length > 0 else hop_limit,
                sort_by=sort_by,
                num_paths=num_paths,
                allowed_intermediate_nodes=allowed_intermediate_nodes,
                node_egress_budgets=node_egress_budgets,
            )
        except ValueError as e:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="cf_tree",
                lp_f_star=None,
                error=str(e),
            )
    else:
        raise ValueError(f"Unknown lp_backend: {lp_backend}")

    # Build CF-Tree using LP guidance
    forwarding_nodes = set(selected_relays)
    forwarding_nodes.update(terminal_forwarders)
    forwarding_nodes.add(src)
    algo_name = "cf_tree_mwu" if lp_backend == "mwu" else "cf_tree"
    lp_f_star = None
    x_idx = variables.get("x")
    if x_idx is not None:
        try:
            lp_f_star = float(sol[x_idx])
        except Exception:
            lp_f_star = None

    try:
        result = cf_tree_from_lp(
            graph,
            variables,
            sol,
            src,
            destinations,
            hop_limit=hop_limit,
            eta=eta,
            delta=delta,
            use_lp_guidance=True,
            forwarding_nodes=forwarding_nodes,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
        )
    except ValueError as e:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm=algo_name,
            lp_f_star=lp_f_star,
            error=str(e),
        )

    err = _validate_arborescence(
        result.edges, src=src, destinations=destinations, hop_limit=hop_limit
    )
    if err:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm=algo_name,
            lp_f_star=result.lp_f_star,
            error=f"invalid arborescence: {err}",
        )

    return TreeResult(
        edges=result.edges,
        throughput=result.tree_rate,
        algorithm=algo_name,
        lp_f_star=result.lp_f_star,
    )


def compute_cf_bottleneck_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    hop_limit: int = 3,
    eta: float = 0.1,
    delta: float = 1e-3,
    lp_backend: str = "lp",
    relay_nodes: t.Sequence[int] | None = None,
    max_relays: int | None = None,
    relay_scoring: str = "coverage",
    allow_destinations_as_relays: bool = False,
    node_egress_budgets: dict[int, float | None] | None = None,
    fanout_caps: dict[int, int | None] | None = None,
    max_length: int = -1,
    sort_by: str = "shortest",
    num_paths: int = 2,
) -> TreeResult:
    """Compute multicast tree using bottleneck-aware CF-Tree."""
    from . import mFlow
    from .cf_tree import (
        cf_tree_bottleneck_from_lp,
        compute_edge_importance,
        extract_lp_solution,
    )

    if lp_backend == "mwu" and node_egress_budgets:
        if any(budget is not None for budget in node_egress_budgets.values()):
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="cf_bottleneck_mwu",
                lp_f_star=None,
                error="CF-MWU does not support node egress budgets; use lp_backend='lp'.",
            )

    terminals = set(destinations)
    terminals.discard(src)
    terminal_forwarders, relay_candidates = _split_relay_candidates(
        graph,
        src=src,
        destinations=destinations,
        relay_nodes=relay_nodes,
        allow_destinations_as_relays=allow_destinations_as_relays,
    )

    if max_relays is not None and max_relays >= 0:
        if max_relays == 0:
            selected_relays: list[int] = []
        elif max_relays < len(relay_candidates):
            allowed_intermediate_nodes = set(relay_candidates)
            allowed_intermediate_nodes.update(terminal_forwarders)
            allowed_intermediate_nodes.add(src)

            if lp_backend == "mwu":
                variables, sol = mFlow.solve_mwu(
                    graph,
                    [src],
                    {src: list(destinations)},
                    max_length=hop_limit,
                    sort_by=sort_by,
                    num_paths=num_paths,
                    allowed_intermediate_nodes=allowed_intermediate_nodes,
                    node_egress_budgets=node_egress_budgets,
                )
            elif lp_backend == "lp":
                try:
                    variables, sol = mFlow.solve(
                        graph,
                        [src],
                        {src: list(destinations)},
                        max_length=hop_limit,
                        sort_by=sort_by,
                        num_paths=num_paths,
                        allowed_intermediate_nodes=allowed_intermediate_nodes,
                        node_egress_budgets=node_egress_budgets,
                    )
                except ValueError as e:
                    return TreeResult(
                        edges=[],
                        throughput=None,
                        algorithm="cf_bottleneck",
                        lp_f_star=None,
                        error=str(e),
                    )
                else:
                    raise ValueError(f"Unknown lp_backend: {lp_backend}")
            lp_sol = extract_lp_solution(graph, variables, sol, src)
            selected_relays = _select_relays_lp_guided(
                graph,
                relay_candidates=relay_candidates,
                terminals=terminals,
                max_relays=max_relays,
                relay_scoring=relay_scoring,
                lp_sol=lp_sol,
            )
        else:
            selected_relays = relay_candidates
    else:
        selected_relays = relay_candidates

    allowed_intermediate_nodes = set(selected_relays)
    allowed_intermediate_nodes.update(terminal_forwarders)
    allowed_intermediate_nodes.add(src)
    if lp_backend == "mwu":
        variables, sol = mFlow.solve_mwu(
            graph,
            [src],
            {src: list(destinations)},
            max_length=max_length if max_length > 0 else hop_limit,
            sort_by=sort_by,
            num_paths=num_paths,
            allowed_intermediate_nodes=allowed_intermediate_nodes,
            node_egress_budgets=node_egress_budgets,
        )
    elif lp_backend == "lp":
        try:
            variables, sol = mFlow.solve(
                graph,
                [src],
                {src: list(destinations)},
                max_length=max_length if max_length > 0 else hop_limit,
                sort_by=sort_by,
                num_paths=num_paths,
                allowed_intermediate_nodes=allowed_intermediate_nodes,
                node_egress_budgets=node_egress_budgets,
            )
        except ValueError as e:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="cf_bottleneck",
                lp_f_star=None,
                error=str(e),
            )
    else:
        raise ValueError(f"Unknown lp_backend: {lp_backend}")

    forwarding_nodes = set(selected_relays)
    forwarding_nodes.update(terminal_forwarders)
    forwarding_nodes.add(src)
    algo_name = "cf_bottleneck_mwu" if lp_backend == "mwu" else "cf_bottleneck"
    lp_f_star = None
    x_idx = variables.get("x")
    if x_idx is not None:
        try:
            lp_f_star = float(sol[x_idx])
        except Exception:
            lp_f_star = None

    try:
        result = cf_tree_bottleneck_from_lp(
            graph,
            variables,
            sol,
            src,
            destinations,
            hop_limit=hop_limit,
            eta=eta,
            delta=delta,
            use_lp_guidance=True,
            forwarding_nodes=forwarding_nodes,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
        )
    except ValueError as e:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm=algo_name,
            lp_f_star=lp_f_star,
            error=str(e),
        )

    err = _validate_arborescence(
        result.edges, src=src, destinations=destinations, hop_limit=hop_limit
    )
    if err:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm=algo_name,
            lp_f_star=result.lp_f_star,
            error=f"invalid arborescence: {err}",
        )

    return TreeResult(
        edges=result.edges,
        throughput=result.tree_rate,
        algorithm=algo_name,
        lp_f_star=result.lp_f_star,
    )


def compute_basic_bottleneck_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    hop_limit: int = 3,
    relay_nodes: t.Sequence[int] | None = None,
    max_relays: int | None = None,
    allow_destinations_as_relays: bool = False,
    node_egress_budgets: dict[int, float | None] | None = None,
    fanout_caps: dict[int, int | None] | None = None,
) -> TreeResult:
    """Compute bottleneck-sweep Basic-Tree (no LP).

    This is the natural baseline that isolates the effect of the bottleneck sweep
    from LP-derived importance signals.
    """
    from .cf_tree import build_cf_tree_bottleneck, compute_basic_weights

    terminals = set(destinations)
    terminals.discard(src)
    terminal_forwarders, relay_candidates = _split_relay_candidates(
        graph,
        src=src,
        destinations=destinations,
        relay_nodes=relay_nodes,
        allow_destinations_as_relays=allow_destinations_as_relays,
    )

    if max_relays is not None and max_relays >= 0:
        if max_relays == 0:
            selected_relays: list[int] = []
        elif max_relays < len(relay_candidates):
            selected_relays = relay_candidates[:max_relays]
        else:
            selected_relays = relay_candidates
    else:
        selected_relays = relay_candidates

    forwarding_nodes = set(selected_relays)
    forwarding_nodes.update(terminal_forwarders)
    forwarding_nodes.add(src)

    weights = compute_basic_weights(graph)
    try:
        edges, _, achieved = build_cf_tree_bottleneck(
            graph,
            weights,
            src,
            destinations,
            hop_limit,
            forwarding_nodes=forwarding_nodes,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
        )
    except ValueError as e:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm="basic_bottleneck",
            lp_f_star=None,
            error=str(e),
        )

    err = _validate_arborescence(edges, src=src, destinations=destinations, hop_limit=hop_limit)
    if err:
        return TreeResult(
            edges=[],
            throughput=None,
            algorithm="basic_bottleneck",
            lp_f_star=None,
            error=f"invalid arborescence: {err}",
        )

    return TreeResult(
        edges=edges,
        throughput=achieved,
        algorithm="basic_bottleneck",
        lp_f_star=None,
    )


def compute_star_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
) -> TreeResult:
    """Compute a depth-1 star (direct fanout) baseline tree.

    This baseline installs one edge from src to each destination and does not use relays.
    """
    terminals = [t for t in destinations if t != src]
    edges: list[Edge] = []
    for t_id in terminals:
        if (src, t_id) not in graph.capacities:
            return TreeResult(edges=[], throughput=None, algorithm="star", lp_f_star=None)
        edges.append((src, t_id))

    if not edges:
        return TreeResult(edges=[], throughput=None, algorithm="star", lp_f_star=None)

    throughput = min(graph.capacities[e] for e in edges)
    return TreeResult(edges=edges, throughput=throughput, algorithm="star", lp_f_star=None)


def compute_two_level_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    relay_nodes: t.Sequence[int] | None = None,
    max_relays: int | None = None,
    allow_destinations_as_relays: bool = False,
) -> TreeResult:
    """Compute a simple 2-level hierarchy baseline (src -> relay -> terminals).

    We select up to `max_relays` relay candidates, assign each terminal either directly from
    src or via the relay that maximizes the bottleneck min(C(src, r), C(r, t)).

    This is a heuristic baseline intended to resemble common hierarchical fanout designs.
    """
    terminals = [t_id for t_id in destinations if t_id != src]
    if not terminals:
        return TreeResult(edges=[], throughput=None, algorithm="two_level", lp_f_star=None)

    terminal_forwarders, relay_candidates = _split_relay_candidates(
        graph,
        src=src,
        destinations=destinations,
        relay_nodes=relay_nodes,
        allow_destinations_as_relays=allow_destinations_as_relays,
    )

    k = len(relay_candidates) if max_relays is None else max(0, int(max_relays))
    chosen_nonterminal_relays: list[int] = []
    if k > 0 and relay_candidates:
        # Only relays that are reachable from src and can reach at least one terminal help.
        scored: list[tuple[int, float]] = []
        for r_id in relay_candidates:
            cap_sr = float(graph.capacities.get((src, r_id), 0.0))
            if cap_sr <= 0.0:
                continue
            score = 0.0
            for t_id in terminals:
                cap_rt = float(graph.capacities.get((r_id, t_id), 0.0))
                if cap_rt > 0.0:
                    score += min(cap_sr, cap_rt)
            if score > 0.0:
                scored.append((r_id, score))

        scored.sort(key=lambda kv: kv[1], reverse=True)
        chosen_nonterminal_relays = [r_id for r_id, _ in scored[: min(k, len(scored))]]

    chosen_relays: list[int] = sorted(set(chosen_nonterminal_relays) | terminal_forwarders)
    if not chosen_relays:
        return compute_star_tree_edges(graph, src=src, destinations=destinations)

    parent: dict[int, int] = {}
    used_relays: set[int] = set()

    for t_id in terminals:
        best_parent = src
        best_bottleneck = float(graph.capacities.get((src, t_id), 0.0))

        for r_id in chosen_relays:
            if r_id == t_id:
                continue
            cap_sr = float(graph.capacities.get((src, r_id), 0.0))
            cap_rt = float(graph.capacities.get((r_id, t_id), 0.0))
            if cap_sr <= 0.0 or cap_rt <= 0.0:
                continue
            bottleneck = min(cap_sr, cap_rt)
            if bottleneck > best_bottleneck:
                best_parent = r_id
                best_bottleneck = bottleneck

        if best_bottleneck <= 0.0:
            return TreeResult(edges=[], throughput=None, algorithm="two_level", lp_f_star=None)

        parent[t_id] = best_parent
        if best_parent != src:
            used_relays.add(best_parent)

    # If a destination is used as a relay, ensure it is a direct child of the
    # source. Otherwise we'd create two parents once we add src->relay.
    for r_id in sorted(used_relays):
        if r_id in terminal_forwarders:
            parent[r_id] = src

    edges: list[Edge] = [(parent[t_id], t_id) for t_id in terminals]

    for r_id in sorted(used_relays):
        if r_id in terminal_forwarders:
            continue
        edges.append((src, r_id))

    throughput = min(float(graph.capacities.get(e, 0.0)) for e in edges) if edges else 0.0
    return TreeResult(edges=edges, throughput=throughput, algorithm="two_level", lp_f_star=None)


def compute_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    algorithm: str = "mflow",
    hop_limit: int = 3,
    eta: float = 0.1,
    relay_nodes: t.Sequence[int] | None = None,
    max_relays: int | None = None,
    relay_scoring: str = "coverage",
    allow_destinations_as_relays: bool = False,
    node_egress_budgets: dict[int, float | None] | None = None,
    fanout_caps: dict[int, int | None] | None = None,
    **kwargs,
) -> TreeResult:
    """Unified interface for computing multicast tree edges.

    Args:
        graph: Network topology
        src: Source node ID
        destinations: List of destination node IDs
        algorithm: "mflow", "cf_tree", "cf_tree_mwu", "cf_bottleneck", "cf_bottleneck_mwu", "basic_tree", "basic_bottleneck", "star", or "two_level"
        hop_limit: Maximum hops (for cf_tree/basic_tree)
        eta: LP guidance weight (for cf_tree)
        relay_scoring: Relay scoring mode when `max_relays` is set ("incident", "path_flow", or "coverage")
        **kwargs: Additional arguments passed to underlying algorithm

    Returns:
        TreeResult with edges and metadata
    """
    if algorithm == "mflow":
        edges, throughput = compute_mflow_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            relay_nodes=relay_nodes,
            allow_destinations_as_relays=allow_destinations_as_relays,
            **kwargs,
        )
        if not edges or throughput is None:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="mflow",
                lp_f_star=None,
                error="mFlow returned no full-coverage tree for this request.",
            )
        err = _validate_arborescence(edges, src=src, destinations=destinations, hop_limit=None)
        if err:
            return TreeResult(
                edges=[],
                throughput=None,
                algorithm="mflow",
                lp_f_star=None,
                error=f"invalid arborescence: {err}",
            )
        return TreeResult(edges=edges, throughput=throughput, algorithm="mflow")
    elif algorithm == "cf_tree":
        return compute_cf_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            eta=eta,
            use_lp_guidance=True,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
            **kwargs,
        )
    elif algorithm == "cf_tree_mwu":
        return compute_cf_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            eta=eta,
            use_lp_guidance=True,
            lp_backend="mwu",
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
            **kwargs,
        )
    elif algorithm == "cf_bottleneck":
        return compute_cf_bottleneck_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            eta=eta,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
            **kwargs,
        )
    elif algorithm == "cf_bottleneck_mwu":
        return compute_cf_bottleneck_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            eta=eta,
            lp_backend="mwu",
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
            **kwargs,
        )
    elif algorithm == "basic_tree":
        return compute_cf_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            use_lp_guidance=False,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            relay_scoring=relay_scoring,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
            **kwargs,
        )
    elif algorithm == "basic_bottleneck":
        return compute_basic_bottleneck_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            allow_destinations_as_relays=allow_destinations_as_relays,
            node_egress_budgets=node_egress_budgets,
            fanout_caps=fanout_caps,
        )
    elif algorithm == "star":
        return compute_star_tree_edges(graph, src=src, destinations=destinations)
    elif algorithm == "two_level":
        return compute_two_level_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            relay_nodes=relay_nodes,
            max_relays=max_relays,
            allow_destinations_as_relays=allow_destinations_as_relays,
        )
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
