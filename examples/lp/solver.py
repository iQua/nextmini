"""Multicast tree helpers for Nextmini.

Design goal: be importable from other examples (notably `examples/rl`) without requiring DB access.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass
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
    lp_upper_bound: float | None = None


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
    if default_capacity is not None:
        topo["default_capacity"] = int(default_capacity)
    return Graph.from_toml({"topology": topo})


def compute_mflow_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
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

    variables, sol = mFlow.solve(
        graph,
        [src],
        {src: list(destinations)},
        max_length=max_length,
        sort_by=sort_by,
        num_paths=num_paths,
    )

    sources, session_trees = mFlow.convert_to_multicast_trees(variables, sol)
    try:
        idx = sources.index(src)
    except ValueError:
        return [], None

    trees = session_trees[idx] if idx < len(session_trees) else []
    if not trees:
        return [], None

    # Each entry is (list_of_paths, throughput).
    # Prefer trees that cover more requested destinations, then highest throughput.
    requested = set(destinations)
    requested.discard(src)

    def score(item: tuple[list[list[int]], float]) -> tuple[int, float]:
        paths, tput = item
        covered = {p[-1] for p in paths if p}
        return (len(covered & requested), tput)

    best_paths, best_throughput = max(trees, key=score)
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
    max_length: int = -1,
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
        TreeResult with edges, throughput, algorithm name, and LP upper bound
    """
    from . import mFlow
    from .cf_tree import cf_tree_from_lp, cf_tree_direct

    if not use_lp_guidance:
        # Basic-Tree: no LP, pure capacity-based
        result = cf_tree_direct(graph, src, destinations, hop_limit=hop_limit)
        return TreeResult(
            edges=result.edges,
            throughput=result.tree_rate,
            algorithm="basic_tree",
            lp_upper_bound=None,
        )

    # Solve LP first
    variables, sol = mFlow.solve(
        graph,
        [src],
        {src: list(destinations)},
        max_length=max_length if max_length > 0 else hop_limit,
        num_paths=num_paths,
    )

    # Build CF-Tree using LP guidance
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
    )

    return TreeResult(
        edges=result.edges,
        throughput=result.tree_rate,
        algorithm="cf_tree",
        lp_upper_bound=result.lp_upper_bound,
    )


def compute_tree_edges(
    graph: Graph,
    *,
    src: int,
    destinations: t.Sequence[int],
    algorithm: str = "mflow",
    hop_limit: int = 3,
    eta: float = 0.1,
    **kwargs,
) -> TreeResult:
    """Unified interface for computing multicast tree edges.

    Args:
        graph: Network topology
        src: Source node ID
        destinations: List of destination node IDs
        algorithm: "mflow" (original), "cf_tree" (LP-guided), or "basic_tree"
        hop_limit: Maximum hops (for cf_tree/basic_tree)
        eta: LP guidance weight (for cf_tree)
        **kwargs: Additional arguments passed to underlying algorithm

    Returns:
        TreeResult with edges and metadata
    """
    if algorithm == "mflow":
        edges, throughput = compute_mflow_tree_edges(
            graph, src=src, destinations=destinations, **kwargs
        )
        return TreeResult(
            edges=edges,
            throughput=throughput,
            algorithm="mflow",
        )
    elif algorithm == "cf_tree":
        return compute_cf_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            eta=eta,
            use_lp_guidance=True,
            **kwargs,
        )
    elif algorithm == "basic_tree":
        return compute_cf_tree_edges(
            graph,
            src=src,
            destinations=destinations,
            hop_limit=hop_limit,
            use_lp_guidance=False,
            **kwargs,
        )
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
