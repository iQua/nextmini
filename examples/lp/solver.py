"""Multicast tree helpers for Nextmini.

Design goal: be importable from other examples (notably `examples/rl`) without requiring DB access.
"""

from __future__ import annotations

import typing as t
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from .graph import Graph

Edge = tuple[int, int]


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
