"""Convert LP path throughput outputs into multicast trees."""

from __future__ import annotations

import typing as t


def convert_to_multicast_trees(
    variables: t.Dict[str, int],
    sol: t.List[float],
) -> t.Tuple[t.List[int], t.List[t.List[t.Tuple[t.List[t.List[int]], float]]]]:
    """
    Convert LP solution to multicast trees.

    Returns:
        (sources, session_trees)

        - sources: list of unique source node IDs (sorted, deterministic)
        - session_trees: one entry per source; each entry is a list of (tree_paths, throughput)
          where tree_paths is a list of node-paths (each a list[int]).
    """
    # all paths with nonzero throughput
    paths: list[tuple[list[int], float]] = []
    for var, idx in variables.items():
        if round(sol[idx], 5) != 0 and var.startswith("p"):
            path_str = var.split("_")[1:]
            path = list(map(int, path_str))

            throughput = round(sol[idx], 5)
            paths.append((path, throughput))

    # All sessions, indexed by src (deterministic order).
    sources = sorted({path[0] for path, _ in paths})

    # List to store resulting multicast trees
    session_trees: list[list[tuple[list[list[int]], float]]] = []

    # Group conceptual flows in each session into multicast trees
    for src in sources:
        # Conceptual flows in session src
        current = [(path, throughput) for path, throughput in paths if path[0] == src]
        # Sort by throughput descending
        current = sorted(current, key=lambda x: x[1], reverse=True)

        trees: list[tuple[list[list[int]], float]] = []

        while len(current) > 0:
            path, throughput = current.pop(0)

            # Initialize a new tree with the base path
            # We track parents to ensure tree property (each node has exactly one parent)
            # parent_map: {node: parent_node}
            parent_map: dict[int, int] = {}
            for i in range(len(path) - 1):
                u, v = path[i], path[i + 1]
                parent_map[v] = u

            dst_set = {path[-1]}
            candidate_tree = [path]

            for candidate_path, candidate_throughput in current:
                # Only merge paths that can carry this tree's throughput.
                #
                # We build trees in descending-throughput order. To keep each tree feasible
                # as a single-rate multicast, every included path must have residual
                # throughput >= the tree rate. With the descending order, this effectively
                # restricts merging to equal-throughput paths (up to rounding).
                if candidate_throughput + 1e-9 < throughput:
                    continue
                # Check 1: Must NOT share destination (multicast tree delivers to unique dests)
                if candidate_path[-1] in dst_set:
                    continue

                # Check 2: Tree Consistency (Single Parent Rule)
                # Verify that merging this path doesn't give any node a SECOND, DIFFERENT parent.
                compatible = True

                # We need to temporarily traverse to check validity without modifying state yet
                for i in range(len(candidate_path) - 1):
                    u, v = candidate_path[i], candidate_path[i + 1]
                    if v in parent_map and parent_map[v] != u:
                        # Conflict: Node v is already in the tree but with a different parent!
                        compatible = False
                        break

                if not compatible:
                    continue

                # If compatible, add to tree
                dst_set.add(candidate_path[-1])
                candidate_tree.append(candidate_path)

                # Update parent_map with new edges
                for i in range(len(candidate_path) - 1):
                    u, v = candidate_path[i], candidate_path[i + 1]
                    if v not in parent_map:
                        parent_map[v] = u

            trees.append((candidate_tree, throughput))

            # Pruning operation
            # Iterate backwards to safely remove items
            for i in range(len(current) - 1, -1, -1):
                p, tput = current[i]
                if p not in candidate_tree:
                    continue
                tput -= throughput
                if round(tput, 5) <= 0:  # Robust check for float zero
                    current.pop(i)
                else:
                    current[i] = (p, tput)

        session_trees.append(trees)

    return sources, session_trees


def paths_to_edges(paths: t.List[t.List[int]]) -> t.List[t.Tuple[int, int]]:
    """Convert list of paths to list of unique directed edges (sorted for determinism)."""
    edges: set[tuple[int, int]] = set()
    for path in paths:
        for i in range(len(path) - 1):
            edges.add((path[i], path[i + 1]))
    return sorted(edges)
