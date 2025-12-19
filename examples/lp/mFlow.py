"""
Modified mFlow LP solver for multicast tree computation.
Adapted from examples/sqaure/experiments/optimization/mFlow.py

Reference: https://iqua.ece.toronto.edu/papers/cflow-infocom05.pdf
"""
import typing as t
import numpy as np
from collections import defaultdict

from graph import Graph

# Try to import solvers, provide helpful error if missing
try:
    from cvxopt import matrix, solvers
    CVXOPT_AVAILABLE = True
except ImportError:
    CVXOPT_AVAILABLE = False
    print("Warning: cvxopt not installed. Install with: pip install cvxopt")

try:
    import mosek
    MOSEK_AVAILABLE = True
except ImportError:
    MOSEK_AVAILABLE = False
    print("Warning: mosek not installed. Using GLPK solver instead.")


def solve(
    graph: Graph,
    sources: t.List[int],
    destinations: t.Dict[int, t.List[int]],
    max_length: int = -1,
    sort_by: str = "shortest",
    num_paths: int = 2,
) -> t.Tuple[t.Dict[str, int], t.List[float]]:
    """
    Solve the modified mFlow LP.
    
    Args:
        graph: Network topology
        sources: List of source node IDs
        destinations: Dict mapping source -> list of destination node IDs
        max_length: Maximum path length to consider
        sort_by: Path selection strategy ("shortest" or "random")
        num_paths: Number of paths to consider per (src, dst) pair
        
    Returns:
        Tuple of (variable_name_to_index, solution_values)
    """
    if not CVXOPT_AVAILABLE:
        raise ImportError("cvxopt is required for LP solving")
    
    # Get all paths
    paths = graph.get_paths(sources, destinations, max_length=max_length)
    
    # Filter paths
    if sort_by == "shortest":
        for flow in paths:
            paths[flow].sort(key=lambda x: len(x))
            paths[flow] = paths[flow][:num_paths]
    elif sort_by == "random":
        for flow in paths:
            np.random.shuffle(paths[flow])
            paths[flow] = paths[flow][:num_paths]
    else:
        raise ValueError(f"Invalid sort_by: {sort_by}")

    ###################################################
    # Variables
    ###################################################
    variable_counter = 0
    variables = {}
    
    # x: max min multicast flow
    variables["x"] = 0
    variable_counter += 1

    # p_n1_..._nk: throughput of path n1 -> ... -> nk
    for _, all_paths in paths.items():
        for path in all_paths:
            name = f"p_{'_'.join(map(str, path))}"
            variables[name] = variable_counter
            variable_counter += 1

    # e_src_n1_n2: capacity of edge n1 -> n2 for multicast session from src
    src_dst_n1_n2_to_path = defaultdict(list)
    for (src, dst), all_paths in paths.items():
        edges = set()
        for path in all_paths:
            for i in range(len(path) - 1):
                n1, n2 = path[i], path[i + 1]
                edges.add((n1, n2))
                src_dst_n1_n2_to_path[(src, dst, n1, n2)].append(path)
        for (n1, n2) in edges:
            name = f"e_{src}_{n1}_{n2}"
            if name in variables:
                continue
            variables[name] = variable_counter
            variable_counter += 1

    ###################################################
    # Constraints
    ###################################################
    G_rows = []
    h_rows = []

    # For all (src, dst) pairs, total throughput >= x
    for _, all_paths in paths.items():
        names = [f"p_{'_'.join(map(str, path))}" for path in all_paths]
        columns = [variables[name] for name in names]

        row = np.zeros(variable_counter, dtype=np.float64)
        row[variables["x"]] = 1
        row[columns] = -1
        G_rows.append(row)
        h_rows.append(0)

    # Path throughput <= edge capacity
    for (src, dst, n1, n2), path_list in src_dst_n1_n2_to_path.items():
        names = [f"p_{'_'.join(map(str, path))}" for path in path_list]
        columns = [variables[name] for name in names]

        row = np.zeros(variable_counter, dtype=np.float64)
        row[columns] = 1
        row[variables[f"e_{src}_{n1}_{n2}"]] = -1
        G_rows.append(row)
        h_rows.append(0)

    # Edge capacity constraints
    for (n1, n2), capacity in graph.capacities.items():
        names = [f"e_{src}_{n1}_{n2}" for src in sources if f"e_{src}_{n1}_{n2}" in variables]
        if len(names) == 0:
            continue
        columns = [variables[name] for name in names]
        row = np.zeros(variable_counter, dtype=np.float64)
        row[columns] = 1
        G_rows.append(row)
        h_rows.append(capacity)

    G = np.array(G_rows, dtype=np.float64)
    h = np.array(h_rows, dtype=np.float64)
        
    # All variables >= 0
    G = np.vstack([G, -np.eye(variable_counter)])
    h = np.hstack([h, np.zeros(variable_counter)])

    # Objective: maximize x
    c = np.zeros(variable_counter, dtype=np.float64)
    c[variables["x"]] = -1

    c = matrix(c)
    G = matrix(G)
    h = matrix(h)

    # Solve LP
    solvers.options['show_progress'] = False
    if MOSEK_AVAILABLE:
        try:
            sol = list(solvers.lp(c, G, h, solver="mosek")["x"])
        except Exception as e:
            # MOSEK license error or other runtime failure, fall back to GLPK
            print(f"Warning: MOSEK failed ({e}), falling back to GLPK")
            sol = list(solvers.lp(c, G, h, solver="glpk")["x"])
    else:
        sol = list(solvers.lp(c, G, h, solver="glpk")["x"])
    
    return variables, sol


def convert_to_multicast_trees(
    variables: t.Dict[str, int],
    sol: t.List[float],
) -> t.Tuple[t.List[int], t.List[t.List[t.Tuple[t.List[t.List[int]], float]]]]:
    """
    Convert LP solution to multicast trees.
    
    This implementation uses a consistency check
    to merge paths into trees, rather than a simple heuristic.
    """
    # all paths with nonzero throughput
    paths = []
    for var, idx in variables.items():
        if round(sol[idx], 5) != 0 and var.startswith('p'):
            path_str = var.split('_')[1:]
            path = list(map(int, path_str))
           
            throughput = round(sol[idx], 5)
            paths.append((path, throughput))

    # All sessions, indexed by src
    # We use a set first to get unique sources, then sort to ensure deterministic order if needed
    sources = list(set([path[0] for path, _ in paths]))
    
    # List to store resulting multicast trees
    session_trees = []
    
    # Group conceptual flows in each session into multicast trees
    for src in sources:
        # Conceptual flows in session src
        current = [(path, throughput) for path, throughput in paths if path[0] == src]
        # Sort by throughput descending
        current = sorted(current, key=lambda x: x[1], reverse=True)

        trees = []
        
        while len(current) > 0:
            path, throughput = current.pop()

            # Initialize a new tree with the base path
            # We track parents to ensure tree property (each node has exactly one parent)
            # parent_map: {node: parent_node}
            parent_map = {}
            for i in range(len(path) - 1):
                u, v = path[i], path[i+1]
                parent_map[v] = u
            
            dst_set = {path[-1]}
            candidate_tree = [path]
            
            for candidate_path, candidate_throughput in current:
                if candidate_throughput < throughput:
                    continue
                
                # Check 1: Must NOT share destination (multicast tree delivers to unique dests)
                if candidate_path[-1] in dst_set:
                    continue
                
                # Check 2: Tree Consistency (Single Parent Rule)
                # Verify that merging this path doesn't give any node a SECOND, DIFFERENT parent.
                compatible = True
                
                # We need to temporarily traverse to check validity without modifying state yet
                for i in range(len(candidate_path) - 1):
                    u, v = candidate_path[i], candidate_path[i+1]
                    if v in parent_map:
                        if parent_map[v] != u:
                            # Conflict: Node v is already in the tree but with a different parent!
                            # This would create a "diamond" or cycle, invalidating the tree structure.
                            compatible = False
                            break
                    # If v is not in parent_map, it's a new branch, which is fine.
                
                if not compatible:
                    continue

                # If compatible, add to tree
                dst_set.add(candidate_path[-1])
                candidate_tree.append(candidate_path)
                
                # Update parent_map with new edges
                for i in range(len(candidate_path) - 1):
                    u, v = candidate_path[i], candidate_path[i+1]
                    if v not in parent_map:
                         parent_map[v] = u

            trees.append((candidate_tree, throughput))

            # Pruning operation
            # Iterate backwards to safely remove items
            for i in range(len(current) - 1, -1, -1):
                p, t = current[i]
                if p not in candidate_tree:
                    continue
                t -= throughput
                if round(t, 5) <= 0: # Robust check for float zero
                    current.pop(i)
                else:
                    current[i] = (p, t)

        session_trees.append(trees)

    return sources, session_trees


def paths_to_edges(paths: t.List[t.List[int]]) -> t.List[t.Tuple[int, int]]:
    """Convert list of paths to list of unique edges."""
    edges = set()
    for path in paths:
        for i in range(len(path) - 1):
            edges.add((path[i], path[i + 1]))
    return list(edges)
