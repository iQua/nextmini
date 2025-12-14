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
    threshold: float = 1e-5,
) -> t.Tuple[t.List[int], t.List[t.List[t.Tuple[t.List[t.List[int]], float]]]]:
    """
    Convert LP solution to multicast trees.
    
    Adapted from: examples/sqaure/experiments/utils/generate_config.py::convert_mFlow_result
    
    NOTE: Tree merging assumes max_length <= 3 for correctness.
    The original code comments state:
        "lemma: need num hops <= 3 for conversion algorithm to 100% match
        the theoretical throughput. If not, please run the checker script
        to ensure approximation is correct."
    
    Returns:
        Tuple of (sources, session_trees)
        where session_trees[i] = list of (tree_edges, throughput) for source i
    """
    # Extract paths with nonzero throughput
    paths = []
    for var, idx in variables.items():
        if round(sol[idx], 5) > threshold and var.startswith('p'):
            path_str = var.split('_')[1:]
            path = list(map(int, path_str))
            throughput = round(sol[idx], 5)
            paths.append((path, throughput))

    # Group by source
    sources = list(set([path[0] for path, _ in paths]))
    session_trees = []
    
    for src in sources:
        current = [(path, throughput) for path, throughput in paths if path[0] == src]
        current = sorted(current, key=lambda x: x[1], reverse=True)
        
        trees = []
        while len(current) > 0:
            path, throughput = current.pop()
            
            # Build tree from paths with same second node
            dst_set = {path[-1]}
            candidate_tree = [path]
            
            for candidate_path, candidate_throughput in current:
                if candidate_throughput < throughput:
                    continue
                if len(candidate_path) > 1 and len(path) > 1 and candidate_path[1] != path[1]:
                    continue
                if candidate_path[-1] in dst_set:
                    continue

                dst_set.add(candidate_path[-1])
                candidate_tree.append(candidate_path)

            trees.append((candidate_tree, throughput))

            # Prune used paths
            for i in range(len(current) - 1, -1, -1):
                p, t = current[i]
                if p not in candidate_tree:
                    continue
                t -= throughput
                if round(t, 5) <= 0:
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
