"""
Modified mFlow LP solver for multicast tree computation.
Adapted from examples/sqaure/experiments/optimization/mFlow.py

Reference: https://iqua.ece.toronto.edu/papers/cflow-infocom05.pdf
"""
import typing as t
import numpy as np
from collections import defaultdict

try:
    # Package-friendly imports (e.g. `python -m examples.lp.main`).
    from .graph import Graph
    from .tree_conversion import convert_to_multicast_trees, paths_to_edges
except ImportError:  # pragma: no cover
    # Fallback for running as a script from inside this directory.
from graph import Graph
    from tree_conversion import convert_to_multicast_trees, paths_to_edges

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
    solvers.options["show_progress"] = False
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


## NOTE: convert_to_multicast_trees() + paths_to_edges() are imported from `tree_conversion.py`.
