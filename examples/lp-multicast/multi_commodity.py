"""
Multi-commodity flow LP solver.
Adapted from examples/sqaure/experiments/optimization/multi_commodity.py
"""
import typing as t
import numpy as np
from collections import defaultdict

from graph import Graph

# Try to import solvers
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
    Objective: max min intake of dst
    """
    ###################################################
    # Preprocessing
    ###################################################
    all_paths = graph.get_paths(sources, destinations, max_length=max_length)

    # assert max_length == -1
    if sort_by == "shortest":
        for flow in all_paths:
            all_paths[flow].sort(key=lambda x: len(x))
            all_paths[flow] = all_paths[flow][:num_paths]
    elif sort_by == "random":
        for flow in all_paths:
            np.random.shuffle(all_paths[flow])
            all_paths[flow] = all_paths[flow][:num_paths]
    elif sort_by == "max bandwidth":
        raise NotImplementedError
    else:
        raise ValueError(f"Invalid sort_by: {sort_by}")

    all_flows = []
    for src in sources:
        for dst in destinations[src]:
            all_flows.append((src, dst))

    ###################################################
    # Variables
    ###################################################
    # variable name to column index
    variable_counter = 0
    variables = {} 
    # x: max min single commodity flow
    variables["x"] = variable_counter
    variable_counter += 1
    # f_n1_n2_k: flow for edge n1 -> n2 for commodity k
    for (n1, n2) in graph.edges:
        for k in range(len(all_flows)):
            name = f"f_{n1}_{n2}_{k}"
            variables[name] = variable_counter
            variable_counter += 1
    # p_n1_..._nm_k: throughput for path n1 -> ... -> nm for commodity k
    edge_to_paths = defaultdict(list)
    for k, (src, dst) in enumerate(all_flows):
        for path in all_paths[(src, dst)]:
            for i in range(len(path) - 1):
                n1, n2 = path[i], path[i + 1]
                edge_to_paths[(n1, n2, k)].append(path)
            name = f"p_{'_'.join(map(str, path))}_{k}"
            variables[name] = variable_counter
            variable_counter += 1

    ###################################################
    # Constraints
    ###################################################
    # Assert edge capacities make sense for undirected graph
    for (n1, n2) in graph.edges:
        if (n2, n1) not in graph.edges:
            continue
        # assert graph.capacities[(n1, n2)] == graph.capacities[(n2, n1)]

    G_rows = []
    h_rows = []

    # 0. Assign throughputs to paths
    for (n1, n2) in graph.edges:
        for k in range(len(all_flows)):
            # f_n1_n2_k = sum of paths that use directed edge n1 -> n2
            flow_name = f"f_{n1}_{n2}_{k}"
            path_name = [
                f"p_{'_'.join(map(str, path))}_{k}" 
                for path in edge_to_paths[(n1, n2, k)]
            ]
            flow_column = variables[flow_name]
            path_columns = [variables[name] for name in path_name]
            # <= 
            row = np.zeros(variable_counter, dtype=np.float64)
            row[flow_column] = -1
            row[path_columns] = 1
            G_rows.append(row)
            h_rows.append(0)
            # >=
            row = np.zeros(variable_counter, dtype=np.float64)
            row[flow_column] = 1
            row[path_columns] = -1
            G_rows.append(row)
            h_rows.append(0)
            
    # 1. Directed edge capacity constraints
    for (n1, n2), capacity in graph.capacities.items():
        names = []
        for k in range(len(all_flows)):
            names.append(f"f_{n1}_{n2}_{k}")
        columns = [variables[name] for name in names]
        row = np.zeros(variable_counter, dtype=np.float64)
        row[columns] = 1
        G_rows.append(row)
        h_rows.append(capacity)

    # 2. Network flow constraints
    in_edges = defaultdict(list)
    out_edges = defaultdict(list)
    for (n1, n2) in graph.edges:
        in_edges[n2].append((n1, n2))
        out_edges[n1].append((n1, n2))

    all_destinations = []
    for src in sources:
        all_destinations.extend(destinations[src])

    for k in range(len(all_flows)):
        for node in graph.nodes:
            if node in sources or node in all_destinations:
                continue
            row_1 = np.zeros(variable_counter, dtype=np.float64)
            row_2 = np.zeros(variable_counter, dtype=np.float64)

            in_names = [f"f_{n1}_{n2}_{k}" for (n1, n2) in in_edges[node]]
            in_columns = [variables[name] for name in in_names]
            out_names = [f"f_{n1}_{n2}_{k}" for (n1, n2) in out_edges[node]]
            out_columns = [variables[name] for name in out_names]

            row_1[in_columns] = 1
            row_1[out_columns] = -1
            row_2[in_columns] = -1
            row_2[out_columns] = 1

            G_rows.append(row_1)
            h_rows.append(0)
            G_rows.append(row_2)
            h_rows.append(0)

    # 3. x <= destination arrival rates 
    for k, (src, dst) in enumerate(all_flows):
        names = [f"f_{n1}_{n2}_{k}" for (n1, n2) in in_edges[dst]]
        columns = [variables[name] for name in names]
        row = np.zeros(variable_counter, dtype=np.float64)
        row[variables["x"]] = 1
        row[columns] = -1
        G_rows.append(row)
        h_rows.append(0)

    # 4. variables >= 0
    G = np.array(G_rows, dtype=np.float64)
    h = np.array(h_rows, dtype=np.float64)
    G = np.vstack([G, -np.eye(variable_counter)])
    h = np.hstack([h, np.zeros(variable_counter)])

    # objective
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


def convert_mc_to_trees(
    variables: t.Dict[str, int],
    sol: t.List[float],
    threshold: float = 1e-5,
) -> t.Tuple[t.List[int], t.List[t.List[t.Tuple[t.List[t.List[int]], float]]]]:
    """
    Convert multi_commodity LP solution to multicast trees.
    
    Adapted from: examples/sqaure/experiments/utils/generate_config_v2.py::with_multi_commodity
    
    NOTE: multi_commodity uses variable format p_n1_..._nm_k where k is the flow index.
    We need to strip the trailing _k to get the actual path.
    
    Returns:
        Tuple of (sources, session_trees)
        where session_trees[i] = list of (tree_paths, throughput) for source i
    """
    # Extract paths with nonzero throughput
    # Variable format: p_n1_n2_..._nm_k where k is flow index (0, 1, 2, ...)
    paths = []
    for var, idx in variables.items():
        if round(sol[idx], 5) > threshold and var.startswith('p'):
            path_str = var.split('_')[1:]
            # Last element is flow_id, not part of path
            path = list(map(int, path_str[:-1]))  # Remove trailing k
            throughput = round(sol[idx], 5)
            paths.append((path, throughput))

    if not paths:
        return [], []

    # Group by source
    sources = list(set([path[0] for path, _ in paths]))
    session_trees = []
    
    for src in sources:
        current = [(path, throughput) for path, throughput in paths if path[0] == src]
        current = sorted(current, key=lambda x: x[1], reverse=True)
        
        trees = []
        while len(current) > 0:
            path, throughput = current.pop()
            
            # Build tree from paths with same second node (if they have one)
            dst_set = {path[-1]}
            candidate_tree = [path]
            
            for candidate_path, candidate_throughput in current:
                if candidate_throughput < throughput:
                    continue
                # Check second node matches (for paths with length > 1)
                if len(candidate_path) > 1 and len(path) > 1:
                    if candidate_path[1] != path[1]:
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


###################################################
# Test cases
###################################################

def test_is_directed():
    """
    Two multicast sessions: 
        1 -> 2 -> 3 and 3 -> 2 -> 1
    
    Each edge has capacity 10
    """
    nodes = [1, 2, 3]
    edges = [(1, 2), (2, 1), (2, 3), (3, 2)]
    capacities = {edge: 10 for edge in edges}

    graph = Graph(
        nodes=nodes,
        edges=edges,
        capacities=capacities,
    )

    sources = [1, 3]
    destinations = {1: [3], 3: [1]}
    variables, sol = solve(graph, sources, destinations)

    assert sol[variables["x"]] == 10
    
    for variable, value in variables.items():
        print(f"{variable}: {sol[value]}")

    print(sol)

    return variables, sol, graph, sources, destinations


def test_single_commodity_lcm():
    """
    Single multicast session: 
        1 -> 2, 3, 4;
        2, 3, 4 -> 5;
        5 -> 6, 7, 8, 9, 10

    Each edge has capacity 10
    """

    nodes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    edges = [
        (1, 2), (1, 3), (1, 4),
        (2, 1), (3, 1), (4, 1),
        (2, 5), (3, 5), (4, 5),
        (5, 2), (5, 3), (5, 4),
        (5, 6), (5, 7), (5, 8), (5, 9), (5, 10),
        (6, 5), (7, 5), (8, 5), (9, 5), (10, 5),
    ]
    capacities = {edge: 10 for edge in edges}

    graph = Graph(
        nodes=nodes,
        edges=edges,
        capacities=capacities,
    )

    sources = [1]
    destinations = {1: [6, 7, 8, 9, 10]}
    variables, sol = solve(graph, sources, destinations)

    assert sol[variables["x"]] == 6
    
    for variable, value in variables.items():
        print(f"{variable}: {sol[value]}")

    print(sol)

    return variables, sol, graph, sources, destinations


if __name__ == "__main__":
    # define edges, nodes, and capacities
    # sources = [1, 2]
    # destinations = {1: [22, 23, 24, 25], 2: [12, 24]}

    # graph = build_fat_tree(6, c=10)
    
    # variables, sol = solve(graph, sources, destinations)
    
    # for variable, value in variables.items():
    #     print(f"{variable}: {sol[value]}")

    # print(sol)

    test_is_directed()
    test_single_commodity_lcm()

