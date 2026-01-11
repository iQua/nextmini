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
    allowed_intermediate_nodes: set[int] | None = None,
    node_egress_budgets: dict[int, float | None] | None = None,
    canonicalize: bool = True,
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
        node_egress_budgets: Optional per-node upload budgets U_u. When provided,
            the LP enforces sum_{(u,v)} e_{src,u,v} <= U_u for each session source.
        canonicalize: If True, run a second LP to minimize total edge usage while
            preserving the optimal common rate (makes the solution more stable for
            computing edge importance).

    Returns:
        Tuple of (variable_name_to_index, solution_values)
    """
    if not CVXOPT_AVAILABLE:
        raise ImportError("cvxopt is required for LP solving")

    # Get candidate paths (bounded by `num_paths` to avoid enumerating all simple paths).
    paths = graph.get_paths(
        sources,
        destinations,
        max_length=max_length,
        allowed_intermediate_nodes=allowed_intermediate_nodes,
        num_paths=num_paths,
        sort_by=sort_by,
    )

    # Fail fast if any (src,dst) lacks a feasible candidate path under the current
    # hop limit and relay-eligibility constraints.
    missing_pairs: list[tuple[int, int]] = [
        (src, dst)
        for (src, dst), all_paths in paths.items()
        if src in destinations and dst in destinations[src] and not all_paths
    ]
    if missing_pairs:
        raise ValueError(f"No feasible candidate paths for pairs: {missing_pairs}")

    # Filter paths
    if sort_by == "shortest":
        c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)

        def _path_key(path: list[int]) -> tuple[int, float, tuple[int, ...]]:
            # Primary: hop count (edges). Tie-break: capacity cost sum 1/tildeC(e).
            hops = max(0, len(path) - 1)
            if c_max <= 0.0:
                return (hops, 0.0, tuple(path))
            inv_cap_cost = 0.0
            for i in range(len(path) - 1):
                edge = (path[i], path[i + 1])
                cap = float(graph.capacities.get(edge, 0.0))
                if cap <= 0.0:
                    inv_cap_cost = float("inf")
                    break
                cap_norm = cap / c_max
                inv_cap_cost += 1.0 / (cap_norm + 1e-9)
            return (hops, inv_cap_cost, tuple(path))

        for flow in paths:
            paths[flow].sort(key=_path_key)
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
            if name in variables:
                continue
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
        for n1, n2 in edges:
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

    # For all (src, dst) pairs, total throughput <= x (prevents slack / degenerate optima)
    for _, all_paths in paths.items():
        names = [f"p_{'_'.join(map(str, path))}" for path in all_paths]
        columns = [variables[name] for name in names]

        row = np.zeros(variable_counter, dtype=np.float64)
        row[variables["x"]] = -1
        row[columns] = 1
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
        names = [
            f"e_{src}_{n1}_{n2}" for src in sources if f"e_{src}_{n1}_{n2}" in variables
        ]
        if len(names) == 0:
            continue
        columns = [variables[name] for name in names]
        row = np.zeros(variable_counter, dtype=np.float64)
        row[columns] = 1
        G_rows.append(row)
        h_rows.append(capacity)

    # Optional: per-node egress budgets (upload caps).
    # Enforce sum_{(u,v)} e_{src,u,v} <= U_u for each session source.
    if node_egress_budgets:
        for u, budget in node_egress_budgets.items():
            if budget is None:
                continue
            cap = float(budget)
            if cap < 0.0:
                continue
            out_neighbors = [v for v, _ in graph.adj.get(int(u), [])]
            if not out_neighbors:
                continue
            for src in sources:
                cols: list[int] = []
                for v in out_neighbors:
                    name = f"e_{src}_{int(u)}_{v}"
                    idx = variables.get(name)
                    if idx is not None:
                        cols.append(idx)
                if not cols:
                    continue
                row = np.zeros(variable_counter, dtype=np.float64)
                row[cols] = 1.0
                G_rows.append(row)
                h_rows.append(cap)

    G = np.array(G_rows, dtype=np.float64)
    h = np.array(h_rows, dtype=np.float64)

    base_G = G
    base_h = h

    # All variables >= 0
    nonneg_G = -np.eye(variable_counter)
    nonneg_h = np.zeros(variable_counter)

    G = np.vstack([base_G, nonneg_G])
    h = np.hstack([base_h, nonneg_h])

    # Objective: maximize x
    c = np.zeros(variable_counter, dtype=np.float64)
    c[variables["x"]] = -1

    c = matrix(c)
    G = matrix(G)
    h = matrix(h)

    def _solve_lp(
        c_mat: "matrix", G_mat: "matrix", h_mat: "matrix", *, solver_name: str
    ) -> list[float]:
        res = solvers.lp(c_mat, G_mat, h_mat, solver=solver_name)
        if res.get("status") != "optimal":
            raise RuntimeError(f"LP solve failed: status={res.get('status')}")
        return list(res["x"])

    def _solve_with_fallback(c_mat: "matrix", G_mat: "matrix", h_mat: "matrix") -> list[float]:
        solvers.options["show_progress"] = False
        if MOSEK_AVAILABLE:
            try:
                # Silence MOSEK console output (it is extremely verbose by default).
                # cvxopt forwards `solvers.options['mosek']` into its Mosek wrapper.
                solvers.options["mosek"] = {mosek.iparam.log: 0}
                return _solve_lp(c_mat, G_mat, h_mat, solver_name="mosek")
            except Exception as e:
                # MOSEK license error or other runtime failure, fall back to GLPK
                print(f"Warning: MOSEK failed ({e}), falling back to GLPK")
        return _solve_lp(c_mat, G_mat, h_mat, solver_name="glpk")

    # Stage 1: maximize x
    sol = _solve_with_fallback(c, G, h)

    if not canonicalize:
        return variables, sol

    # Stage 2: fix x to the optimal value and minimize total edge usage.
    x_idx = variables.get("x")
    if x_idx is None:
        return variables, sol

    x_star = float(sol[x_idx])
    if x_star <= 0.0:
        return variables, sol

    # Pin x to x_star (within tolerance) to prevent the canonicalization LP from
    # drifting x downward (or collapsing it when x_star is tiny).
    tol = 1e-7 * max(1.0, abs(x_star))
    row_lo = np.zeros(variable_counter, dtype=np.float64)
    row_lo[x_idx] = -1  # x >= x_star - tol  ->  -x <= -(x_star - tol)
    row_hi = np.zeros(variable_counter, dtype=np.float64)
    row_hi[x_idx] = 1  # x <= x_star + tol
    G2 = np.vstack([base_G, row_lo, row_hi, nonneg_G])
    h2 = np.hstack([base_h, -(x_star - tol), x_star + tol, nonneg_h])

    # Objective: minimize sum of effective edge rates (e_* variables).
    c2 = np.zeros(variable_counter, dtype=np.float64)
    for name, idx in variables.items():
        if name.startswith("e_"):
            c2[idx] = 1.0

    try:
        sol2 = _solve_with_fallback(matrix(c2), matrix(G2), matrix(h2))
    except Exception as e:
        print(f"Warning: canonicalization LP failed ({e}); using max-x solution")
        return variables, sol

    # Stage 3 (optional): keep x pinned and keep the minimal total edge usage,
    # then break ties deterministically at the path level.
    #
    # This is mainly used for stable path-derived statistics (e.g., relay scoring
    # based on which paths carry conceptual flow).
    edge_sum_star = 0.0
    row_edge_sum = np.zeros(variable_counter, dtype=np.float64)
    for name, idx in variables.items():
        if name.startswith("e_"):
            edge_sum_star += float(sol2[idx])
            row_edge_sum[idx] = 1.0

    tol_edge = 1e-6 * max(1.0, abs(edge_sum_star))
    edge_sum_lo = max(0.0, edge_sum_star - tol_edge)
    row_edge_sum_lo = -row_edge_sum  # sum_e >= edge_sum_lo  ->  -sum_e <= -edge_sum_lo
    row_edge_sum_hi = row_edge_sum   # sum_e <= edge_sum_star + tol_edge
    G3 = np.vstack([base_G, row_lo, row_hi, row_edge_sum_lo, row_edge_sum_hi, nonneg_G])
    h3 = np.hstack([
        base_h,
        -(x_star - tol),
        x_star + tol,
        -edge_sum_lo,
        edge_sum_star + tol_edge,
        nonneg_h,
    ])

    # Objective: minimize a deterministic weighted sum of path flows.
    # For each (src,dst) pair, paths are already ordered (hops, capacity cost);
    # we assign increasing weights by rank to prefer earlier candidates.
    c3 = np.zeros(variable_counter, dtype=np.float64)
    for (src, dst) in sorted(paths):
        for rank, path in enumerate(paths[(src, dst)], start=1):
            name = f"p_{'_'.join(map(str, path))}"
            idx = variables.get(name)
            if idx is not None:
                c3[idx] = float(rank)

    try:
        sol3 = _solve_with_fallback(matrix(c3), matrix(G3), matrix(h3))
    except Exception as e:
        print(f"Warning: path-tiebreak LP failed ({e}); using edge-canonical solution")
        return variables, sol2

    return variables, sol3


## NOTE: convert_to_multicast_trees() + paths_to_edges() are imported from `tree_conversion.py`.


def solve_mwu(
    graph: Graph,
    sources: t.List[int],
    destinations: t.Dict[int, t.List[int]],
    *,
    max_length: int = -1,
    sort_by: str = "shortest",
    num_paths: int = 2,
    allowed_intermediate_nodes: set[int] | None = None,
    node_egress_budgets: dict[int, float | None] | None = None,
    epsilon: float = 0.1,
    delta: float | None = None,
    max_iters: int = 100_000,
) -> t.Tuple[t.Dict[str, int], t.List[float]]:
    """Approximate mFlow using a combinatorial MWU-style algorithm (no LP solver).

    This implements a multiplicative-weights update loop inspired by Square's
    conceptual-flow approximation algorithm. It returns a path-flow solution that
    is subsequently scaled to satisfy edge capacities under the network-coding
    aggregation semantics (x(e) = max_dst f_dst(e)).

    Notes:
    - This implementation currently supports a single multicast session (one source).
    - The returned `(variables, sol)` is compatible with `cf_tree.extract_lp_solution`:
      it contains `x` (the common rate estimate) and path variables `p_<path>`.
    """
    if len(sources) != 1:
        raise ValueError("solve_mwu currently supports a single source session")
    src = sources[0]
    if src not in destinations:
        raise ValueError(f"Missing destination list for src={src}")

    if node_egress_budgets and any(budget is not None for budget in node_egress_budgets.values()):
        raise ValueError("solve_mwu does not support node egress budgets (U_u)")

    # Candidate paths per (src,dst) (bounded by `num_paths` to avoid enumerating all paths).
    paths = graph.get_paths(
        sources,
        destinations,
        max_length=max_length,
        allowed_intermediate_nodes=allowed_intermediate_nodes,
        num_paths=num_paths,
        sort_by=sort_by,
    )

    if sort_by == "shortest":
        c_max = max((c for c in graph.capacities.values() if c > 0.0), default=0.0)

        def _path_key(path: list[int]) -> tuple[int, float, tuple[int, ...]]:
            hops = max(0, len(path) - 1)
            if c_max <= 0.0:
                return (hops, 0.0, tuple(path))
            inv_cap_cost = 0.0
            for i in range(len(path) - 1):
                edge = (path[i], path[i + 1])
                cap = float(graph.capacities.get(edge, 0.0))
                if cap <= 0.0:
                    inv_cap_cost = float("inf")
                    break
                cap_norm = cap / c_max
                inv_cap_cost += 1.0 / (cap_norm + 1e-9)
            return (hops, inv_cap_cost, tuple(path))

        for flow in paths:
            paths[flow].sort(key=_path_key)
            paths[flow] = paths[flow][:num_paths]
    elif sort_by == "random":
        for flow in paths:
            np.random.shuffle(paths[flow])
            paths[flow] = paths[flow][:num_paths]
    else:
        raise ValueError(f"Invalid sort_by: {sort_by}")

    # Per-destination path sets.
    terminals: list[int] = []
    path_sets: dict[int, list[tuple[int, ...]]] = {}
    for dst in destinations[src]:
        if dst == src:
            continue
        cand = paths.get((src, dst), [])
        if not cand:
            # Infeasible: no hop-limited path to this terminal under eligibility.
            return {"x": 0}, [0.0]
        terminals.append(dst)
        path_sets[dst] = [tuple(p) for p in cand]

    if not terminals:
        return {"x": 0}, [0.0]

    # Precompute edge lists per path and the edge universe we track lengths for.
    edges_covered: set[tuple[int, int]] = set()
    path_edges: dict[tuple[int, ...], list[tuple[int, int]]] = {}
    for dst in sorted(terminals):
        for path in path_sets[dst]:
            edges = [(path[i], path[i + 1]) for i in range(len(path) - 1)]
            path_edges[path] = edges
            edges_covered.update(edges)

    if not edges_covered:
        return {"x": 0}, [0.0]

    if delta is None:
        delta = 1.0 / max(1, len(edges_covered))
    if epsilon <= 0.0:
        raise ValueError("epsilon must be positive")

    gamma: dict[tuple[int, int], float] = {e: float(delta) for e in edges_covered}
    k = len(terminals)

    # Raw (unscaled) path flows.
    path_flow: dict[tuple[int, ...], float] = {}

    def _best_path(dst: int) -> tuple[tuple[int, ...], float] | None:
        best_len = float("inf")
        best: tuple[int, ...] | None = None
        for path in path_sets[dst]:
            length = 0.0
            for e in path_edges[path]:
                length += gamma.get(e, 0.0)
            if length < best_len:
                best_len = length
                best = path
        if best is None:
            return None
        return best, best_len

    iters = 0
    while True:
        alpha_sum = 0.0
        min_alpha = float("inf")
        min_dst: int | None = None
        best_cache: dict[int, tuple[tuple[int, ...], float]] = {}

        for dst in terminals:
            best = _best_path(dst)
            if best is None:
                return {"x": 0}, [0.0]
            best_cache[dst] = best
            _, best_len = best
            denom = float(k * max(1, len(path_sets[dst])))
            alpha = best_len / denom
            alpha_sum += alpha
            if alpha < min_alpha:
                min_alpha = alpha
                min_dst = dst

        if alpha_sum >= 1.0 or min_dst is None:
            break

        path, _ = best_cache[min_dst]
        bottleneck = min(float(graph.capacities.get(e, 0.0)) for e in path_edges[path])
        if bottleneck <= 0.0:
            return {"x": 0}, [0.0]

        path_flow[path] = path_flow.get(path, 0.0) + bottleneck
        for e in path_edges[path]:
            cap = float(graph.capacities.get(e, 0.0))
            if cap <= 0.0:
                continue
            gamma[e] *= 1.0 + epsilon * bottleneck / cap

        iters += 1
        if iters >= max_iters:
            break

    # Scale the raw flows to satisfy capacity constraints under max aggregation:
    # x(e) = max_dst sum_{p in P_dst(e)} x_dst(p) <= C(e).
    per_edge_per_dst: dict[tuple[int, tuple[int, int]], float] = {}
    totals: dict[int, float] = {dst: 0.0 for dst in terminals}
    for path, flow in path_flow.items():
        if flow <= 0.0:
            continue
        dst = path[-1]
        totals[dst] = totals.get(dst, 0.0) + flow
        for e in path_edges[path]:
            key = (dst, e)
            per_edge_per_dst[key] = per_edge_per_dst.get(key, 0.0) + flow

    edge_usage: dict[tuple[int, int], float] = {}
    for (dst, edge), flow in per_edge_per_dst.items():
        edge_usage[edge] = max(edge_usage.get(edge, 0.0), flow)

    scale = 1.0
    for edge, usage in edge_usage.items():
        if usage <= 0.0:
            continue
        cap = float(graph.capacities.get(edge, 0.0))
        if cap <= 0.0:
            continue
        scale = min(scale, cap / usage)

    if not (scale > 0.0):
        return {"x": 0}, [0.0]

    for path in list(path_flow):
        path_flow[path] *= scale
    for dst in list(totals):
        totals[dst] *= scale

    f_star = min(totals.get(dst, 0.0) for dst in terminals) if terminals else 0.0
    if f_star <= 0.0:
        return {"x": 0}, [0.0]

    # Tighten: enforce exact per-destination totals to avoid slack polluting importance.
    tol = 1e-7
    for dst in terminals:
        total = totals.get(dst, 0.0)
        if total <= f_star * (1.0 + tol) or total <= 0.0:
            continue
        s = f_star / total
        for path in path_sets[dst]:
            if path in path_flow:
                path_flow[path] *= s
        totals[dst] = f_star

    variables: dict[str, int] = {"x": 0}
    sol: list[float] = [float(f_star)]

    for path in sorted(path_flow):
        flow = float(path_flow[path])
        if flow <= 1e-12:
            continue
        name = f"p_{'_'.join(map(str, path))}"
        variables[name] = len(sol)
        sol.append(flow)

    return variables, sol
