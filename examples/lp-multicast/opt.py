"""Solving an optimization problem for the INFOCOM 2025 paper."""

from cvxopt import matrix, solvers, spmatrix, sparse, printing
from collections import defaultdict
import typing as t

printing.options["dformat"] = "%.1f"
printing.options['width'] = -1

# function to build the graph
def build_graph(edges, capacity):
    graph = defaultdict(list)
    for edge in edges:
        # Add edge with capacity
        graph[edge[0]].append((edge[1], capacity[edge]))
    return graph

# recursive function to find all paths
def find_paths(graph, current_node, end, path, all_paths):
    if current_node == end:
        all_paths.append(path.copy())
        return
    for neighbor, _ in graph[current_node]:
        if neighbor not in path:
            path.append(neighbor)
            find_paths(graph, neighbor, end, path, all_paths)
            path.pop()

# general function to get paths
def get_paths(edges, capacities, sources, destinations):
    graph = build_graph(edges, capacities)
    all_source_paths = {}
    for src in sources:
        for dest in destinations[src]:
            all_paths = []
            find_paths(graph, src, dest, [src], all_paths)
            all_source_paths[(src, dest)] = all_paths
    return all_source_paths

c = matrix([-1.0] + [0.0] * 16)
print(c)
# solution = matrix([10, 10,6.1, 3.9, 6.1, 3.9, 3.9, 6.1, 10, 6.1, 3.9, 3.9, 10, 6.1, 6.1, 3.9, 3.9])
# row =      matrix([0,  0, 0,  0,    0,    0, 1,    0,    0, 0,    0,   0,  0,  -1,  0,    0, 0])
#       [10| 10,6.1,3.9,3.9,6.1,6.1,3.9|10,6.1，3.9，3.9，10，6.1，6.1，3.9，3.9]
# X = 10
# path_124 = 10
# path_125 = 6.1, path_1235 = 3.9
# path_23 = 6.1, path_253 = 3.9
# path_25 = 3.9, path_235 = 6.1
# x^1(e_12) = 10, x^1(e_25) = 6.1, x^1(e_23) = 3.9, x^1(e_35) = 3.9
# x^1(e_24) = 10, x^2(e_23) = 6.1, x^2(e_35) = 6.1, x^2(e_25) = 3.9, x^2(e_53) = 3.9

# Solution computed:
# X = 10
# path_124 = 10
# path_125 = 0, path_1235 = 10
# path_23 = 0, path_253 = 10 
# path_25 = 10, path_235 = 0 
# x^1(e_12) = 10, x^1(e_25) = 0, x^1(e_23) = 10, x^1(e_35) = 10
# x^1(e_24) = 10, x^2(e_23) = 0, x^2(e_35) = 0, x^2(e_25) = 10, x^2(e_53) = 10

G = matrix(
    [
        [1.0, -1,  0,  0,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 1, j = 4， path = {1, 2, 4}
        [1,    0, -1, -1,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 1, j = 5, path = {1, 2, 3, 5}, {1, 2, 5} 
        [1,    0,  0,  0, -1, -1,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 2, j = 3, path = {2, 3}, {2, 5, 3}
        [1,    0,  0,  0,  0,  0, -1, -1, 0,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 2, j = 5, path = {2, 3, 5}, {2, 5}
        [0,    1,  0,  0,  0,  0,  0,  0,-1,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 1, j = 4, x^1(e_12) covers paths {1, 2, 4}
        [0,    0,  1,  1,  0,  0,  0,  0,-1,  0, 0, 0, 0, 0, 0, 0, 0],  # i = 1, j = 5, x^1(e_12) covers paths {1, 2, 3, 5} and {1, 2, 5} 
        [0,    1,  0,  0,  0,  0,  0,  0, 0, -1, 0, 0, 0, 0, 0, 0, 0],  # i = 1, j = 5, x^1(e_24) covers paths {1, 2, 4}
        [0,    0,  1,  0,  0,  0,  0,  0, 0,  0,-1, 0, 0, 0, 0, 0, 0],  # i = 1, j = 5, x^1(e_23) covers paths {1, 2, 3, 5}
        [0,    0,  1,  0,  0,  0,  0,  0, 0,  0, 0,-1, 0, 0, 0, 0, 0],  # i = 1, j = 5, x^1(e_35) covers paths {1, 2, 3, 5}
        [0,    0,  0,  1,  0,  0,  0,  0, 0,  0, 0, 0,-1, 0, 0, 0, 0],  # i = 1, j = 4, x^1(e_25) covers paths {1, 2, 5}
        [0,    0,  0,  0,  1,  0,  0,  0, 0,  0, 0, 0, 0,-1, 0, 0, 0],  # i = 2, j = 3, x^2(e_23) covers paths {2, 3} index is 4,
        [0,    0,  0,  0,  0,  0,  1,  0, 0,  0, 0, 0, 0,-1, 0, 0, 0],  # i = 2, j = 5, x^2(e_23) covers paths {2, 3, 5} index is 6
        [0,    0,  0,  0,  0,  1,  0,  0, 0,  0, 0, 0, 0, 0,-1, 0, 0],  # i = 2, j = 3, x^2(e_25) covers paths {2, 5, 3}: 5 
        [0,    0,  0,  0,  0,  0,  0,  1, 0,  0, 0, 0, 0, 0,-1, 0, 0],  # i = 2, j = 5, x^2(e_25) covers paths {2, 5}: 7
        [0,    0,  0,  0,  0,  1,  0,  0, 0,  0, 0, 0, 0, 0, 0,-1, 0],  # i = 2, j = 3, x^2(e_53) covers paths {2, 5, 3}: 5
        [0,    0,  0,  0,  0,  0,  1,  0, 0,  0, 0, 0, 0, 0, 0, 0,-1],  # i = 2，j = 5, x^2(e_35) covers paths {2, 3, 5}: 6
        [0,    0,  0,  0,  0,  0,  0,  0, 1,  0, 0, 0, 0, 0, 0, 0, 0],  # x^1(e_12) <= C(e_12) 
        [0,    0,  0,  0,  0,  0,  0,  0, 0,  0, 1, 0, 0, 1, 0, 0, 0],  # x^1(e_23) + x^2(e_23) <= C(e_23) 
        [0,   0,  0,  0,  0,  0,  0,  0, 0,  1, 0, 0, 0, 0, 0, 0, 0],  # x^1(e_24) <= C(e_24) 
        [0,    0,  0,  0,  0,  0,  0,  0, 0,  0, 0, 0, 1, 0, 1, 0, 0],  # x^1(e_25) + x^2(e_25) <= C(e_25)
        [0,    0,  0,  0,  0,  0,  0,  0, 0,  0, 0, 1, 0, 0, 0, 0, 1],  # x^1(e_35) + x^2(e_35) <= C(e_35) 
        [0,    0,  0,  0,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 1, 0],  # x^2(e_53) <= C(e_53) 
        [-1,   0,  0,  0,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,   -1,  0,  0,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0, -1,  0,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, -1,  0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,  -1,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0, -1,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0, -1,  0, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0, -1, 0,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0,-1,  0, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0, -1, 0, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0,-1, 0, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0,-1, 0, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0, 0,-1, 0, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0, 0, 0,-1, 0, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0, 0, 0, 0,-1, 0, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0,-1, 0],
        [0,    0,  0, 0,   0,  0,  0,  0, 0,  0, 0, 0, 0, 0, 0, 0,-1],
    ],
).trans()


# define edges, nodes, and capacities
nodes = [1, 2, 3, 4, 5]
edges = [(1, 2), (2, 3), (2, 4), (2, 5), (3, 5), (5, 3)]
capacities = {
    (1, 2): 10,
    (2, 3): 10,
    (2, 4): 10,
    (2, 5): 10,
    (3, 5): 10,
    (5, 3): 10
}

# define source and destination nodes
sources = [1, 2]
destinations = {1: [4, 5], 2: [3, 5]}

# get paths
paths = get_paths(edges, capacities, sources, destinations)
for key, value in paths.items():
    print(f"Paths from {key[0]} to {key[1]}: {value}")

# building matrix c for the optimization objective
# num_paths + xi(e)
# find the edges for path with the same source and destination.
path_collections = {}
for src_dst, all_paths in paths.items():
    for path in all_paths:
        path_start = path[0]
        path_end = path[-1]
        path_edges = [(path[i], path[i+1]) for i in range(len(path)-1)]
        if path_start not in path_collections:
            path_collections[path_start] = {}
        for edge in path_edges:
            if edge not in path_collections[path_start]:
                path_collections[path_start][edge] = {}
            if path_end not in path_collections[path_start][edge]:
                path_collections[path_start][edge][path_end] = []
            path_collections[path_start][edge][path_end].append(path)
# print(path_collections)
# {1: {(1, 2): {4: [[1, 2, 4]], 5: [[1, 2, 3, 5], [1, 2, 5]]}, (2, 4): {4: [[1, 2, 4]]}, (2, 3): {5: [[1, 2, 3, 5]]}, (3, 5): {5: [[1, 2, 3, 5]]}, (2, 5): {5: [[1, 2, 5]]}}, 2: {(2, 3): {3: [[2, 3]], 5: [[2, 3, 5]]}, (2, 5): {3: [[2, 5, 3]], 5: [[2, 5]]}, (5, 3): {3: [[2, 5, 3]]}, (3, 5): {5: [[2, 3, 5]]}}}
            
session_num = sum([len(edges) for start, edges in path_collections.items() ]) #total_ = 9

c = matrix([-1.0] + [0.0]*(sum([len(paths[key]) for key in paths])+session_num))

# building matrix G for inequality constraints
print("Producing matrix G: first constraints...")
# initial constraint matrix constr_1
constr_1_num_row = (len(sources)*len(destinations))
constr_1_num_colume = 1 + sum([len(paths[key]) for key in paths])+session_num
constr_1 = matrix(
        0.0,
        (constr_1_num_row,
        constr_1_num_colume),
    )

constr_1[:, 0]=1.0

# find the colume_index that should be -1 in constr_1
path_indexes = {}
for key, value in paths.items():
    path_indexes.update({tuple(path): path_idx + len(path_indexes) + 1 for path_idx, path in enumerate(value)})
# {(1, 2, 4): 1, (1, 2, 3, 5): 2, (1, 2, 5): 3, (2, 3): 4, (2, 5, 3): 5, (2, 3, 5): 6, (2, 5): 7}

# find the row_index that should be -1 in constr_1
# match src_dst pair and paths
src_dst_indexes = {}
for src, dsts in destinations.items():
    src_dst_indexes.update({(src, dst): len(src_dst_indexes) + idx for idx, dst in enumerate(dsts)})
# (1, 4): 0, (1, 5): 1, (2, 3): 2, (2, 5): 3}

# change the corresponding elements to be -1
for src_dst, idx in src_dst_indexes.items():
    for path, path_idx in path_indexes.items():
        if src_dst[0] == path[0] and src_dst[1] == path[-1]:
           constr_1[idx, path_idx] = -1 

# [ 1.00e+00 -1.00e+00  0.00e+00  0.00e+00  0.00e+00  0.00e+00  0.00e+00 ... ]
# [ 1.00e+00  0.00e+00 -1.00e+00 -1.00e+00  0.00e+00  0.00e+00  0.00e+00 ... ]
# [ 1.00e+00  0.00e+00  0.00e+00  0.00e+00 -1.00e+00 -1.00e+00  0.00e+00 ... ]
# [ 1.00e+00  0.00e+00  0.00e+00  0.00e+00  0.00e+00  0.00e+00 -1.00e+00 ... ]

print("Producing matrix G: second constraints...")

constr_2_num_row = sum([len(edge_info[edge]) for _, edge_info in path_collections.items() for edge in edge_info])
constr_2_num_column = 1 + sum([len(paths[key]) for key in paths])+session_num
# initial constraint matrix constr_2
constr_2 = matrix(
        0.0,
        (constr_2_num_row,
        constr_2_num_column),
    )

# get the column index for edges and their corresponding sessions, e.g. X1_e12
start_edge_column_indexes = {}
idx = 0
for start, edges_info in path_collections.items():
    start_edge_column_indexes[start] = {}
    for edge in edges_info:
        start_edge_column_indexes[start][edge] = idx    
        idx += 1

# find the index that should be -1 and 1 in constr_2
constr_2_matched_assignment = []
constr_2_matched_assignment_minus_1 = []
row_idx = 0
edge_idx = 0 # column index
for start, edges_info in path_collections.items():   
    for edge, edge_paths in edges_info.items():
        # edge_paths: {4: [[1, 2, 4]], 5: [[1, 2, 3, 5], [1, 2, 5]]}
        # paths: [[1, 2, 4]]
        for path_end, src_dst_paths in edge_paths.items():
            constr_2_matched_assignment.extend([(row_idx, path_indexes[tuple(path)]) for path in src_dst_paths])
            constr_2_matched_assignment_minus_1.extend([(row_idx, edge_idx) for path in src_dst_paths])
            row_idx += 1
        edge_idx += 1

# print(constr_2_matched_assignment)
# [(0, 1), (1, 2), (1, 3), (2, 1), (3, 2), (4, 2), (5, 3), (6, 4), (7, 6), (8, 5), (9, 7), (10, 5), (11, 6)]
# print(constr_2_matched_assignment_minus_1)
# [(0, 0), (1, 0), (1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 5), (8, 6), (9, 6), (10, 7), (11, 8)]

# assign -1 and 1 by their row and colum index
constr_2_matched_assignment_minus_1 = set(constr_2_matched_assignment_minus_1)
for assign in constr_2_matched_assignment:
    constr_2[assign[0], assign[1]] = 1
for assign in constr_2_matched_assignment_minus_1:
    constr_2[assign[0], 1 + sum([len(paths[key]) for key in paths]) + assign[1]] = -1


print("Producing matrix G: third constraints...")
# initial constraint matrix constr_1
constr_3_num_row = len(edges)
constr_3_num_colume = 1 + sum([len(paths[key]) for key in paths])+session_num
constr_3 = matrix(
        0.0,
        (constr_3_num_row,
        constr_3_num_colume),
    )

# find the index that should be 1 in constr_3
assign_positions = []
for row_idx, edge in enumerate(edges):
    assign_positions.extend([(row_idx, idx) for pos, cur_edges in start_edge_column_indexes.items() for search_edge, idx in cur_edges.items() if edge == search_edge])
# assign 1 by their row and colum index
for assign in assign_positions:
    constr_3[assign[0], 1 + sum([len(paths[key]) for key in paths]) + assign[1]] = 1

print("Producing matrix G: x >= 0 constraints...")
# initial constraint matrix constr_4
constr_4_num_row = 1 + sum([len(paths[key]) for key in paths])+session_num
constr_4_num_colume = 1 + sum([len(paths[key]) for key in paths])+session_num
constr_4= spmatrix(
        -1.0,
        range(constr_4_num_row),
        range(constr_4_num_colume),
    )

# print(constr_4.size) # (17, 17)

G = matrix([[constr_1, constr_2, constr_3, constr_4]])
print(G.size) # (39, 17)
print(G)

# h = matrix([0.0] * 4 + [0.0] * 12 + [10.0] * 6 + [0.0] * 17)
h = matrix([0.0] * (constr_1_num_row) + [0.0] * constr_2_num_row + list(capacities.values()) + [0.0] * constr_4_num_row) # (39, 1)

sol = solvers.lp(c, G, h, solver="mosek")
print(sol["x"])

flow_rate = path_indexes.copy()
print(flow_rate)

for key in flow_rate: 
    flow_rate[key] = "{:.2f}".format(abs(list(sol["x"])[path_indexes[key]]))
print("The flow rate for paths are ...")
print(flow_rate)
# {(1, 2, 4): '10.00', (1, 2, 3, 5): '10.00', (1, 2, 5): '0.00', (2, 3): '0.00', (2, 5, 3): '10.00', (2, 3, 5): '0.00', (2, 5): '10.00'}

session_rate =  start_edge_column_indexes

for start in session_rate.keys():
    for edge in session_rate[start]:
        # print(int(start_edge_column_indexes[start][edge] + 1 + len(flow_rate)))
        session_rate[start][edge] = "{:.2f}".format(abs(list(sol["x"])[int(start_edge_column_indexes[start][edge] + 1 + len(flow_rate))]))
print("The session rate for all sessions are ...")
print(session_rate)
# {1: {(1, 2): '10.00', (2, 4): '10.00', (2, 3): '10.00', (3, 5): '10.00', (2, 5): '0.00'}, 2: {(2, 3): '0.00', (2, 5): '10.00', (5, 3): '10.00', (3, 5): '0.00'}}


def convert_to_multicast_trees(
    variables: t.Dict[str, int],
    sol: t.List[float],
) -> t.Tuple[t.List[int], t.List[t.List[t.Tuple[t.List[t.List[int]], float]]]]:
    """
    Convert LP solution to multicast trees.
    
    This implementation uses a robust "single parent" consistency check
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

# Construct variables dict for conversion
variables = {}
solution_vals = list(sol["x"])

# path_indexes maps tuple path -> index in sol['x']
# format in mFlow is "p_{n1}_{n2}_..."
for path_tuple, idx in path_indexes.items():
    name = f"p_{'_'.join(map(str, path_tuple))}"
    variables[name] = idx

print("\n--- Multicast Tree Conversion Result ---")
sources, session_trees = convert_to_multicast_trees(variables, solution_vals)
print(f"Sources: {sources}")
for i, src in enumerate(sources):
    print(f"Source {src} Trees:")
    for tree, throughput in session_trees[i]:
        print(f"  Throughput: {throughput}")
        print(f"  Tree Paths: {tree}")
