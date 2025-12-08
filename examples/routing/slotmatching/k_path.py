import json
import itertools
import heapq


class Graph:
    def __init__(self, num_nodes):
        self.nodes = range(1, num_nodes + 1)
        self.edges = {(i, j): 1 for i in self.nodes for j in self.nodes if i != j}

    def neighbors(self, node):
        return [v for u, v in self.edges if u == node]

    def get_edge_weight(self, u, v):
        return self.edges.get((u, v), float("inf"))


def k_shortest_paths(graph, start, end, k):
    paths = [(0, [start])]
    shortest_paths = []

    while paths and len(shortest_paths) < k:
        cost, path = heapq.heappop(paths)
        last_node = path[-1]
        if last_node == end:
            shortest_paths.append(path)
            continue
        for neighbor in graph.neighbors(last_node):
            if neighbor not in path:
                new_path = path + [neighbor]
                new_cost = cost + graph.get_edge_weight(last_node, neighbor)
                heapq.heappush(paths, (new_cost, new_path))

    return shortest_paths


def generate_routes(num_nodes, k):
    graph = Graph(num_nodes)
    all_pairs = itertools.permutations(graph.nodes, 2)
    routes = []

    for src, dst in all_pairs:
        route_id = 0
        paths = k_shortest_paths(graph, src, dst, k)
        for path in paths:
            route = {
                "route_id": route_id,
                "src_node_id": src,
                "dst_node_id": dst,
                "hops": path,
            }
            routes.append(route)
            route_id += 1

    return routes


# Example usage
num_nodes = 6  # Number of nodes in the graph
k = 5  # Number of shortest paths to find
routes = generate_routes(num_nodes, k)

# Output to JSON
with open("routes.json", "w") as file:
    json.dump(routes, file, indent=4)

print("Generated routes saved to 'routes.json'")
