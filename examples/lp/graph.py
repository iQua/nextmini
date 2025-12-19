"""
Graph data structure for LP solver.
Adapted from examples/sqaure/experiments/optimization/graph.py
"""
import typing as t
from dataclasses import dataclass
from itertools import product


@dataclass
class MulticastTree:
    """Represents a multicast tree with source, destinations, and paths."""
    src: int
    dst: t.List[int]
    paths: t.Dict[t.Tuple[int, int], t.List[int]]


class Graph:
    """Graph representation for LP solver."""
    
    def __init__(
        self, 
        nodes: t.List[int], 
        edges: t.List[t.Tuple[int, int]],
        capacities: t.Dict[t.Tuple[int, int], float],
    ):
        self.nodes = nodes
        self.edges = edges
        self.capacities = capacities
        self.adj = self._build_adjacency()
    
    def _build_adjacency(self) -> t.Dict[int, t.List[t.Tuple[int, float]]]:
        """Build adjacency list from edges."""
        adj = {node: [] for node in self.nodes}
        for src, dst in self.edges:
            adj[src].append((dst, self.capacities[(src, dst)]))
        return adj

    def find_paths(
        self, 
        src: int, 
        dst: int, 
        path: t.List[int], 
        all_paths: t.List[t.List[int]], 
        max_length: int = -1
    ):
        """Find all paths from src to dst using DFS.
        
        Args:
            max_length: Maximum number of EDGES (hops) in path, or -1 for unlimited
        """
        if src == dst:
            all_paths.append(path.copy())
            return
        
        # max_length counts edges, so check len(path) - 1 (since path includes src)
        if max_length > 0 and len(path) - 1 >= max_length:
            return
        
        for neighbor, _ in self.adj[src]:
            if neighbor in path:
                continue
            path.append(neighbor)
            self.find_paths(neighbor, dst, path, all_paths, max_length=max_length)
            path.pop()

    def find_shortest_path_from_src(self, src: int) -> t.Dict[int, int]:
        """BFS to find shortest path tree from src."""
        visited = {src}
        parent = {src: None}
        queue = [src]
        while queue:
            node = queue.pop(0)
            for neighbor, _ in self.adj[node]:
                if neighbor in visited:
                    continue
                parent[neighbor] = node
                visited.add(neighbor)
                queue.append(neighbor)
        return parent

    def get_paths(
        self, 
        sources: t.List[int], 
        destinations: t.Dict[int, t.List[int]],
        max_length: int = -1,
    ) -> t.Dict[t.Tuple[int, int], t.List[t.List[int]]]:
        """Get all paths for given source-destination pairs."""
        all_source_paths = {}
        for src in sources:
            for dst in destinations[src]:
                all_paths = []
                self.find_paths(src, dst, [src], all_paths, max_length=max_length)
                all_source_paths[(src, dst)] = all_paths
        return all_source_paths

    @classmethod
    def from_toml(cls, toml_dict: dict) -> "Graph":
        """Create Graph from TOML topology definition."""
        topo = toml_dict.get("topology", {})
        nodes = topo.get("nodes", [])
        edge_list = topo.get("edges", [])
        default_capacity = topo.get("default_capacity", 1000)
        
        # Convert to bidirectional edges
        edges = []
        capacities = {}
        for e in edge_list:
            src, dst = e[0], e[1]
            edges.append((src, dst))
            edges.append((dst, src))
            capacities[(src, dst)] = default_capacity
            capacities[(dst, src)] = default_capacity
        
        # Apply per-link overrides if present
        for link in toml_dict.get("link", []):
            src, dst = link["src"], link["dst"]
            cap = link.get("capacity", default_capacity)
            capacities[(src, dst)] = cap
            capacities[(dst, src)] = cap
        
        return cls(nodes, edges, capacities)
