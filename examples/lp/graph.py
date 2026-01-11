"""
Graph data structure for LP solver.
Adapted from examples/sqaure/experiments/optimization/graph.py
"""

import heapq
import random
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

    def _edge_inv_capacity_cost(self, src: int, dst: int, *, c_max: float) -> float:
        cap = float(self.capacities.get((src, dst), 0.0))
        if cap <= 0.0 or c_max <= 0.0:
            return float("inf")
        cap_norm = cap / c_max
        return 1.0 / (cap_norm + 1e-9)

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
        max_length: int = -1,
        allowed_intermediate_nodes: set[int] | None = None,
    ):
        """Find all paths from src to dst using DFS.

        Args:
            max_length: Maximum number of EDGES (hops) in path, or -1 for unlimited
            allowed_intermediate_nodes: If provided, only these nodes may appear as
                intermediate hops (the destination is always allowed).
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
            if (
                allowed_intermediate_nodes is not None
                and neighbor != dst
                and neighbor not in allowed_intermediate_nodes
            ):
                continue
            path.append(neighbor)
            self.find_paths(
                neighbor,
                dst,
                path,
                all_paths,
                max_length=max_length,
                allowed_intermediate_nodes=allowed_intermediate_nodes,
            )
            path.pop()

    def _k_shortest_paths(
        self,
        src: int,
        dst: int,
        *,
        max_length: int,
        allowed_intermediate_nodes: set[int] | None,
        num_paths: int,
        c_max: float,
    ) -> t.List[t.List[int]]:
        """Return up to `num_paths` simple paths ordered by the mFlow shortest key.

        The mFlow solver orders paths by:
          (hop_count, inv_capacity_cost_sum, tuple(path))

        where inv_capacity_cost_sum sums 1/(C(e)/C_max + 1e-9) over edges.

        This avoids enumerating all simple paths on dense graphs.
        """
        if num_paths <= 0:
            return []

        heap: list[tuple[int, float, tuple[int, ...]]] = [(0, 0.0, (src,))]
        out: list[list[int]] = []

        while heap and len(out) < num_paths:
            hops, inv_cost, path = heapq.heappop(heap)
            node = path[-1]

            if node == dst:
                out.append(list(path))
                continue

            if max_length > 0 and hops >= max_length:
                continue

            for neighbor, _cap in self.adj.get(node, []):
                if neighbor in path:
                    continue
                if (
                    allowed_intermediate_nodes is not None
                    and neighbor != dst
                    and neighbor not in allowed_intermediate_nodes
                ):
                    continue

                new_path = path + (neighbor,)
                new_hops = hops + 1
                new_cost = inv_cost + self._edge_inv_capacity_cost(
                    node, neighbor, c_max=c_max
                )
                heapq.heappush(heap, (new_hops, new_cost, new_path))

        return out

    def _k_random_paths(
        self,
        src: int,
        dst: int,
        *,
        max_length: int,
        allowed_intermediate_nodes: set[int] | None,
        num_paths: int,
    ) -> t.List[t.List[int]]:
        """Sample up to `num_paths` simple paths using randomized DFS.

        This is a lightweight alternative to enumerating all paths and shuffling.
        """
        if num_paths <= 0:
            return []

        out: list[list[int]] = []
        seen: set[tuple[int, ...]] = set()

        max_attempts = max(50, num_paths * 200)
        for _ in range(max_attempts):
            if len(out) >= num_paths:
                break

            stack = [(src, [src])]
            found: list[int] | None = None
            while stack:
                node, path = stack.pop()
                if node == dst:
                    found = path
                    break

                hops = len(path) - 1
                if max_length > 0 and hops >= max_length:
                    continue

                neighbors = [n for n, _ in self.adj.get(node, [])]
                random.shuffle(neighbors)
                for neighbor in neighbors:
                    if neighbor in path:
                        continue
                    if (
                        allowed_intermediate_nodes is not None
                        and neighbor != dst
                        and neighbor not in allowed_intermediate_nodes
                    ):
                        continue
                    stack.append((neighbor, path + [neighbor]))

            if not found:
                continue

            key = tuple(found)
            if key in seen:
                continue
            seen.add(key)
            out.append(found)

        return out

    def get_paths(
        self,
        sources: t.List[int],
        destinations: t.Dict[int, t.List[int]],
        max_length: int = -1,
        allowed_intermediate_nodes: set[int] | None = None,
        num_paths: int = -1,
        sort_by: str = "shortest",
    ) -> t.Dict[t.Tuple[int, int], t.List[t.List[int]]]:
        """Get candidate paths for given source-destination pairs.

        When `num_paths` is > 0, returns up to `num_paths` simple paths per (src,dst),
        using `sort_by` to choose/select paths without enumerating all simple paths.
        """
        all_source_paths = {}
        for src in sources:
            for dst in destinations[src]:
                if num_paths is not None and int(num_paths) > 0:
                    if sort_by == "shortest":
                        c_max = max(
                            (c for c in self.capacities.values() if c > 0.0),
                            default=0.0,
                        )
                        paths = self._k_shortest_paths(
                            src,
                            dst,
                            max_length=max_length,
                            allowed_intermediate_nodes=allowed_intermediate_nodes,
                            num_paths=int(num_paths),
                            c_max=c_max,
                        )
                    elif sort_by == "random":
                        paths = self._k_random_paths(
                            src,
                            dst,
                            max_length=max_length,
                            allowed_intermediate_nodes=allowed_intermediate_nodes,
                            num_paths=int(num_paths),
                        )
                    else:
                        raise ValueError(f"Invalid sort_by: {sort_by}")
                    all_source_paths[(src, dst)] = paths
                    continue

                all_paths: t.List[t.List[int]] = []
                self.find_paths(
                    src,
                    dst,
                    [src],
                    all_paths,
                    max_length=max_length,
                    allowed_intermediate_nodes=allowed_intermediate_nodes,
                )
                all_source_paths[(src, dst)] = all_paths
        return all_source_paths

    @classmethod
    def from_toml(cls, toml_dict: dict) -> "Graph":
        """Create Graph from TOML topology definition."""
        topo = toml_dict.get("topology", {})
        # Nodes can be explicitly provided, inferred from edges, or derived from n_nodes.
        nodes = topo.get("nodes")
        edge_list = topo.get("edges")

        if nodes is None:
            # Derive nodes from controller-style config if present (optional convenience).
            full_mesh_cfg = (
                topo.get("full_mesh_config") if isinstance(topo, dict) else None
            )
            if isinstance(full_mesh_cfg, dict) and "n_nodes" in full_mesh_cfg:
                n_nodes = int(full_mesh_cfg["n_nodes"])
                nodes = list(range(1, n_nodes + 1))
            elif "n_nodes" in topo:
                n_nodes = int(topo["n_nodes"])
                nodes = list(range(1, n_nodes + 1))
            elif edge_list:
                # Infer nodes from edges list.
                uniq = set()
                for e in edge_list:
                    uniq.add(int(e[0]))
                    uniq.add(int(e[1]))
                nodes = sorted(uniq)
            else:
                nodes = []
        else:
            nodes = [int(n) for n in nodes]

        # If edges are omitted, assume a full-mesh among the declared nodes.
        # This keeps configs concise for common "full mesh N nodes" examples.
        if edge_list is None:
            edge_list = [[a, b] for i, a in enumerate(nodes) for b in nodes[i + 1 :]]
        else:
            edge_list = list(edge_list)

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
