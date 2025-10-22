"""
ECMP (Equal Cost Multi-Path) algorithm for new nextmini architecture.

Core idea:
1. Maintain N optimal paths for each node pair
2. Jump Hash automatically distributes flows evenly among these paths
3. Periodically adjust available path set based on link status

This version does not need to modify dataplane, leveraging Jump Hash for load balancing.
"""

from collections import defaultdict
from time import sleep
import heapq

from common import Database


class ECMPAlgorithm:
    def __init__(self, db_creds: dict, num_paths: int):
        """
        Initialize ECMP algorithm.
        
        Args:
            db_creds: Database connection configuration
            num_paths: Number of paths to maintain per node pair
        """
        self.db = Database(db_creds)
        self.num_paths = num_paths
        self.topology = None
        self.all_paths_cache = {}
        
    def discover_topology(self):
        """Discover network topology from database."""
        print("Discovering network topology...")
        nodes = self.db.get_all_nodes()
        
        routes = self.db.get_all_routes()
        links = set()
        
        for src, dst, route_id, edges in routes:
            for edge in edges:
                links.add((edge[0], edge[1]))
        
        self.topology = {
            'nodes': set(nodes),
            'links': links
        }
        print(f"Topology: {len(nodes)} nodes, {len(links)} links")
        
    def find_k_shortest_paths(self, src, dst, k):
        """
        Find k shortest paths from src to dst (simplified Yen's algorithm).
        In production, could use more complex algorithms considering bandwidth, latency, etc.
        
        Args:
            src: Source node ID
            dst: Destination node ID
            k: Number of paths to find
            
        Returns:
            List of paths, each path is a list of node IDs
        """
        cache_key = (src, dst, k)
        if cache_key in self.all_paths_cache:
            return self.all_paths_cache[cache_key]
        
        graph = defaultdict(list)
        for u, v in self.topology['links']:
            graph[u].append(v)
        
        all_paths = []
        queue = [(src, [src])]
        
        while queue and len(all_paths) < k * 10:
            node, path = queue.pop(0)
            
            if node == dst:
                all_paths.append(path)
                continue
            
            if len(path) > 10:
                continue
            
            for neighbor in graph.get(node, []):
                if neighbor not in path:
                    queue.append((neighbor, path + [neighbor]))
        
        all_paths.sort(key=len)
        result = all_paths[:k]
        
        self.all_paths_cache[cache_key] = result
        return result
    
    def evaluate_path_quality(self, path):
        """
        Evaluate path quality (considering bandwidth, latency, packet loss, etc.).
        Current simplified version: only considers maximum link utilization on the path.
        
        Args:
            path: List of node IDs representing the path
            
        Returns:
            Quality score (lower is better)
        """
        link_util = self.db.get_link_utilization()
        
        max_utilization = 0
        for i in range(len(path) - 1):
            util = link_util.get((path[i], path[i+1]), 0)
            max_utilization = max(max_utilization, util)
        
        return max_utilization
    
    def select_best_paths(self, src, dst):
        """
        Select num_paths best paths for src-dst pair.
        
        Args:
            src: Source node ID
            dst: Destination node ID
            
        Returns:
            List of selected paths
        """
        candidate_paths = self.find_k_shortest_paths(src, dst, self.num_paths * 2)
        
        if not candidate_paths:
            print(f"Warning: No path found from {src} to {dst}")
            return []
        
        path_scores = []
        for path in candidate_paths:
            score = self.evaluate_path_quality(path)
            path_scores.append((score, path))
        
        path_scores.sort(key=lambda x: x[0])
        selected = [path for score, path in path_scores[:self.num_paths]]
        
        return selected
    
    def run(self, update_interval: int):
        """
        Main loop: periodically update routing table.
        
        Args:
            update_interval: Update interval in seconds
        """
        self.discover_topology()
        
        round_num = 0
        
        while True:
            print(f"\n=== Round {round_num} ===")
            
            nodes = list(self.topology['nodes'])
            node_pairs = [(src, dst) for src in nodes for dst in nodes if src != dst]
            
            total_updates = 0
            
            for src, dst in node_pairs:
                best_paths = self.select_best_paths(src, dst)
                
                if not best_paths:
                    continue
                
                current_routes = [
                    (route_id, self.db.edges_to_path(edges))
                    for s, d, route_id, edges in self.db.get_all_routes()
                    if s == src and d == dst
                ]
                
                current_paths = set(tuple(path) for _, path in current_routes)
                new_paths = set(tuple(path) for path in best_paths)
                
                if current_paths != new_paths:
                    print(f"Updating routes for {src} -> {dst}")
                    print(f"  Old paths: {len(current_routes)}")
                    print(f"  New paths: {len(best_paths)}")
                    
                    self.db.update_routes(src, dst, best_paths)
                    total_updates += 1
            
            print(f"\nStatistics:")
            print(f"  Route table updates: {total_updates}")
            
            route_util = self.db.get_route_utilization()
            if route_util:
                print(f"  Active routes: {len(route_util)}")
                total_bps = sum(route_util.values())
                print(f"  Total traffic: {total_bps / 1e9:.2f} Gbps")
            
            flow_dist = self.db.get_flow_distribution()
            if flow_dist:
                print(f"  Flow pairs: {len(flow_dist)}")
                for (src, dst), routes in flow_dist.items():
                    print(f"    {src}->{dst}: {sum(routes.values())} flows across {len(routes)} routes")
            
            round_num += 1
            sleep(update_interval)


if __name__ == "__main__":
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "nextmini",
    }
    
    alg = ECMPAlgorithm(creds, num_paths=3)
    
    alg.run(update_interval=30)
