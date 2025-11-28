"""
Simplified waterfilling algorithm - static allocation based on configured link rates.

Instead of measuring traffic, uses the configured link rates to determine capacity.
This is more predictable for demonstration purposes.
"""

from collections import defaultdict
from time import sleep
import json

from common import Database


class SimpleWaterfillingAlgorithm:
    def __init__(self, db_creds: dict, max_routes_per_pair: int = 10):
        self.db = Database(db_creds)
        self.max_routes = max_routes_per_pair
        
        # Path capacities (end-to-end, considering bottlenecks)
        # Based on controller-config.toml link_rates:
        # - Path [1,2]: 100 Mbps (direct link)
        # - Path [1,3,2]: min(200, 50) = 50 Mbps (clear bottleneck!)
        # - Path [1,4,2]: min(200, 200) = 200 Mbps (high capacity!)
        # Ratio: 100:50:200 = 2:1:4
        # Expected allocation: ~3:1:6
        self.path_capacities_config = {
            (1, 2): 100_000_000,
            (1, 3, 2): 50_000_000,
            (1, 4, 2): 200_000_000,
            (2, 1): 100_000_000,
        }
        
    def get_path_capacity(self, path):
        """Get path capacity from preconfigured values."""
        path_tuple = tuple(path)
        return self.path_capacities_config.get(path_tuple, 100_000_000)  # Default 100 Mbps
    
    def allocate_by_capacity(self, paths):
        """Allocate route copies based on path capacities."""
        capacities = {}
        for path in paths:
            cap = self.get_path_capacity(path)
            capacities[tuple(path)] = cap
        
        total_capacity = sum(capacities.values())
        if total_capacity == 0:
            return {tuple(path): 1 for path in paths}
        
        allocations = {}
        for path_tuple, cap in capacities.items():
            ratio = cap / total_capacity
            slots = max(1, round(ratio * self.max_routes))
            allocations[path_tuple] = slots
        
        # Normalize to exactly max_routes
        total_allocated = sum(allocations.values())
        if total_allocated != self.max_routes:
            diff = self.max_routes - total_allocated
            # Add/subtract from largest allocation
            largest_path = max(allocations, key=allocations.get)
            allocations[largest_path] += diff
        
        return allocations
    
    def run_once(self):
        """Run algorithm once to set up optimal routes."""
        print("=" * 60)
        print("Simple Waterfilling - One-time Setup")
        print("=" * 60)
        
        # Get initial routes
        routes = self.db.get_all_routes()
        paths_by_pair = defaultdict(list)
        
        for src, dst, route_id, edges in routes:
            path = self.db.edges_to_path(edges)
            paths_by_pair[(src, dst)].append(path)
        
        print(f"\nFound {len(paths_by_pair)} src-dst pairs")
        
        for (src, dst), paths in paths_by_pair.items():
            if src == 1 and dst == 2:  # Focus on 1->2
                print(f"\nProcessing {src} -> {dst}:")
                print(f"  Available paths: {len(paths)}")
                
                # Show path capacities
                for path in paths:
                    cap = self.get_path_capacity(path)
                    print(f"    {path}: {cap/1e6:.0f} Mbps")
                
                # Calculate allocation
                allocations = self.allocate_by_capacity(paths)
                
                print(f"\n  Allocation (total {sum(allocations.values())} routes):")
                for path_tuple, count in sorted(allocations.items(), key=lambda x: x[1], reverse=True):
                    cap = self.get_path_capacity(list(path_tuple))
                    ratio = count / sum(allocations.values()) * 100
                    print(f"    {list(path_tuple)}: {count} copies ({ratio:.0f}%, {cap/1e6:.0f} Mbps)")
                
                # Install routes
                paths_to_install = []
                for path_tuple, count in allocations.items():
                    for _ in range(count):
                        paths_to_install.append(list(path_tuple))
                
                print(f"\n  Installing {len(paths_to_install)} routes...")
                self.db.update_routes(src, dst, paths_to_install)
                print(f"  ✓ Done")
        
        print("\n" + "=" * 60)
        print("Routes installed successfully!")
        print("=" * 60)
        print("\nVerify with:")
        print('  docker exec postgres psql -U pgusr -d nextmini -c "SELECT edges, COUNT(*) FROM routes WHERE src_node_id=1 AND dst_node_id=2 GROUP BY edges"')


if __name__ == "__main__":
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "nextmini",
    }
    
    alg = SimpleWaterfillingAlgorithm(creds, max_routes_per_pair=10)
    alg.run_once()

