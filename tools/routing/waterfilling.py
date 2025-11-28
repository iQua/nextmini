"""
Waterfilling algorithm implementation for new nextmini architecture.

Core idea:
1. Measure actual capacity of each path
2. Allocate flows proportionally based on capacity (like water flowing to lower levels)
3. Dynamically adjust available route set to influence Jump Hash selection

This version uses "route weight simulation":
- To make a path carry more traffic, install multiple copies of it
- Jump Hash uniformly selects from all routes, achieving weighted load balancing

Example: Path A has 2x capacity of Path B, so install 2 copies of A and 1 copy of B
"""

from collections import defaultdict
from time import sleep
import math

from common import Database

EPS_BPS = 1e5          # 0.1 Mbps lower bound to avoid treating a quiet window as zero capacity
EXPECTED_RATIO = 0.95  # If actual < expected * 0.95, path hit bottleneck (stricter detection)
UP_MARGIN = 1.2        # If actual > capacity * 1.2, increase capacity estimate
DEADBAND = 0.15        # Required change in share (absolute) to trigger reallocation
INITIAL_CAP_MULTIPLIER = 3.0  # Initially assume capacity is 3x observed traffic for exploration
EXPLORE_BOOST = 1.5    # Periodically boost high-performer allocations by this factor to explore capacity


class WaterfillingAlgorithm:
    def __init__(self, db_creds: dict, max_routes_per_pair: int = 10):
        """
        Initialize waterfilling algorithm.
        
        Args:
            db_creds: Database connection configuration
            max_routes_per_pair: Maximum number of routes to install per node pair (for weight simulation)
        """
        self.db = Database(db_creds)
        self.max_routes = max_routes_per_pair
        self.topology = None
        self.path_capacities = defaultdict(lambda: float('inf'))
        self.path_max_observed = defaultdict(float)  # Track maximum observed traffic per path
        self.path_expected_bps = defaultdict(float)
        self.path_bottleneck_found = defaultdict(bool)  # Track if we've found bottleneck for this path
        self.round_num = 0
        
    def discover_topology(self):
        """Discover network topology from database."""
        print("Discovering network topology...")
        nodes = self.db.get_all_nodes()
        
        routes = self.db.get_all_routes()
        links = set()
        paths = defaultdict(list)
        
        for src, dst, route_id, edges in routes:
            path = self.db.edges_to_path(edges)
            paths[(src, dst)].append(path)
            for edge in edges:
                links.add((edge[0], edge[1]))
        
        self.topology = {
            'nodes': set(nodes),
            'links': links,
            'paths': dict(paths)
        }
        print(f"Topology: {len(nodes)} nodes, {len(links)} links")
        
    def get_path_traffic(self, path):
        """
        Get current traffic on a path (bottleneck link utilization).
        Uses database aggregated values (same as dashboard).
        
        Returns:
            Current traffic in bps, or None if no traffic
        """
        link_util = self.db.get_link_utilization()

        # Use the minimum link utilization as the path bottleneck
        link_bps = []
        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            bps = float(link_util.get((u, v), 0.0))
            link_bps.append(bps)

        if not link_bps:
            return None

        min_bps = min(link_bps)
        if min_bps <= EPS_BPS:
            return None

        return min_bps
    
    def measure_route_performance(self):
        """
        Measure actual performance of each route.
        Uses strato-style capacity discovery: if actual < expected, we hit bottleneck.
        """
        # Measure link utilization on each path
        for (src, dst), paths in self.topology['paths'].items():
            for path in paths:
                path_tuple = tuple(path)
                actual_bps = self.get_path_traffic(path)

                if actual_bps is None:
                    continue

                # Always track the maximum observed traffic
                prev_max = self.path_max_observed.get(path_tuple, 0)
                if actual_bps > prev_max:
                    self.path_max_observed[path_tuple] = actual_bps

                expected_bps = self.path_expected_bps.get(path_tuple, 0)
                current_cap = self.path_capacities.get(path_tuple, float('inf'))

                # Initialize with optimistic estimate (assume capacity is higher than current traffic)
                if math.isinf(current_cap):
                    # Start with an optimistic estimate to encourage exploration
                    self.path_capacities[path_tuple] = actual_bps * INITIAL_CAP_MULTIPLIER
                    self.path_max_observed[path_tuple] = actual_bps
                    print(f"  Path {list(path)} capacity initialized: {actual_bps/1e6:.1f} Mbps observed, assuming {actual_bps*INITIAL_CAP_MULTIPLIER/1e6:.1f} Mbps capacity")
                    continue

                # Key insight from strato: if actual < expected * 0.8, we hit bottleneck
                if expected_bps > 0 and actual_bps < expected_bps * EXPECTED_RATIO:
                    # Path cannot deliver expected traffic → bottleneck found
                    self.path_capacities[path_tuple] = actual_bps
                    self.path_bottleneck_found[path_tuple] = True
                    print(f"  Path {list(path)} BOTTLENECK: expected {expected_bps/1e6:.1f} Mbps, got {actual_bps/1e6:.1f} Mbps → capacity = {actual_bps/1e6:.1f} Mbps")
                
                # If actual exceeds current capacity estimate, increase it
                elif actual_bps > current_cap * UP_MARGIN:
                    self.path_capacities[path_tuple] = actual_bps * INITIAL_CAP_MULTIPLIER
                    print(f"  Path {list(path)} capacity increased: {actual_bps/1e6:.1f} Mbps observed, assuming {actual_bps*INITIAL_CAP_MULTIPLIER/1e6:.1f} Mbps capacity")
                
                # If path meets expectations and no bottleneck found yet, assume it has more capacity (exploration)
                elif not self.path_bottleneck_found[path_tuple] and actual_bps >= expected_bps * 0.95:
                    # Path is meeting expectations → likely has more capacity
                    # Keep current capacity estimate (which is optimistic)
                    pass
    
    def allocate_routes_by_capacity(self, src, dst, available_paths):
        """
        Allocate route count based on path capacity (to simulate weighting).
        
        Args:
            src: Source node ID
            dst: Destination node ID
            available_paths: List of available paths
            
        Returns:
            Dictionary mapping each path to number of copies to install
        """
        if not available_paths:
            return {}
        
        # Get capacities, using measured values or equal distribution if not measured
        capacities = {}
        measured_count = 0
        
        for path in available_paths:
            path_tuple = tuple(path)
            cap = self.path_capacities.get(path_tuple, float('inf'))
            if not math.isinf(cap):
                capacities[path_tuple] = cap
                measured_count += 1
        
        # If we haven't measured any capacities yet, distribute equally
        if measured_count == 0:
            equal_slots = max(1, self.max_routes // len(available_paths))
            return {tuple(path): equal_slots for path in available_paths}
        
        # For unmeasured paths, use average of measured capacities
        if measured_count < len(available_paths):
            avg_cap = sum(capacities.values()) / measured_count if measured_count > 0 else 1e8
            for path in available_paths:
                path_tuple = tuple(path)
                if path_tuple not in capacities:
                    capacities[path_tuple] = avg_cap
        
        total_capacity = sum(capacities.values())
        if total_capacity == 0:
            return {tuple(path): 1 for path in available_paths}
        
        # Exploration: in early rounds, if no bottlenecks found, boost high-capacity paths
        # This helps discover true capacities by pushing paths to their limits
        exploration_phase = self.round_num < 10
        any_bottleneck_found = any(self.path_bottleneck_found.get(tuple(p), False) for p in available_paths)
        
        if exploration_phase and not any_bottleneck_found:
            # Ranked boosting: give different weights to create allocation differences
            print(f"  Exploration mode (round {self.round_num}): ranked boosting")
            sorted_paths = sorted(capacities.items(), key=lambda x: x[1], reverse=True)
            for rank, (path_tuple, cap) in enumerate(sorted_paths):
                # Top path gets 2x boost, middle gets 1.5x, bottom gets 1x
                if rank == 0:
                    boost = 2.0
                elif rank == 1:
                    boost = 1.5
                else:
                    boost = 1.0
                old_cap = cap
                capacities[path_tuple] = cap * boost
                print(f"    Rank {rank+1} {list(path_tuple)}: {old_cap/1e6:.1f} → {capacities[path_tuple]/1e6:.1f} Mbps (boost {boost}x)")
            total_capacity = sum(capacities.values())
        
        # Allocate proportionally
        allocations = {}
        total_slots = self.max_routes
        
        for path in available_paths:
            path_tuple = tuple(path)
            ratio = capacities[path_tuple] / total_capacity
            slots = max(1, round(ratio * total_slots))
            allocations[path_tuple] = slots
        
        # Normalize to exactly max_routes
        total_allocated = sum(allocations.values())
        if total_allocated != total_slots:
            diff = total_slots - total_allocated
            # Adjust the path with highest capacity
            best_path = max(allocations, key=lambda p: capacities[p])
            allocations[best_path] = max(1, allocations[best_path] + diff)
        
        return allocations
    
    def install_weighted_routes(self, src, dst, path_allocations):
        """
        Install weighted routes by assigning paths to fixed route slots.
        Always uses max_routes slots to keep Jump Hash mappings stable.
        
        Args:
            src: Source node ID
            dst: Destination node ID
            path_allocations: Dictionary {path_tuple: count, ...}
        """
        # Create exactly max_routes paths, distributing according to allocations
        paths_to_install = []
        for path_tuple, count in path_allocations.items():
            for _ in range(count):
                paths_to_install.append(list(path_tuple))
        
        # Pad to max_routes by repeating paths proportionally
        while len(paths_to_install) < self.max_routes:
            # Add paths in proportion to their current allocation
            for path_tuple, count in sorted(path_allocations.items(), key=lambda x: -x[1]):
                if len(paths_to_install) >= self.max_routes:
                    break
                paths_to_install.append(list(path_tuple))
        
        # Trim to exactly max_routes if somehow over
        paths_to_install = paths_to_install[:self.max_routes]
        
        if not paths_to_install:
            return
        
        # Estimate total traffic for this src-dst pair
        route_util = self.db.get_route_utilization()
        total_traffic = sum(bps for (s, d, _), bps in route_util.items() if s == src and d == dst)
        if total_traffic < 1e6:  # Less than 1 Mbps, use default estimate
            total_traffic = 120e6  # 120 Mbps default
        
        # Calculate expected traffic per path
        path_counts = {}
        for path in paths_to_install:
            path_tuple = tuple(path)
            path_counts[path_tuple] = path_counts.get(path_tuple, 0) + 1
        
        for path_tuple, count in path_counts.items():
            share = count / len(paths_to_install)
            self.path_expected_bps[path_tuple] = total_traffic * share
        
        print(f"  Installing {len(paths_to_install)} routes for {src}->{dst} (fixed slot count)")
        for path_tuple, count in path_counts.items():
            cap = self.path_capacities.get(path_tuple, float('inf'))
            if math.isinf(cap):
                cap_str = "unmeasured"
            else:
                cap_str = f"{cap/1e6:.1f} Mbps"
            ratio = count / len(paths_to_install) * 100
            expected = self.path_expected_bps[path_tuple] / 1e6
            print(f"    Path {list(path_tuple)}: {count} copies ({ratio:.0f}%, capacity={cap_str}, expect={expected:.1f} Mbps)")
        
        # Update routes - this will keep route IDs stable
        self.db.update_routes(src, dst, paths_to_install)
    
    def run(self, update_interval: int):
        """
        Main loop: periodically measure and adjust routes.
        
        Args:
            update_interval: Interval between updates in seconds
        """
        self.discover_topology()
        
        # Wait for initial traffic data to stabilize
        print("\nWaiting for traffic data to stabilize (30 seconds)...")
        sleep(30)
        print("✓ Ready to start optimization")
        
        while True:
            print(f"\n=== Round {self.round_num} ===")
            
            print("Measuring route performance...")
            self.measure_route_performance()
            
            print("Reallocating routes...")
            for (src, dst), paths in self.topology['paths'].items():
                if not paths:
                    continue
                
                allocations = self.allocate_routes_by_capacity(src, dst, paths)
                
                current_routes = [
                    self.db.edges_to_path(edges)
                    for s, d, _, edges in self.db.get_all_routes()
                    if s == src and d == dst
                ]
                
                # Build current distribution
                current_dist = {}
                for route in current_routes:
                    route_tuple = tuple(route)
                    current_dist[route_tuple] = current_dist.get(route_tuple, 0) + 1
                
                # Smooth transition: move gradually towards target
                # Don't change more than 2 route copies per round for stability
                smoothed_allocations = {}
                max_change = 2  # Maximum route copies to add/remove per round
                
                # Deadband gating: only change if share difference is meaningful
                cur_total = max(1, sum(current_dist.values()))
                tgt_total = max(1, sum(allocations.values()))
                change_needed = False
                for path_tuple in set(list(current_dist.keys()) + list(allocations.keys())):
                    curr = current_dist.get(path_tuple, 0) / cur_total
                    targ = allocations.get(path_tuple, 0) / tgt_total
                    if abs(targ - curr) > DEADBAND:
                        change_needed = True
                        break

                if not change_needed:
                    # No significant change needed, keep current allocation
                    continue

                for path_tuple in set(list(current_dist.keys()) + list(allocations.keys())):
                    current = current_dist.get(path_tuple, 0)
                    target = allocations.get(path_tuple, 0)
                    
                    if abs(target - current) <= max_change:
                        smoothed_allocations[path_tuple] = target
                    elif target > current:
                        smoothed_allocations[path_tuple] = current + max_change
                    else:
                        smoothed_allocations[path_tuple] = max(0, current - max_change)
                
                # Remove zero allocations
                smoothed_allocations = {k: v for k, v in smoothed_allocations.items() if v > 0}
                
                # Check if allocation actually changed
                need_update = (current_dist != smoothed_allocations)
                
                if need_update:
                    print(f"\nUpdating {src} -> {dst}:")
                    print(f"  Current: {current_dist}")
                    print(f"  Target: {dict(allocations)}")
                    print(f"  Smoothed: {dict(smoothed_allocations)} (max ±{max_change} per round)")
                    self.install_weighted_routes(src, dst, smoothed_allocations)
                    
                    # Wait for flows to redistribute after route update
                    print(f"  Waiting 20 seconds for flows to redistribute...")
                    sleep(20)
            
            print("\n=== Statistics ===")
            route_util = self.db.get_route_utilization()
            if route_util:
                print(f"Active routes: {len(route_util)}")
                total_bps = sum(route_util.values())
                print(f"Total traffic: {total_bps / 1e9:.2f} Gbps")
                
                for (src, dst, route_id), bps in sorted(route_util.items()):
                    print(f"  Route {route_id} ({src}->{dst}): {bps/1e6:.2f} Mbps")
            
            link_util = self.db.get_link_utilization()
            if link_util:
                print(f"\nLink utilization:")
                for (u, v), bps in sorted(link_util.items()):
                    print(f"  Link {u}->{v}: {bps/1e6:.2f} Mbps")
            
            self.round_num += 1
            sleep(update_interval)


if __name__ == "__main__":
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "nextmini",
    }
    
    alg = WaterfillingAlgorithm(creds, max_routes_per_pair=10)
    
    alg.run(update_interval=10)
