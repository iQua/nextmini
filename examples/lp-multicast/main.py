#!/usr/bin/env python3
"""
LP Multicast Solver and Injector for Nextmini.

This script:
1. Reads topology from topo.toml
2. Solves LP to compute optimal multicast trees
3. Injects the trees into Nextmini's database
4. Triggers route push to dataplane

Usage:
    python main.py --config config.toml --topo topo.toml
    python main.py --topo topo.toml  # Uses default config

Example:
    cd examples/lp-multicast
    python main.py --topo topo.toml --dry-run
"""
import argparse
import sys
from pathlib import Path

try:
    import tomli
except ImportError:
    try:
        import tomllib as tomli  # Python 3.11+
    except ImportError:
        print("Error: tomli or tomllib required. Install with: pip install tomli")
        sys.exit(1)

from graph import Graph
from adapter import NextminiAdapter

# Solver imports - will select based on config
import mFlow
import multi_commodity


def load_toml(path: str) -> dict:
    """Load TOML file."""
    with open(path, "rb") as f:
        return tomli.load(f)


def main():
    parser = argparse.ArgumentParser(description="LP Solver from Square in Nextmini")
    parser.add_argument("--config", type=str, default="lp-config.toml", help="Path to lp-config.toml")
    parser.add_argument("--topo", type=str, required=True, help="Path to topology TOML file")
    parser.add_argument("--dry-run", action="store_true", help="Solve LP but don't inject into DB")
    parser.add_argument("--clear", action="store_true", help="Clear all existing groups before injecting")
    args = parser.parse_args()

    # Load configuration
    config_path = Path(args.config)
    if config_path.exists():
        config = load_toml(str(config_path))
    else:
        print(f"Warning: Config file {config_path} not found, using defaults")
        config = {}

    # Load topology
    topo_data = load_toml(args.topo)
    graph = Graph.from_toml(topo_data)
    
    print(f"Loaded topology: {len(graph.nodes)} nodes, {len(graph.edges)} edges")
    print(f"  Nodes: {graph.nodes}")
    
    # Extract sessions from topology
    sessions = topo_data.get("session", [])
    if not sessions:
        print("Error: No sessions defined in topology file")
        sys.exit(1)
    
    print(f"\nSolving LP for {len(sessions)} multicast sessions...")
    
    # Solver config
    solver_config = config.get("solver", {})
    solver_type = solver_config.get("type", "mFlow")
    
    # Select solver module based on type
    if solver_type == "multi_commodity":
        print(f"  Using solver: multi_commodity")
        solve = multi_commodity.solve
        convert_to_multicast_trees = multi_commodity.convert_mc_to_trees
        paths_to_edges = multi_commodity.paths_to_edges
    else:
        print(f"  Using solver: mFlow")
        solve = mFlow.solve
        convert_to_multicast_trees = mFlow.convert_to_multicast_trees
        paths_to_edges = mFlow.paths_to_edges
    
    # Solve for each session
    results = []
    for session in sessions:
        label = session["label"]
        src = session["src"]
        destinations = session["destinations"]
        
        print(f"\n  Session '{label}': src={src}, dst={destinations}")
        
        # Solve LP
        sources = [src]
        dest_map = {src: destinations}
        
        try:
            variables, sol = solve(graph, sources, dest_map)
        except Exception as e:
            print(f"    Error solving LP: {e}")
            continue
        
        # Convert to trees
        src_list, session_trees = convert_to_multicast_trees(variables, sol)
        
        if not session_trees or not session_trees[0]:
            print(f"    No trees found (possibly no paths exist)")
            continue
        
        # For each tree in the session
        tree_idx = 0
        for trees in session_trees:
            for tree_paths, throughput in trees:
                edges = paths_to_edges(tree_paths)
                print(f"    Tree: {len(edges)} edges, throughput={throughput:.2f}")
                print(f"      Edges: {edges}")
                results.append({
                    "label": f"{label}-{tree_idx}" if tree_idx > 0 else label,
                    "src": src,
                    "destinations": destinations,
                    "edges": edges,
                    "throughput": throughput,
                })
                tree_idx += 1
    
    if args.dry_run:
        print("\n[Dry run] Skipping database injection")
        print(f"\nTotal: {len(results)} trees computed")
        return
    
    # Inject into Nextmini
    print("\nInjecting into Nextmini database...")
    
    db_config = config.get("database", {})
    
    try:
        with NextminiAdapter(db_config) as adapter:
            if args.clear:
                print("  Clearing existing groups...")
                adapter.clear_all_groups()
            
            for result in results:
                label = result["label"]
                src = result["src"]
                destinations = result["destinations"]
                edges = result["edges"]
                
                # Create group
                group = adapter.create_group(label, src)
                print(f"  Created group {group.id}: '{label}' (src={src}, ip={group.group_ip})")
                
                # Inject tree edges
                adapter.inject_tree(group.id, src, edges)
                print(f"    Injected {len(edges)} edges")
                
                # Inject members (destinations)
                adapter.inject_members(group.id, destinations)
                print(f"    Injected {len(destinations)} members")
                
                # Trigger route push
                adapter.notify_update(group.id)
                print(f"    Triggered route update")
        
        print(f"\nDone! Injected {len(results)} multicast trees into Nextmini.")
        print("The controller will push routes to dataplane nodes.")
        
    except Exception as e:
        print(f"\nError connecting to database: {e}")
        print("Make sure Nextmini controller is running and database is accessible.")
        sys.exit(1)


if __name__ == "__main__":
    main()
