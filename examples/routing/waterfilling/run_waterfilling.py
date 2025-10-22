#!/usr/bin/env python3.13
"""
Waterfilling algorithm for the routing example.

This script demonstrates how to use the waterfilling algorithm to dynamically
optimize traffic distribution based on path capacities.

Scenario:
- 4 nodes in full mesh topology
- Two paths from node 1 to node 2:
  - Path A: [1, 3, 2] (via node 3)
  - Path B: [1, 4, 2] (via node 4)
- Links have different capacities (set in controller-config.toml)
- Algorithm will distribute traffic proportionally to capacity

Requirements:
- Python 3.13
- uv package manager
- Install dependencies: uv pip install psycopg2-binary
"""

import sys
import os
import time

# Verify Python version
if sys.version_info < (3, 13):
    print(f"Error: Python 3.13 required, but running {sys.version}")
    sys.exit(1)

# Add routing tools to path (works both in container and on host)
tools_paths = [
    '/var/nextmini/tools/routing',  # Container path
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tools', 'routing'),  # Relative path
]

for path in tools_paths:
    if os.path.exists(path):
        sys.path.insert(0, path)
        break

try:
    from common import Database
    from waterfilling import WaterfillingAlgorithm  # Dynamic measurement version
    # from waterfilling_simple import SimpleWaterfillingAlgorithm  # Static config version
except ImportError as e:
    print(f"Error importing routing modules: {e}")
    print(f"Tried paths: {tools_paths}")
    print(f"\nMake sure dependencies are installed:")
    print(f"  uv sync")
    sys.exit(1)


def main():
    """Run waterfilling algorithm for the example."""
    
    print("=" * 60)
    print("Waterfilling Algorithm - Example")
    print(f"Python version: {sys.version}")
    print("=" * 60)
    
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",  # Use localhost when running on host, "postgres" when in container
        "port": "5432",
        "database": "nextmini",
    }
    
    print("\nWaiting for database to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            db = Database(creds)
            nodes = db.get_all_nodes()
            print(f"✓ Database connected! Found {len(nodes)} nodes")
            break
        except Exception as e:
            if i < max_retries - 1:
                print(f"  Waiting... ({i+1}/{max_retries})")
                time.sleep(2)
            else:
                print(f"✗ Failed to connect to database: {e}")
                return 1
    
    print("\n" + "=" * 60)
    print("Starting Waterfilling Algorithm")
    print("=" * 60)
    print("\nConfiguration:")
    print("  - Max routes per pair: 10")
    print("  - Update interval: 10 seconds")
    print("  - Weight simulation: Enabled")
    print("\nHow it works:")
    print("  1. Measures traffic on each path")
    print("  2. Estimates path capacities")
    print("  3. Installs multiple route copies based on capacity ratio")
    print("  4. Jump Hash distributes flows across route copies")
    print("\nExpected behavior:")
    print("  - Higher capacity paths get more route copies")
    print("  - More route copies → more flows assigned")
    print("  - Traffic distribution matches capacity ratio")
    print("\n" + "=" * 60)
    
    alg = WaterfillingAlgorithm(
        creds,
        max_routes_per_pair=10
    )
    
    alg.run(update_interval=10)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
