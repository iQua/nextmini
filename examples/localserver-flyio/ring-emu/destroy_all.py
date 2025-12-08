#!/usr/bin/env python3
"""
Destroy all baseline-ring-* apps.

Usage:
    python3 destroy_all.py --num-nodes 10
"""

import argparse
import subprocess
import sys


def destroy_app(app_name: str):
    """Destroy a single app."""
    print(f"   Destroying {app_name}...")
    result = subprocess.run(
        ["flyctl", "apps", "destroy", app_name, "--yes"],
        capture_output=True,
        text=True,
        check=False
    )
    if result.returncode == 0:
        print(f"  {app_name} destroyed")
        return True
    elif "Could not find App" in result.stderr or "not found" in result.stderr:
        print(f"  {app_name} not found (skipping)")
        return True
    else:
        print(f"  Failed to destroy {app_name}: {result.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Destroy all baseline-ring nodes")
    parser.add_argument("--num-nodes", type=int, default=10, help="Number of nodes")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Destroying Baseline Ring Nodes")
    print("=" * 60)
    print(f"   Nodes: {args.num_nodes}")
    print()
    
    success_count = 0
    for i in range(1, args.num_nodes + 1):
        app_name = f"baseline-ring-{i}"
        if destroy_app(app_name):
            success_count += 1
    
    print()
    print("=" * 60)
    print(f"Destroyed {success_count}/{args.num_nodes} apps")
    print("=" * 60)


if __name__ == "__main__":
    main()

