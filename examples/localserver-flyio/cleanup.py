#!/usr/bin/env python3
"""Cleanup script for Nextmini hybrid deployment."""

import subprocess
import sys


def run(cmd):
    """Run command and return success status."""
    return subprocess.run(cmd, shell=True).returncode == 0


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  uv run cleanup.py <node_numbers>  # e.g., '1 2 3' or 'all'")
        print()
        print("Examples:")
        print("  uv run cleanup.py 1 2        # Destroy node-1 and node-2")
        print(
            "  uv run cleanup.py all        # Destroy all nodes and stop local services"
        )
        sys.exit(1)

    nodes = sys.argv[1:]

    # Destroy Fly.io nodes
    if nodes == ["all"]:
        print("Destroying all Fly.io nodes...")
        run(
            "flyctl apps list | grep nextmini-node | awk '{print $1}' | xargs -I {} flyctl apps destroy {} --yes"
        )
        print("\nStopping local services...")
        run("docker compose down")
        print("Done!")
    else:
        for node in nodes:
            app = f"nextmini-node-{node}"
            print(f"Destroying {app}...")
            run(f"flyctl apps destroy {app} --yes")
        print("Done!")


if __name__ == "__main__":
    main()
