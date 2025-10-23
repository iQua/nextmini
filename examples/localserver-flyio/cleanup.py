#!/usr/bin/env python3
"""Cleanup script for Nextmini hybrid deployment."""

import argparse
import subprocess


def run(cmd: str) -> bool:
    """Run command and return success status."""
    return subprocess.run(cmd, shell=True).returncode == 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Destroy Fly.io dataplane nodes and optionally stop the local controller."
    )
    parser.add_argument(
        "nodes",
        nargs="+",
        help="Node numbers to destroy (e.g. 1 2) or 'all' for every node.",
    )
    parser.add_argument(
        "--stop-controller",
        action="store_true",
        help="Stop the local controller stack via 'docker compose down' (always done for 'all').",
    )
    return parser.parse_args()


def cleanup_all(stop_controller: bool) -> None:
    print("Destroying all Fly.io nodes...")
    run(
        "flyctl apps list | grep nextmini-node | awk '{print $1}' | xargs -I {} flyctl apps destroy {} --yes"
    )
    if stop_controller:
        print("\nStopping local services (controller and dependencies)...")
        run("docker compose down")
    print("Done!")


def cleanup_subset(nodes, stop_controller: bool) -> None:
    for node in nodes:
        app = f"nextmini-node-{node}"
        print(f"Destroying {app}...")
        run(f"flyctl apps destroy {app} --yes")
    if stop_controller:
        print("Stopping local services (controller and dependencies)...")
        run("docker compose down")
    print("Done!")


def main():
    args = parse_args()
    if args.nodes == ["all"]:
        cleanup_all(stop_controller=True)
    else:
        cleanup_subset(args.nodes, stop_controller=args.stop_controller)


if __name__ == "__main__":
    main()
